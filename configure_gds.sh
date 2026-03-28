#!/usr/bin/env bash
# configure_gds.sh — Configure NVIDIA GDS rdma_dev_addr_list for WEKA access
#
# Usage: configure_gds.sh [-l] [ip_file|ip ...] [-u user] [-k keyfile] [-p port] [-d] [-v] [-f]
#
#   -l            Run on the local system (no SSH)
#   ip_file       File containing one IP address per line (blank lines and # comments ignored)
#   ip [ip ...]   One or more IP addresses/hostnames passed directly on the command line
#   -u user   SSH username (default: root)
#   -k file   SSH private key file
#   -p port   SSH port (default: 22)
#   -d        Dry-run: detect and report only, make no changes
#   -v        Verbose: show [INFO] lines per host (normally suppressed)
#   -f        Force: skip confirmation prompt

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_VERSION="1.0.0"
DEFAULT_USER="root"
DEFAULT_PORT=22

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
SSH_USER="$DEFAULT_USER"
SSH_PORT=$DEFAULT_PORT
SSH_KEYFILE=""
DRY_RUN=false
VERBOSE=false
FORCE=false
LOCAL_MODE=false

declare -a HOSTS=()
declare -A HOST_STATUS=()   # host -> SUCCESS | FAILED | SKIPPED
declare -A HOST_MESSAGE=()  # host -> human-readable reason

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[$(date +%H:%M:%S)] $*"; }
err()  { echo "[$(date +%H:%M:%S)] ERROR: $*" >&2; }

usage() {
    cat <<EOF
Usage: $0 [-l] [ip_file|ip ...] [-u user] [-k keyfile] [-p port] [-d] [-v] [-f]

  -l            Run on the local system (no SSH, no hosts needed)
  ip_file       File with one IP address per line (# comments and blank lines ok)
  ip [ip ...]   One or more IP addresses/hostnames passed directly

  -u user   SSH username            (default: root)
  -k file   SSH private key file
  -p port   SSH port                (default: 22)
  -d        Dry-run (no writes)
  -v        Verbose (show [INFO] lines)
  -f        Skip confirmation prompt

Examples:
  $0 -l                          # configure the local system
  $0 -l -d -v                    # local dry-run with verbose output
  $0 hosts.txt                   # configure remote hosts from file
  $0 10.0.0.1 10.0.0.2 -u admin  # configure remote hosts inline
  $0 hosts.txt 10.0.0.99 -d -v   # mixed file + inline, dry-run

Version: $SCRIPT_VERSION
EOF
    exit 1
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
parse_args() {
    # Pre-scan for -l flag so we know whether positional args are required.
    local arg
    for arg in "$@"; do
        [[ "$arg" == "-l" ]] && LOCAL_MODE=true
    done

    # Collect positional arguments (before flags).
    declare -ga RAW_TARGETS=()
    while [[ $# -gt 0 && "$1" != -* ]]; do
        RAW_TARGETS+=("$1")
        shift
    done

    # In remote mode, at least one host or file is required.
    if [[ "$LOCAL_MODE" != true && ${#RAW_TARGETS[@]} -eq 0 ]]; then
        err "At least one IP address, an IP file, or -l (local) is required."
        usage
    fi

    OPTIND=1
    while getopts ":u:k:p:dvfl" opt; do
        case "$opt" in
            u) SSH_USER="$OPTARG" ;;
            k) SSH_KEYFILE="$OPTARG" ;;
            p) SSH_PORT="$OPTARG" ;;
            d) DRY_RUN=true ;;
            v) VERBOSE=true ;;
            f) FORCE=true ;;
            l) LOCAL_MODE=true ;;
            :) err "Option -$OPTARG requires an argument."; usage ;;
            \?) err "Unknown option: -$OPTARG"; usage ;;
        esac
    done

    # Warn if hosts were provided alongside -l
    if [[ "$LOCAL_MODE" == true && ${#RAW_TARGETS[@]} -gt 0 ]]; then
        err "Warning: -l (local mode) ignores any hosts/files provided."
    fi
}

add_host() {
    local h="$1"
    h="${h#"${h%%[![:space:]]*}"}"   # strip leading whitespace
    h="${h%"${h##*[![:space:]]}"}"   # strip trailing whitespace
    [[ -z "$h" || "$h" == \#* ]] && return
    HOSTS+=("$h")
}

validate_inputs() {
    if [[ "$LOCAL_MODE" == true ]]; then
        HOSTS=("localhost")
        return
    fi

    if [[ -n "$SSH_KEYFILE" && ! -f "$SSH_KEYFILE" ]]; then
        err "Key file not found: $SSH_KEYFILE"
        exit 1
    fi

    # Each RAW_TARGET is either a file path or an IP/hostname.
    # If exactly one target and it's a readable file, treat it as an IP file.
    # Otherwise treat all targets as inline IPs/hostnames (a mix is also allowed:
    # any target that is a readable file gets expanded; others are used directly).
    local target
    for target in "${RAW_TARGETS[@]}"; do
        if [[ -f "$target" ]]; then
            # Expand file into hosts
            while IFS= read -r line || [[ -n "$line" ]]; do
                add_host "$line"
            done < "$target"
        else
            add_host "$target"
        fi
    done

    if [[ ${#HOSTS[@]} -eq 0 ]]; then
        err "No valid IP addresses or hostnames found."
        exit 1
    fi
}

build_ssh_opts() {
    SSH_OPTS=(
        -o StrictHostKeyChecking=no
        -o ConnectTimeout=10
        -o BatchMode=yes
        -o LogLevel=ERROR
        -p "$SSH_PORT"
    )
    if [[ -n "$SSH_KEYFILE" ]]; then
        SSH_OPTS+=(-i "$SSH_KEYFILE")
    fi
}

confirm_execution() {
    if [[ "$FORCE" == true ]]; then return; fi

    echo ""
    echo "=== GDS Configuration Script v${SCRIPT_VERSION} ==="
    if [[ "$LOCAL_MODE" == true ]]; then
        echo "  Mode:     LOCAL"
    else
        echo "  Hosts:    ${#HOSTS[@]}"
        echo "  User:     $SSH_USER"
        echo "  Port:     $SSH_PORT"
        [[ -n "$SSH_KEYFILE" ]] && echo "  Key:      $SSH_KEYFILE"
    fi
    echo "  Dry-run:  $DRY_RUN"
    echo ""
    read -r -p "Proceed? [y/N] " answer
    case "$answer" in
        [yY][eE][sS]|[yY]) ;;
        *) echo "Aborted."; exit 0 ;;
    esac
    echo ""
}

# ---------------------------------------------------------------------------
# Remote script body (emitted by get_remote_script, executed on target host)
# ---------------------------------------------------------------------------
get_remote_script() {
    cat <<'REMOTE_SCRIPT'

detect_gpu_numa_nodes() {
    # Returns associative array GPU_NUMA_NODES[bus_id]=numa and
    # VALID_GPU_NUMAS[numa]=1 for numa >= 0
    declare -gA GPU_NUMA_NODES=()
    declare -gA VALID_GPU_NUMAS=()

    local raw_bus_ids
    raw_bus_ids=$(nvidia-smi --query-gpu=gpu_bus_id --format=csv,noheader 2>/dev/null) || true

    if [[ -z "$raw_bus_ids" ]]; then
        echo "[ERROR] No GPUs detected by nvidia-smi"
        return 1
    fi

    while IFS= read -r raw_id; do
        raw_id="${raw_id//$'\r'/}"   # strip carriage returns
        [[ -z "$raw_id" ]] && continue
        # Normalize: lowercase, 4-char domain (e.g. 00000000:3B:00.0 -> 0000:3b:00.0)
        local lower domain rest
        lower=$(echo "$raw_id" | tr '[:upper:]' '[:lower:]')
        domain="${lower%%:*}"
        rest="${lower#*:}"
        # strip leading zeros from domain, keep at least one char, then pad to 4
        domain=$(echo "$domain" | sed 's/^0*//')
        [[ -z "$domain" ]] && domain="0"
        while [[ ${#domain} -lt 4 ]]; do domain="0${domain}"; done
        local bus_id="${domain}:${rest}"

        local sysfs="/sys/bus/pci/devices/${bus_id}/numa_node"
        if [[ -f "$sysfs" ]]; then
            local numa
            numa=$(cat "$sysfs")
            GPU_NUMA_NODES["$bus_id"]="$numa"
            if [[ "$numa" -ge 0 ]]; then
                VALID_GPU_NUMAS["$numa"]=1
            fi
        else
            # Try without leading domain (some kernels)
            local short_id="${rest}"  # e.g. 3b:00.0
            sysfs="/sys/bus/pci/devices/${short_id}/numa_node"
            if [[ -f "$sysfs" ]]; then
                local numa
                numa=$(cat "$sysfs")
                GPU_NUMA_NODES["$short_id"]="$numa"
                if [[ "$numa" -ge 0 ]]; then
                    VALID_GPU_NUMAS["$numa"]=1
                fi
            else
                echo "[WARN] Cannot find sysfs entry for GPU $raw_id (tried $bus_id)"
            fi
        fi
    done <<< "$raw_bus_ids"

    local gpu_count="${#GPU_NUMA_NODES[@]}"
    if [[ "$gpu_count" -eq 0 ]]; then
        echo "[ERROR] No GPUs could be mapped to sysfs entries"
        return 1
    fi
    echo "[INFO]  Found ${gpu_count} GPU(s)"
}

detect_mlx_devices() {
    declare -gA MLX_NUMA=()

    local dev
    for dev in /sys/class/infiniband/mlx5_*; do
        [[ -d "$dev" ]] || continue
        local dev_name
        dev_name=$(basename "$dev")
        local numa_file="${dev}/device/numa_node"
        if [[ -f "$numa_file" ]]; then
            MLX_NUMA["$dev_name"]=$(cat "$numa_file")
        else
            MLX_NUMA["$dev_name"]="unknown"
        fi
    done

    if [[ ${#MLX_NUMA[@]} -eq 0 ]]; then
        echo "[ERROR] No mlx5 InfiniBand devices found in /sys/class/infiniband/"
        return 1
    fi
    echo "[INFO]  Found ${#MLX_NUMA[@]} mlx5 device(s): ${!MLX_NUMA[*]}"
}

find_gpu_resident_devs() {
    declare -ga GPU_RESIDENT_DEVS=()

    if [[ ${#VALID_GPU_NUMAS[@]} -gt 0 ]]; then
        # Primary path: NUMA matching
        echo "[INFO]  Using NUMA-based GPU-resident detection"
        local dev_name dev_numa
        for dev_name in "${!MLX_NUMA[@]}"; do
            dev_numa="${MLX_NUMA[$dev_name]}"
            if [[ "$dev_numa" == "unknown" ]]; then
                echo "[WARN]  $dev_name: NUMA node unreadable, skipping"
                continue
            fi
            if [[ "$dev_numa" -lt 0 ]]; then
                echo "[WARN]  $dev_name: NUMA node = $dev_numa (no NUMA info), skipping in primary pass"
                continue
            fi
            if [[ -n "${VALID_GPU_NUMAS[$dev_numa]+x}" ]]; then
                GPU_RESIDENT_DEVS+=("$dev_name")
                echo "[INFO]  $dev_name: NUMA $dev_numa matches a GPU NUMA node -> GPU-resident"
            else
                echo "[INFO]  $dev_name: NUMA $dev_numa does not match any GPU NUMA node"
            fi
        done
    else
        # Fallback: PCIe root complex matching (all GPU NUMA nodes were -1)
        echo "[WARN]  All GPU NUMA nodes report -1; falling back to PCIe root complex matching"
        declare -A GPU_ROOT_BUSES=()
        local bus_id
        for bus_id in "${!GPU_NUMA_NODES[@]}"; do
            local root_bus
            root_bus=$(echo "$bus_id" | cut -d: -f1-2)
            GPU_ROOT_BUSES["$root_bus"]=1
        done

        local dev_name
        for dev_name in "${!MLX_NUMA[@]}"; do
            local dev_sysfs="/sys/class/infiniband/${dev_name}/device"
            if [[ -L "$dev_sysfs" ]]; then
                local dev_pci dev_root_bus
                dev_pci=$(basename "$(readlink -f "$dev_sysfs")")
                dev_root_bus=$(echo "$dev_pci" | cut -d: -f1-2)
                if [[ -n "${GPU_ROOT_BUSES[$dev_root_bus]+x}" ]]; then
                    GPU_RESIDENT_DEVS+=("$dev_name")
                    echo "[INFO]  $dev_name: PCIe root bus $dev_root_bus matches a GPU -> GPU-resident (fallback)"
                else
                    echo "[INFO]  $dev_name: PCIe root bus $dev_root_bus does not match any GPU"
                fi
            else
                echo "[WARN]  $dev_name: cannot resolve PCI device symlink, skipping"
            fi
        done
    fi

    if [[ ${#GPU_RESIDENT_DEVS[@]} -eq 0 ]]; then
        echo "[WARN]  No GPU-resident mlx5 devices found. Nothing to configure."
        return 0
    fi
    echo "[INFO]  GPU-resident mlx5 devices: ${GPU_RESIDENT_DEVS[*]}"
}

resolve_rdma_addresses() {
    # Resolve GPU-resident mlx device names to their IP addresses.
    # cufile.json rdma_dev_addr_list requires IPs for user-space RDMA (WEKA).
    # Only devices with an assigned IPv4 address are included.
    declare -ga RDMA_ADDR_LIST=()

    local dev_name
    for dev_name in "${GPU_RESIDENT_DEVS[@]}"; do
        # Find the network interface(s) for this IB device via sysfs
        local net_dir="/sys/class/infiniband/${dev_name}/device/net"
        if [[ ! -d "$net_dir" ]]; then
            echo "[WARN]  $dev_name: no network interface found in sysfs, skipping"
            continue
        fi

        local iface
        for iface in "$net_dir"/*/; do
            [[ -d "$iface" ]] || continue
            iface=$(basename "$iface")

            # Get IPv4 addresses on this interface
            local ip
            ip=$(ip -4 -o addr show dev "$iface" 2>/dev/null \
                 | awk '{print $4}' | cut -d/ -f1 | head -1)

            if [[ -n "$ip" ]]; then
                RDMA_ADDR_LIST+=("$ip")
                echo "[INFO]  $dev_name -> $iface -> $ip"
            else
                echo "[INFO]  $dev_name -> $iface (no IPv4 address, skipping)"
            fi
        done
    done

    if [[ ${#RDMA_ADDR_LIST[@]} -eq 0 ]]; then
        echo "[WARN]  No GPU-resident mlx5 devices have IPv4 addresses assigned"
        echo "[WARN]  rdma_dev_addr_list will be empty — assign IPs to data plane interfaces"
        return 0
    fi
    echo "[INFO]  RDMA addresses for cufile.json: ${RDMA_ADDR_LIST[*]}"
}

strip_json_comments() {
    # cufile.json ships with C-style // comments which are not valid JSON.
    # Strip them in-place before any JSON tool processes the file.
    local file="$1"
    if command -v python3 &>/dev/null; then
        python3 - "$file" <<'STRIP_PYEOF'
import sys

def strip_comments(text):
    result = []
    i = 0
    in_string = False
    while i < len(text):
        if in_string:
            if text[i] == '\\' and i + 1 < len(text):
                result.append(text[i:i+2])
                i += 2
                continue
            if text[i] == '"':
                in_string = False
            result.append(text[i])
            i += 1
        else:
            if text[i] == '"':
                in_string = True
                result.append(text[i])
                i += 1
            elif i + 1 < len(text) and text[i:i+2] == '//':
                while i < len(text) and text[i] != '\n':
                    i += 1
            elif i + 1 < len(text) and text[i:i+2] == '/*':
                i += 2
                while i + 1 < len(text) and text[i:i+2] != '*/':
                    i += 1
                if i + 1 < len(text):
                    i += 2
            else:
                result.append(text[i])
                i += 1
    return ''.join(result)

filepath = sys.argv[1]
with open(filepath) as f:
    text = f.read()
with open(filepath, 'w') as f:
    f.write(strip_comments(text))
STRIP_PYEOF
    else
        # sed fallback: strip // line comments (safe for cufile.json)
        sed -i 's|[[:space:]]*//.*$||' "$file"
    fi
}

read_or_create_cufile() {
    local CUFILE="/etc/cufile.json"
    declare -g FILE_EXISTED=false
    declare -g EXISTING_JSON=""

    local DEFAULT_TEMPLATE='{
  "logging": {
    "dir": "/tmp/",
    "level": "WARN"
  },
  "properties": {
    "use_compat_mode": false,
    "force_compat_mode": false,
    "gds_rdma_write_support": true,
    "rdma_dev_addr_list": []
  }
}'

    if [[ ! -f "$CUFILE" ]]; then
        echo "[INFO]  $CUFILE does not exist, will create from default template"
        EXISTING_JSON="$DEFAULT_TEMPLATE"
        FILE_EXISTED=false
    else
        EXISTING_JSON=$(cat "$CUFILE")
        FILE_EXISTED=true
        echo "[INFO]  $CUFILE exists, will merge into existing config"
    fi
}

merge_and_write() {
    local CUFILE="/etc/cufile.json"
    local TIMESTAMP
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    local BAK="${CUFILE}.bak.${TIMESTAMP}"

    # Write existing JSON to a temp file, strip C-style comments, then manipulate
    local TMPJSON
    TMPJSON=$(mktemp /tmp/cufile_update.XXXXXX)
    echo "$EXISTING_JSON" > "$TMPJSON"
    strip_json_comments "$TMPJSON"
    # Re-read cleaned JSON so comparisons later work correctly
    EXISTING_JSON=$(cat "$TMPJSON")

    local NEW_JSON=""
    local JSON_TOOL=""

    if command -v python3 &>/dev/null; then
        JSON_TOOL="python3"
        NEW_JSON=$(python3 - "$TMPJSON" ${RDMA_ADDR_LIST[@]+"${RDMA_ADDR_LIST[@]}"} <<'PYEOF'
import json, sys

tmpfile = sys.argv[1]
new_devs = sys.argv[2:]

with open(tmpfile) as f:
    try:
        current = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON: {e}", file=sys.stderr)
        sys.exit(1)

changes = []

# --- properties section ---
props = current.setdefault("properties", {})

# rdma_dev_addr_list: merge detected devices
existing = props.get("rdma_dev_addr_list", [])
if not isinstance(existing, list):
    print(f"[WARN]  rdma_dev_addr_list was not a list ({type(existing).__name__}), resetting to []",
          file=sys.stderr)
    existing = []
merged = list(existing)
for d in new_devs:
    if d not in merged:
        merged.append(d)
        changes.append(f"rdma_dev_addr_list: added {d}")
props["rdma_dev_addr_list"] = merged

# use_compat_mode / allow_compat_mode: must be false for GDS direct path
for key in ("use_compat_mode", "allow_compat_mode"):
    if props.get(key) is not False:
        props[key] = False
        changes.append(f"{key}: set to false")

# gds_rdma_write_support: must be true
if props.get("gds_rdma_write_support") is not True:
    props["gds_rdma_write_support"] = True
    changes.append("gds_rdma_write_support: set to true")

# --- fs.weka section ---
fs = current.setdefault("fs", {})
weka = fs.setdefault("weka", {})
if weka.get("rdma_write_support") is not True:
    weka["rdma_write_support"] = True
    changes.append("fs.weka.rdma_write_support: set to true")

if not changes:
    print("NO_CHANGES", file=sys.stderr)
else:
    for c in changes:
        print(f"CHANGE:{c}", file=sys.stderr)

print(json.dumps(current, indent=2))
PYEOF
        ) || { rm -f "$TMPJSON"; echo "[ERROR] python3 JSON manipulation failed"; return 1; }

    elif command -v jq &>/dev/null; then
        JSON_TOOL="jq"
        # Build a JSON array of device names for jq
        local devs_json
        devs_json="["
        local first=true
        local d
        for d in ${RDMA_ADDR_LIST[@]+"${RDMA_ADDR_LIST[@]}"}; do
            [[ "$first" == true ]] || devs_json+=","
            devs_json+="\"${d}\""
            first=false
        done
        devs_json+="]"

        NEW_JSON=$(jq --argjson new_devs "$devs_json" '
            # Merge RDMA devices
            .properties.rdma_dev_addr_list = ((.properties.rdma_dev_addr_list // []) + $new_devs | unique) |
            # Disable compat mode for GDS direct path
            .properties.use_compat_mode = false |
            .properties.allow_compat_mode = false |
            # Enable RDMA write support
            .properties.gds_rdma_write_support = true |
            # Enable WEKA RDMA write support
            .fs.weka.rdma_write_support = true
        ' "$TMPJSON") \
            || { rm -f "$TMPJSON"; echo "[ERROR] jq JSON manipulation failed"; return 1; }
    else
        rm -f "$TMPJSON"
        echo "[ERROR] Neither python3 nor jq is available. Cannot update JSON."
        return 1
    fi

    rm -f "$TMPJSON"
    echo "[INFO]  JSON updated using $JSON_TOOL"

    # Check if no changes were needed by comparing full sorted JSON
    local no_changes=false
    local old_norm new_norm
    if [[ "$JSON_TOOL" == "python3" ]]; then
        old_norm=$(echo "$EXISTING_JSON" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin),sort_keys=True))' 2>/dev/null || echo "")
        new_norm=$(echo "$NEW_JSON"      | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin),sort_keys=True))' 2>/dev/null || echo "")
    else
        old_norm=$(echo "$EXISTING_JSON" | jq -S '.' 2>/dev/null || echo "")
        new_norm=$(echo "$NEW_JSON"      | jq -S '.' 2>/dev/null || echo "")
    fi
    [[ "$old_norm" == "$new_norm" ]] && no_changes=true

    if [[ "$no_changes" == true ]]; then
        echo "[INFO]  No changes needed — configuration already up to date"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        local preview
        if [[ "$JSON_TOOL" == "python3" ]]; then
            preview=$(echo "$NEW_JSON" | python3 -c \
                'import json,sys; d=json.load(sys.stdin); print(d.get("properties",{}).get("rdma_dev_addr_list",[]))' \
                2>/dev/null || echo "(parse error)")
        else
            preview=$(echo "$NEW_JSON" | jq '.properties.rdma_dev_addr_list' 2>/dev/null || echo "(parse error)")
        fi
        echo "[DRY-RUN] Would backup $CUFILE to $BAK"
        echo "[DRY-RUN] Would write new config to $CUFILE"
        echo "[DRY-RUN] New rdma_dev_addr_list: $preview"
        return 0
    fi

    # Backup existing file if it existed
    if [[ "$FILE_EXISTED" == true ]]; then
        cp "$CUFILE" "$BAK"
        echo "[INFO]  Backed up $CUFILE to $BAK"
    fi

    # Atomic write via temp file + mv
    local TMPOUT
    TMPOUT=$(mktemp /tmp/cufile_new.XXXXXX)
    echo "$NEW_JSON" > "$TMPOUT"
    mv "$TMPOUT" "$CUFILE"

    local final_list
    if [[ "$JSON_TOOL" == "python3" ]]; then
        final_list=$(echo "$NEW_JSON" | python3 -c \
            'import json,sys; d=json.load(sys.stdin); print(d.get("properties",{}).get("rdma_dev_addr_list",[]))' \
            2>/dev/null || echo "(see $CUFILE)")
    else
        final_list=$(echo "$NEW_JSON" | jq '.properties.rdma_dev_addr_list' 2>/dev/null || echo "(see $CUFILE)")
    fi
    echo "[CHANGE] $CUFILE updated. rdma_dev_addr_list: $final_list"
}

# ---- System preparation ----
detect_pkg_manager() {
    # Sets PKG_MGR to "apt" or "yum"/"dnf" based on what's available
    declare -g PKG_MGR=""
    if command -v apt-get &>/dev/null; then
        PKG_MGR="apt"
    elif command -v dnf &>/dev/null; then
        PKG_MGR="dnf"
    elif command -v yum &>/dev/null; then
        PKG_MGR="yum"
    fi
}

detect_cuda_version() {
    # Detect the installed CUDA major.minor version.
    # Sets CUDA_VER (e.g. "12-6") in package-name format and
    # CUDA_VER_DOT (e.g. "12.6") for display.
    declare -g CUDA_VER="" CUDA_VER_DOT=""

    local ver=""

    # Method 1: nvcc --version (most reliable if in PATH)
    if command -v nvcc &>/dev/null; then
        ver=$(nvcc --version 2>/dev/null | grep -oP 'release \K[0-9]+\.[0-9]+' | head -1)
    fi

    # Method 2: /usr/local/cuda/version.json
    if [[ -z "$ver" && -f /usr/local/cuda/version.json ]]; then
        ver=$(python3 -c "import json; print(json.load(open('/usr/local/cuda/version.json'))['cuda']['version'])" 2>/dev/null \
              | grep -oP '^[0-9]+\.[0-9]+')
    fi

    # Method 3: /usr/local/cuda/version.txt (older CUDA)
    if [[ -z "$ver" && -f /usr/local/cuda/version.txt ]]; then
        ver=$(grep -oP '[0-9]+\.[0-9]+' /usr/local/cuda/version.txt | head -1)
    fi

    # Method 4: dpkg/rpm query for cuda-toolkit package
    if [[ -z "$ver" ]]; then
        ver=$(dpkg -l 'cuda-toolkit-*' 2>/dev/null | awk '/^ii/{print $2}' \
              | grep -oP '[0-9]+-[0-9]+' | head -1 | tr '-' '.')
    fi
    if [[ -z "$ver" ]]; then
        ver=$(rpm -qa 'cuda-toolkit-*' 2>/dev/null \
              | grep -oP '[0-9]+-[0-9]+' | head -1 | tr '-' '.')
    fi

    if [[ -n "$ver" ]]; then
        CUDA_VER_DOT="$ver"
        CUDA_VER="${ver/./-}"
        echo "[INFO]  Detected CUDA version: $CUDA_VER_DOT"
    else
        echo "[WARN]  Could not detect CUDA version — will install unversioned GDS package"
    fi
}

install_gds_packages() {
    detect_pkg_manager
    if [[ -z "$PKG_MGR" ]]; then
        echo "[ERROR] No supported package manager found (need apt, dnf, or yum)"
        return 1
    fi

    detect_cuda_version

    echo "[INFO]  Installing GDS packages using $PKG_MGR..."
    case "$PKG_MGR" in
        apt)
            export DEBIAN_FRONTEND=noninteractive
            apt-get update -qq 2>/dev/null
            # Try versioned package first (matches installed CUDA), then unversioned
            if [[ -n "$CUDA_VER" ]]; then
                echo "[INFO]  Targeting GDS for CUDA $CUDA_VER_DOT"
                if apt-get install -y -qq "nvidia-gds-${CUDA_VER}" 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-gds-${CUDA_VER}"
                elif apt-get install -y -qq nvidia-gds 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-gds (unversioned fallback)"
                elif apt-get install -y -qq "nvidia-fs-dkms" "libcufile-${CUDA_VER}" 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-fs-dkms + libcufile-${CUDA_VER}"
                else
                    echo "[ERROR] Failed to install GDS packages via apt"
                    return 1
                fi
            else
                if apt-get install -y -qq nvidia-gds 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-gds"
                elif apt-get install -y -qq nvidia-fs-dkms libcufile0 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-fs-dkms + libcufile0"
                else
                    echo "[ERROR] Failed to install GDS packages via apt"
                    return 1
                fi
            fi
            ;;
        dnf|yum)
            if [[ -n "$CUDA_VER" ]]; then
                echo "[INFO]  Targeting GDS for CUDA $CUDA_VER_DOT"
                if $PKG_MGR install -y "nvidia-gds-${CUDA_VER}" 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-gds-${CUDA_VER}"
                elif $PKG_MGR install -y nvidia-gds 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-gds (unversioned fallback)"
                else
                    echo "[ERROR] Failed to install GDS packages via $PKG_MGR"
                    return 1
                fi
            else
                if $PKG_MGR install -y nvidia-gds 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-gds"
                elif $PKG_MGR install -y nvidia-fs nvidia-fs-dkms 2>/dev/null; then
                    echo "[CHANGE] Installed nvidia-fs packages"
                else
                    echo "[ERROR] Failed to install GDS packages via $PKG_MGR"
                    return 1
                fi
            fi
            ;;
    esac
    return 0
}

ensure_gds_installed() {
    # Check if nvidia_fs kernel module is available; install GDS if missing
    if ! lsmod | grep -q nvidia_fs && ! modinfo nvidia_fs &>/dev/null; then
        echo "[WARN]  nvidia_fs kernel module not found — GDS is not installed"
        if [[ "$DRY_RUN" == "true" ]]; then
            detect_cuda_version
            if [[ -n "$CUDA_VER" ]]; then
                echo "[DRY-RUN] Would install GDS packages for CUDA $CUDA_VER_DOT"
            else
                echo "[DRY-RUN] Would install GDS packages (unversioned)"
            fi
        else
            install_gds_packages || exit 1
            if ! modinfo nvidia_fs &>/dev/null; then
                echo "[ERROR] nvidia_fs module still not available after install"
                echo "[ERROR] A DKMS build may have failed — check dkms status"
                exit 1
            fi
        fi
    fi

    # Ensure nvidia_fs is loaded (not just installed)
    if lsmod | grep -q nvidia_fs; then
        echo "[INFO]  nvidia_fs kernel module loaded"
    else
        if [[ "$DRY_RUN" == "true" ]]; then
            echo "[DRY-RUN] Would load nvidia_fs kernel module"
        else
            echo "[INFO]  Loading nvidia_fs kernel module..."
            if modprobe nvidia_fs 2>/dev/null; then
                echo "[CHANGE] nvidia_fs module loaded"
            else
                echo "[ERROR] Failed to load nvidia_fs module"
                exit 1
            fi
        fi
    fi

    # Check for libcufile.so (non-fatal, but warn)
    if ldconfig -p 2>/dev/null | grep -q libcufile.so; then
        echo "[INFO]  libcufile.so found"
    elif [[ -f /usr/local/cuda/lib64/libcufile.so ]] || [[ -f /usr/lib/x86_64-linux-gnu/libcufile.so ]]; then
        echo "[INFO]  libcufile.so found"
    else
        echo "[WARN]  libcufile.so not found — applications may fail to use GDS"
    fi
}

ensure_nvidia_peermem() {
    # nvidia_peermem (or nvidia-peermem) is required for GDS RDMA.
    # It enables GPU peer memory access for RDMA NICs.
    if lsmod | grep -q nvidia_peermem; then
        echo "[INFO]  nvidia_peermem module already loaded"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] Would load nvidia_peermem kernel module"
        return 0
    fi

    echo "[INFO]  Loading nvidia_peermem kernel module..."
    if modprobe nvidia_peermem 2>/dev/null; then
        echo "[CHANGE] nvidia_peermem module loaded"
    elif modprobe nvidia-peermem 2>/dev/null; then
        echo "[CHANGE] nvidia-peermem module loaded"
    else
        echo "[WARN]  Could not load nvidia_peermem — GDS RDMA may not function"
        echo "[WARN]  Ensure nvidia-fs / nvidia-peermem packages are installed"
    fi
}

ensure_rdma_library() {
    # libcufile_rdma.so must be findable by the linker for GDS RDMA to work.
    if ldconfig -p 2>/dev/null | grep -q libcufile_rdma.so; then
        echo "[INFO]  libcufile_rdma.so found in linker cache"
        return 0
    fi

    # Check common CUDA library paths
    local rdma_lib=""
    local search_paths=(
        /usr/local/cuda/lib64
        /usr/local/cuda/targets/x86_64-linux/lib
        /usr/lib/x86_64-linux-gnu
    )
    local p
    for p in "${search_paths[@]}"; do
        if [[ -f "${p}/libcufile_rdma.so" ]]; then
            rdma_lib="${p}/libcufile_rdma.so"
            break
        fi
        # Also check for versioned variants (libcufile_rdma.so.1.x.x)
        local match
        match=$(ls "${p}"/libcufile_rdma.so* 2>/dev/null | head -1)
        if [[ -n "$match" ]]; then
            rdma_lib="$match"
            break
        fi
    done

    if [[ -z "$rdma_lib" ]]; then
        echo "[WARN]  libcufile_rdma.so not found — GDS RDMA transfers will not work"
        echo "[WARN]  It should be part of the GDS/CUDA packages"
        return 0
    fi

    local lib_dir
    lib_dir=$(dirname "$rdma_lib")
    echo "[INFO]  Found $rdma_lib"

    # Ensure the directory is in the linker path
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY-RUN] Would add $lib_dir to /etc/ld.so.conf.d/ and run ldconfig"
        return 0
    fi

    if [[ ! -f /etc/ld.so.conf.d/cuda-gds.conf ]] || ! grep -q "$lib_dir" /etc/ld.so.conf.d/cuda-gds.conf 2>/dev/null; then
        echo "$lib_dir" >> /etc/ld.so.conf.d/cuda-gds.conf
        ldconfig 2>/dev/null
        echo "[CHANGE] Added $lib_dir to linker path and ran ldconfig"
    fi
}

# ---- Main remote execution ----
main_remote() {
    # Step 1: Prerequisites
    if ! command -v nvidia-smi &>/dev/null; then
        echo "[ERROR] nvidia-smi not found — is the NVIDIA driver installed?"
        exit 1
    fi
    if [[ ! -d /sys/class/infiniband ]]; then
        echo "[ERROR] /sys/class/infiniband does not exist — no RDMA subsystem found"
        exit 1
    fi

    # Check that GDS (nvidia-fs) is installed; install if missing
    ensure_gds_installed

    # Step 2: GPU detection
    detect_gpu_numa_nodes || exit 1

    # Step 3: MLX device detection
    detect_mlx_devices || exit 1

    # Step 4: GPU-resident matching
    find_gpu_resident_devs
    if [[ ${#GPU_RESIDENT_DEVS[@]} -eq 0 ]]; then
        exit 0
    fi

    # Step 5: Resolve mlx devices to IP addresses for rdma_dev_addr_list
    resolve_rdma_addresses

    # Step 6: Ensure nvidia_peermem is loaded (required for GDS RDMA)
    ensure_nvidia_peermem

    # Step 7: Ensure libcufile_rdma.so is findable
    ensure_rdma_library

    # Step 8: Read/create cufile.json
    read_or_create_cufile

    # Step 9: Merge and write
    merge_and_write || exit 1

    exit 0
}

main_remote

REMOTE_SCRIPT
}

# ---------------------------------------------------------------------------
# Per-host runner (dispatches to local bash or SSH)
# ---------------------------------------------------------------------------
run_on_host() {
    local host="$1"
    local exit_code=0
    local label="$host"

    if [[ "$LOCAL_MODE" == true ]]; then
        label="local"
        echo "[$label] Running..."
        local output
        output=$(get_remote_script | DRY_RUN=${DRY_RUN} VERBOSE=${VERBOSE} bash -s 2>&1) \
            || exit_code=$?
    else
        echo "[$label] Connecting..."
        local output
        output=$(get_remote_script | ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
            "DRY_RUN=${DRY_RUN} VERBOSE=${VERBOSE} bash -s" 2>&1) \
            || exit_code=$?
    fi

    # Process captured output.
    # For failed hosts, always show all output so the user can see why.
    local show_all=false
    [[ $exit_code -ne 0 ]] && show_all=true

    local line
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        if [[ "$VERBOSE" == true || "$show_all" == true ]]; then
            echo "[$label] $line"
        elif [[ "$line" == *"[ERROR]"* || "$line" == *"[WARN]"* || \
                "$line" == *"[CHANGE]"* || "$line" == *"[DRY-RUN]"* ]]; then
            echo "[$label] $line"
        fi
    done <<< "$output"

    if [[ "$LOCAL_MODE" != true && $exit_code -eq 255 ]]; then
        HOST_STATUS["$host"]="FAILED"
        HOST_MESSAGE["$host"]="SSH connection failed (exit 255)"
        echo "[$label] FAILED (SSH connection)"
    elif [[ $exit_code -ne 0 ]]; then
        HOST_STATUS["$host"]="FAILED"
        HOST_MESSAGE["$host"]="Script exited with code $exit_code"
        echo "[$label] FAILED (exit $exit_code)"
    else
        HOST_STATUS["$host"]="SUCCESS"
        HOST_MESSAGE["$host"]="OK"
        echo "[$label] SUCCESS"
    fi
    echo ""
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print_summary() {
    local total=${#HOSTS[@]}
    local succeeded=0 failed=0

    for host in "${HOSTS[@]}"; do
        case "${HOST_STATUS[$host]:-UNKNOWN}" in
            SUCCESS) succeeded=$(( succeeded + 1 )) ;;
            FAILED)  failed=$(( failed + 1 )) ;;
        esac
    done

    echo "=== Summary ==="
    echo "Succeeded: $succeeded / $total"
    echo "Failed:    $failed"

    if [[ $failed -gt 0 ]]; then
        echo ""
        echo "FAILED hosts:"
        for host in "${HOSTS[@]}"; do
            if [[ "${HOST_STATUS[$host]:-}" == "FAILED" ]]; then
                echo "  $host  ->  ${HOST_MESSAGE[$host]}"
            fi
        done
    fi
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    parse_args "$@"
    validate_inputs
    if [[ "$LOCAL_MODE" != true ]]; then
        build_ssh_opts
    fi
    confirm_execution

    if [[ "$LOCAL_MODE" == true ]]; then
        log "Running GDS configuration locally..."
    else
        log "Starting GDS configuration for ${#HOSTS[@]} host(s)..."
    fi
    echo ""

    for host in "${HOSTS[@]}"; do
        run_on_host "$host"
    done

    print_summary
}

main "$@"
