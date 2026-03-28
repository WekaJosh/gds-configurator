#!/usr/bin/env bash
# configure_gds.sh — Configure NVIDIA GDS rdma_dev_addr_list on remote hosts for WEKA access
#
# Usage: configure_gds.sh <ip_file|ip [ip ...]> [-u user] [-k keyfile] [-p port] [-d] [-v] [-f]
#
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
IP_FILE=""

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
Usage: $0 <ip_file|ip [ip ...]> [-u user] [-k keyfile] [-p port] [-d] [-v] [-f]

  ip_file       File with one IP address per line (# comments and blank lines ok)
  ip [ip ...]   One or more IP addresses/hostnames passed directly

  -u user   SSH username            (default: root)
  -k file   SSH private key file
  -p port   SSH port                (default: 22)
  -d        Dry-run (no writes)
  -v        Verbose (show [INFO] lines)
  -f        Skip confirmation prompt

Examples:
  $0 hosts.txt
  $0 10.0.0.1 10.0.0.2 10.0.0.3 -u admin -k ~/.ssh/id_rsa
  $0 hosts.txt 10.0.0.99 -d -v

Version: $SCRIPT_VERSION
EOF
    exit 1
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
parse_args() {
    if [[ $# -lt 1 ]]; then
        err "At least one IP address or an IP file is required."
        usage
    fi

    # Collect all positional arguments (before and between flags) as potential
    # IPs or a file. getopts stops at the first flag, so we process positionals
    # manually first, then hand off to getopts for flags.
    declare -ga RAW_TARGETS=()
    while [[ $# -gt 0 && "$1" != -* ]]; do
        RAW_TARGETS+=("$1")
        shift
    done

    if [[ ${#RAW_TARGETS[@]} -eq 0 ]]; then
        err "At least one IP address or an IP file is required."
        usage
    fi

    while getopts ":u:k:p:dvf" opt; do
        case "$opt" in
            u) SSH_USER="$OPTARG" ;;
            k) SSH_KEYFILE="$OPTARG" ;;
            p) SSH_PORT="$OPTARG" ;;
            d) DRY_RUN=true ;;
            v) VERBOSE=true ;;
            f) FORCE=true ;;
            :) err "Option -$OPTARG requires an argument."; usage ;;
            \?) err "Unknown option: -$OPTARG"; usage ;;
        esac
    done
}

add_host() {
    local h="$1"
    h="${h#"${h%%[![:space:]]*}"}"   # strip leading whitespace
    h="${h%"${h##*[![:space:]]}"}"   # strip trailing whitespace
    [[ -z "$h" || "$h" == \#* ]] && return
    HOSTS+=("$h")
}

validate_inputs() {
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
    [[ -n "$SSH_KEYFILE" ]] && SSH_OPTS+=(-i "$SSH_KEYFILE")
}

confirm_execution() {
    if [[ "$FORCE" == true ]]; then return; fi

    echo ""
    echo "=== GDS Configuration Script v${SCRIPT_VERSION} ==="
    echo "  Hosts:    ${#HOSTS[@]}"
    echo "  User:     $SSH_USER"
    echo "  Port:     $SSH_PORT"
    [[ -n "$SSH_KEYFILE" ]] && echo "  Key:      $SSH_KEYFILE"
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
# Per-host runner
# ---------------------------------------------------------------------------
run_remote() {
    local host="$1"
    local exit_code=0

    echo "[$host] Connecting..."

    # The heredoc uses a single-quoted delimiter so variables inside are NOT
    # expanded by the local shell. DRY_RUN and VERBOSE are passed as env vars
    # prefixed to the bash command.
    local output
    output=$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
        "DRY_RUN=${DRY_RUN} VERBOSE=${VERBOSE} bash -s" <<'REMOTE_SCRIPT' 2>&1) \
        || exit_code=$?

# ============================================================
# BEGIN REMOTE SCRIPT (runs on the remote host)
# ============================================================

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
        [[ -z "$raw_id" ]] && continue
        # Normalize: lowercase, 4-char domain (e.g. 00000000:3B:00.0 -> 0000:3b:00.0)
        local lower domain rest
        lower=$(echo "$raw_id" | tr '[:upper:]' '[:lower:]')
        domain="${lower%%:*}"
        rest="${lower#*:}"
        # strip leading zeros from domain, keep at least one char, then pad to 4
        domain="${domain##0}"   # simple strip won't work for "0000", use sed
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

    # Write existing JSON to a temp file for safe Python manipulation
    local TMPJSON
    TMPJSON=$(mktemp /tmp/cufile_update.XXXXXX.json)
    echo "$EXISTING_JSON" > "$TMPJSON"

    local NEW_JSON=""
    local JSON_TOOL=""

    if command -v python3 &>/dev/null; then
        JSON_TOOL="python3"
        NEW_JSON=$(python3 - "$TMPJSON" "${GPU_RESIDENT_DEVS[@]}" <<'PYEOF'
import json, sys

tmpfile = sys.argv[1]
new_devs = sys.argv[2:]

with open(tmpfile) as f:
    try:
        current = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON: {e}", file=sys.stderr)
        sys.exit(1)

props = current.setdefault("properties", {})
existing = props.get("rdma_dev_addr_list", [])
if not isinstance(existing, list):
    print(f"[WARN]  rdma_dev_addr_list was not a list ({type(existing).__name__}), resetting to []",
          file=sys.stderr)
    existing = []

merged = list(existing)
added = []
for d in new_devs:
    if d not in merged:
        merged.append(d)
        added.append(d)

props["rdma_dev_addr_list"] = merged

if not added:
    print("NO_CHANGES", file=sys.stderr)
else:
    print(f"ADDED:{','.join(added)}", file=sys.stderr)

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
        for d in "${GPU_RESIDENT_DEVS[@]}"; do
            [[ "$first" == true ]] || devs_json+=","
            devs_json+="\"${d}\""
            first=false
        done
        devs_json+="]"

        NEW_JSON=$(jq --argjson new_devs "$devs_json" \
            '.properties.rdma_dev_addr_list = ((.properties.rdma_dev_addr_list // []) + $new_devs | unique)' \
            "$TMPJSON") \
            || { rm -f "$TMPJSON"; echo "[ERROR] jq JSON manipulation failed"; return 1; }
    else
        rm -f "$TMPJSON"
        echo "[ERROR] Neither python3 nor jq is available. Cannot update JSON."
        return 1
    fi

    rm -f "$TMPJSON"
    echo "[INFO]  JSON updated using $JSON_TOOL"

    # Check if no changes were needed (python3 path signals this via stderr marker)
    # For jq path, compare old and new lists
    local no_changes=false
    if [[ "$JSON_TOOL" == "python3" ]]; then
        # NEW_JSON contains stdout; change status came via stderr to the subshell
        # Re-run a quick check by comparing old vs new rdma_dev_addr_list
        local old_list new_list
        old_list=$(echo "$EXISTING_JSON" | python3 -c \
            'import json,sys; d=json.load(sys.stdin); print(sorted(d.get("properties",{}).get("rdma_dev_addr_list",[])))' 2>/dev/null || echo "[]")
        new_list=$(echo "$NEW_JSON" | python3 -c \
            'import json,sys; d=json.load(sys.stdin); print(sorted(d.get("properties",{}).get("rdma_dev_addr_list",[])))' 2>/dev/null || echo "[]")
        [[ "$old_list" == "$new_list" ]] && no_changes=true
    fi

    if [[ "$no_changes" == true ]]; then
        echo "[INFO]  No changes needed — rdma_dev_addr_list already up to date"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        local preview
        preview=$(echo "$NEW_JSON" | python3 -c \
            'import json,sys; d=json.load(sys.stdin); print(d.get("properties",{}).get("rdma_dev_addr_list",[]))' \
            2>/dev/null || echo "(python3 unavailable for preview)")
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
    TMPOUT=$(mktemp /tmp/cufile_new.XXXXXX.json)
    echo "$NEW_JSON" > "$TMPOUT"
    mv "$TMPOUT" "$CUFILE"

    local final_list
    final_list=$(echo "$NEW_JSON" | python3 -c \
        'import json,sys; d=json.load(sys.stdin); print(d.get("properties",{}).get("rdma_dev_addr_list",[]))' \
        2>/dev/null || echo "(see $CUFILE)")
    echo "[CHANGE] $CUFILE updated. rdma_dev_addr_list: $final_list"
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

    # Step 2: GPU detection
    detect_gpu_numa_nodes || exit 1

    # Step 3: MLX device detection
    detect_mlx_devices || exit 1

    # Step 4: GPU-resident matching
    find_gpu_resident_devs
    if [[ ${#GPU_RESIDENT_DEVS[@]} -eq 0 ]]; then
        exit 0
    fi

    # Step 5: Read/create cufile.json
    read_or_create_cufile

    # Step 6 & 7: Merge and write
    merge_and_write || exit 1

    exit 0
}

main_remote

REMOTE_SCRIPT

    # Process captured output
    local line
    while IFS= read -r line; do
        if [[ "$VERBOSE" == true ]]; then
            echo "[$host] $line"
        elif [[ "$line" == *"[ERROR]"* || "$line" == *"[WARN]"* || \
                "$line" == *"[CHANGE]"* || "$line" == *"[DRY-RUN]"* ]]; then
            echo "[$host] $line"
        fi
    done <<< "$output"

    if [[ $exit_code -eq 255 ]]; then
        HOST_STATUS["$host"]="FAILED"
        HOST_MESSAGE["$host"]="SSH connection failed (exit 255)"
        echo "[$host] FAILED (SSH connection)"
    elif [[ $exit_code -ne 0 ]]; then
        HOST_STATUS["$host"]="FAILED"
        HOST_MESSAGE["$host"]="Remote script exited with code $exit_code"
        echo "[$host] FAILED (remote exit $exit_code)"
    else
        HOST_STATUS["$host"]="SUCCESS"
        HOST_MESSAGE["$host"]="OK"
        echo "[$host] SUCCESS"
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
            SUCCESS) ((succeeded++)) ;;
            FAILED)  ((failed++)) ;;
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
    build_ssh_opts
    confirm_execution

    log "Starting GDS configuration for ${#HOSTS[@]} host(s)..."
    echo ""

    for host in "${HOSTS[@]}"; do
        run_remote "$host"
    done

    print_summary
}

main "$@"
