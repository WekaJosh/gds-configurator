#!/usr/bin/env bash
# configure_gds.sh — Configure and validate NVIDIA GDS for storage access
#
# Usage: configure_gds.sh [-l] [-c] [-t type] [ip_file|ip ...] [-u user] [-k keyfile] [-p port] [-d] [-v] [-f]
#
#   -l            Run on the local system (no SSH)
#   -c            Validate: check GDS configuration and parse gdscheck -p output
#   -t type       Storage backend: weka,lustre,nfs,beegfs,gpfs,scatefs,nvme,auto (default: auto)
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
SCRIPT_VERSION="2.0.0"
VALID_BACKENDS="weka lustre nfs beegfs gpfs scatefs nvme"
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
VALIDATE_MODE=false
BACKEND_TYPE="auto"

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
Usage: $0 [-l] [-c] [-t type] [ip_file|ip ...] [-u user] [-k keyfile] [-p port] [-d] [-v] [-f]

  -l            Run on the local system (no SSH, no hosts needed)
  -c            Validate: check GDS config and run gdscheck -p
  -t type       Storage backend (default: auto)
                  weka, lustre, nfs, beegfs, gpfs, scatefs, nvme, auto
                  Comma-separated for multiple: -t lustre,nvme
  ip_file       File with one IP address per line (# comments and blank lines ok)
  ip [ip ...]   One or more IP addresses/hostnames passed directly

  -u user   SSH username            (default: root)
  -k file   SSH private key file
  -p port   SSH port                (default: 22)
  -d        Dry-run (no writes)
  -v        Verbose (show [INFO] lines)
  -f        Skip confirmation prompt

Examples:
  $0 -l                             # configure local (auto-detect backend)
  $0 -l -t weka                     # configure local for WEKA
  $0 -l -c                          # validate the local system
  $0 hosts.txt -u ubuntu -t lustre  # configure remote hosts for Lustre
  $0 hosts.txt -u ubuntu            # auto-detect and configure
  $0 hosts.txt -u ubuntu -c         # validate remote hosts
  $0 -l -t nvme -d -v               # NVMe P2P dry-run locally
  $0 hosts.txt -t lustre,nvme -d -v # multi-backend dry-run

Version: $SCRIPT_VERSION
EOF
    exit 1
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
parse_args() {
    # Pre-scan for -l and -c flags so we know whether positional args are required.
    local arg
    for arg in "$@"; do
        [[ "$arg" == "-l" ]] && LOCAL_MODE=true
        [[ "$arg" == "-c" ]] && VALIDATE_MODE=true
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
    while getopts ":u:k:p:t:dvflc" opt; do
        case "$opt" in
            u) SSH_USER="$OPTARG" ;;
            k) SSH_KEYFILE="$OPTARG" ;;
            p) SSH_PORT="$OPTARG" ;;
            t) BACKEND_TYPE="$OPTARG" ;;
            d) DRY_RUN=true ;;
            v) VERBOSE=true ;;
            f) FORCE=true ;;
            l) LOCAL_MODE=true ;;
            c) VALIDATE_MODE=true ;;
            :) err "Option -$OPTARG requires an argument."; usage ;;
            \?) err "Unknown option: -$OPTARG"; usage ;;
        esac
    done

    # Validate backend type(s)
    if [[ "$BACKEND_TYPE" != "auto" ]]; then
        local b
        for b in ${BACKEND_TYPE//,/ }; do
            local valid=false
            local vb
            for vb in $VALID_BACKENDS; do
                if [[ "$b" == "$vb" ]]; then valid=true; break; fi
            done
            if [[ "$valid" != true ]]; then
                err "Unknown backend type: $b (valid: $VALID_BACKENDS)"
                usage
            fi
        done
    fi

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

confirm_apply() {
    if [[ "$FORCE" == true ]]; then return; fi
    echo ""
    read -r -p "Apply these changes? [y/N] " answer
    case "$answer" in
        [yY][eE][sS]|[yY]) ;;
        *) echo "Aborted."; exit 0 ;;
    esac
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
    # Management interfaces (carrying the default route) are excluded.
    declare -ga RDMA_ADDR_LIST=()

    # Identify management interface(s) — those carrying the default route.
    # These are not part of the storage data plane and should be excluded.
    declare -A MGMT_IFACES=()
    local def_iface
    while IFS= read -r def_iface; do
        [[ -n "$def_iface" ]] && MGMT_IFACES["$def_iface"]=1
    done < <(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev") print $(i+1)}')

    if [[ ${#MGMT_IFACES[@]} -gt 0 ]]; then
        echo "[INFO]  Management interface(s) (default route): ${!MGMT_IFACES[*]} — will exclude from GDS"
    fi

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

            # Skip management interfaces (carry the default route)
            if [[ -n "${MGMT_IFACES[$iface]+x}" ]]; then
                echo "[INFO]  $dev_name -> $iface (management interface, skipping)"
                continue
            fi

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

# ============================================================
# Backend profile functions
# Each backend defines: needs_rdma, needs_mount_table, mount_type
# Configuration and validation are dispatched per-backend.
# ============================================================

# --- WEKA ---
backend_weka_needs_rdma()       { echo "true"; }
backend_weka_needs_mount_table() { echo "false"; }
backend_weka_mount_type()       { echo "wekafs"; }
backend_weka_gdscheck_name()    { echo "WekaFS"; }

# --- Lustre ---
backend_lustre_needs_rdma()       { echo "true"; }
backend_lustre_needs_mount_table() { echo "true"; }
backend_lustre_mount_type()       { echo "lustre"; }
backend_lustre_gdscheck_name()    { echo "Lustre"; }

# --- NFS (includes VAST) ---
backend_nfs_needs_rdma()       { echo "true"; }
backend_nfs_needs_mount_table() { echo "true"; }
backend_nfs_mount_type()       { echo "nfs"; }
backend_nfs_gdscheck_name()    { echo "NFS"; }

# --- BeeGFS ---
backend_beegfs_needs_rdma()       { echo "true"; }
backend_beegfs_needs_mount_table() { echo "true"; }
backend_beegfs_mount_type()       { echo "beegfs"; }
backend_beegfs_gdscheck_name()    { echo "BeeGFS"; }

# --- GPFS (IBM Spectrum Scale) ---
backend_gpfs_needs_rdma()       { echo "true"; }
backend_gpfs_needs_mount_table() { echo "true"; }
backend_gpfs_mount_type()       { echo "gpfs"; }
backend_gpfs_gdscheck_name()    { echo "IBM Spectrum Scale"; }

# --- ScaTeFS ---
backend_scatefs_needs_rdma()       { echo "true"; }
backend_scatefs_needs_mount_table() { echo "true"; }
backend_scatefs_mount_type()       { echo "scatefs"; }
backend_scatefs_gdscheck_name()    { echo "ScaTeFS"; }

# --- NVMe (local P2P) ---
backend_nvme_needs_rdma()       { echo "false"; }
backend_nvme_needs_mount_table() { echo "false"; }
backend_nvme_mount_type()       { echo ""; }
backend_nvme_gdscheck_name()    { echo "NVMe"; }

# ============================================================
# Auto-detection and mount discovery
# ============================================================

auto_detect_backends() {
    # Scan the system for active storage backends.
    # Sets ACTIVE_BACKENDS array.
    declare -ga ACTIVE_BACKENDS=()

    # Check for WEKA
    if mount -t wekafs 2>/dev/null | grep -q wekafs || command -v weka &>/dev/null; then
        ACTIVE_BACKENDS+=("weka")
        echo "[INFO]  Auto-detected: weka"
    fi

    # Check for Lustre
    if mount -t lustre 2>/dev/null | grep -q lustre; then
        ACTIVE_BACKENDS+=("lustre")
        echo "[INFO]  Auto-detected: lustre"
    fi

    # Check for NFS (includes VAST which uses NFS)
    if mount -t nfs 2>/dev/null | grep -q nfs || mount -t nfs4 2>/dev/null | grep -q nfs4; then
        ACTIVE_BACKENDS+=("nfs")
        echo "[INFO]  Auto-detected: nfs"
    fi

    # Check for BeeGFS
    if mount -t beegfs 2>/dev/null | grep -q beegfs || mount -t fuse.beegfs 2>/dev/null | grep -q beegfs; then
        ACTIVE_BACKENDS+=("beegfs")
        echo "[INFO]  Auto-detected: beegfs"
    fi

    # Check for GPFS
    if mount -t gpfs 2>/dev/null | grep -q gpfs; then
        ACTIVE_BACKENDS+=("gpfs")
        echo "[INFO]  Auto-detected: gpfs"
    fi

    # Check for ScaTeFS
    if mount -t scatefs 2>/dev/null | grep -q scatefs; then
        ACTIVE_BACKENDS+=("scatefs")
        echo "[INFO]  Auto-detected: scatefs"
    fi

    # Check for NVMe devices on GPU PCIe buses
    if [[ -d /sys/class/nvme ]]; then
        local nvme_count
        nvme_count=$(ls -d /sys/class/nvme/nvme* 2>/dev/null | wc -l)
        if [[ "$nvme_count" -gt 0 ]]; then
            ACTIVE_BACKENDS+=("nvme")
            echo "[INFO]  Auto-detected: nvme ($nvme_count controller(s))"
        fi
    fi

    if [[ ${#ACTIVE_BACKENDS[@]} -eq 0 ]]; then
        echo "[WARN]  No storage backends auto-detected"
        echo "[WARN]  Use -t to specify a backend explicitly"
    fi
}

discover_mounts() {
    # Discover mount points for a given filesystem type.
    # Sets DISCOVERED_MOUNTS array.
    local backend="$1"
    declare -ga DISCOVERED_MOUNTS=()

    local mount_type
    mount_type=$(backend_${backend}_mount_type)
    [[ -z "$mount_type" ]] && return

    # Scan /proc/mounts for matching filesystem types
    local types=("$mount_type")
    # Add variant types
    case "$backend" in
        nfs) types+=("nfs4") ;;
        beegfs) types+=("fuse.beegfs") ;;
    esac

    local t mount_point
    for t in "${types[@]}"; do
        while IFS=' ' read -r _ mount_point _ fs_type _; do
            if [[ "$fs_type" == "$t" ]]; then
                DISCOVERED_MOUNTS+=("$mount_point")
            fi
        done < /proc/mounts
    done

    if [[ ${#DISCOVERED_MOUNTS[@]} -gt 0 ]]; then
        echo "[INFO]  Discovered ${#DISCOVERED_MOUNTS[@]} $backend mount(s): ${DISCOVERED_MOUNTS[*]}"
    else
        echo "[INFO]  No active $backend mounts found"
    fi
}

detect_nvme_p2p_devices() {
    # Detect NVMe controllers on the same PCIe root complex as GPUs.
    declare -ga NVME_P2P_DEVS=()

    if [[ ! -d /sys/class/nvme ]]; then
        echo "[WARN]  /sys/class/nvme does not exist — no NVMe controllers found"
        return
    fi

    # Build GPU root bus set (reuses GPU_NUMA_NODES from detect_gpu_numa_nodes)
    declare -A GPU_PCIE_ROOTS=()
    local bus_id
    for bus_id in "${!GPU_NUMA_NODES[@]}"; do
        local root
        root=$(echo "$bus_id" | cut -d: -f1-2)
        GPU_PCIE_ROOTS["$root"]=1
    done

    local nvme_dev
    for nvme_dev in /sys/class/nvme/nvme*; do
        [[ -d "$nvme_dev" ]] || continue
        local dev_name
        dev_name=$(basename "$nvme_dev")
        local pci_path="${nvme_dev}/device"

        if [[ -L "$pci_path" ]]; then
            local pci_addr nvme_root
            pci_addr=$(basename "$(readlink -f "$pci_path")")
            nvme_root=$(echo "$pci_addr" | cut -d: -f1-2)

            if [[ -n "${GPU_PCIE_ROOTS[$nvme_root]+x}" ]]; then
                NVME_P2P_DEVS+=("$dev_name")
                echo "[INFO]  $dev_name: PCIe root $nvme_root matches a GPU -> P2P capable"
            else
                echo "[INFO]  $dev_name: PCIe root $nvme_root does not match any GPU"
            fi
        fi
    done

    if [[ ${#NVME_P2P_DEVS[@]} -eq 0 ]]; then
        echo "[WARN]  No NVMe controllers found on GPU PCIe buses"
    else
        echo "[INFO]  GPU-local NVMe controllers: ${NVME_P2P_DEVS[*]}"
    fi
}

# Helper: check if any active backend needs RDMA
any_backend_needs_rdma() {
    local b
    for b in "${ACTIVE_BACKENDS[@]}"; do
        if [[ $(backend_${b}_needs_rdma) == "true" ]]; then
            return 0
        fi
    done
    return 1
}

# Helper: check if any active backend needs mount tables
any_backend_needs_mount_table() {
    local b
    for b in "${ACTIVE_BACKENDS[@]}"; do
        if [[ $(backend_${b}_needs_mount_table) == "true" ]]; then
            return 0
        fi
    done
    return 1
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

    # Build backends and mount table data for the merge script
    local backends_csv
    backends_csv=$(IFS=,; echo "${ACTIVE_BACKENDS[*]}")
    local mount_json="{}"
    if any_backend_needs_mount_table; then
        # Build mount table JSON: {"lustre": ["/mnt/lustre1"], "nfs": ["/mnt/data"]}
        mount_json=$(python3 -c "
import json, sys, os
mt = {}
for b in '${backends_csv}'.split(','):
    mounts_var = os.environ.get(f'MOUNTS_{b.upper()}', '')
    if mounts_var:
        mt[b] = mounts_var.split(':')
print(json.dumps(mt))
" 2>/dev/null || echo "{}")
    fi

    if command -v python3 &>/dev/null; then
        JSON_TOOL="python3"
        NEW_JSON=$(ACTIVE_BACKENDS_CSV="$backends_csv" MOUNT_TABLE_JSON="$mount_json" \
            python3 - "$TMPJSON" ${RDMA_ADDR_LIST[@]+"${RDMA_ADDR_LIST[@]}"} <<'PYEOF'
import json, sys, os

tmpfile = sys.argv[1]
new_devs = sys.argv[2:]
active_backends = os.environ.get("ACTIVE_BACKENDS_CSV", "weka").split(",")
mount_table = json.loads(os.environ.get("MOUNT_TABLE_JSON", "{}"))

with open(tmpfile) as f:
    try:
        current = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON: {e}", file=sys.stderr)
        sys.exit(1)

changes = []
props = current.setdefault("properties", {})
fs = current.setdefault("fs", {})

# --- Common RDMA properties (for any RDMA backend) ---
rdma_backends = {"weka", "lustre", "nfs", "beegfs", "gpfs", "scatefs"}
has_rdma = any(b in rdma_backends for b in active_backends)

if has_rdma:
    # rdma_dev_addr_list: merge detected IPs
    existing = props.get("rdma_dev_addr_list", [])
    if not isinstance(existing, list):
        print(f"[WARN]  rdma_dev_addr_list was not a list, resetting to []", file=sys.stderr)
        existing = []
    merged = list(existing)
    for d in new_devs:
        if d not in merged:
            merged.append(d)
            changes.append(f"rdma_dev_addr_list: added {d}")
    props["rdma_dev_addr_list"] = merged

    # Compat mode must be false for GDS direct path
    for key in ("use_compat_mode", "allow_compat_mode"):
        if props.get(key) is not False:
            props[key] = False
            changes.append(f"{key}: set to false")

    # RDMA write support
    if props.get("gds_rdma_write_support") is not True:
        props["gds_rdma_write_support"] = True
        changes.append("gds_rdma_write_support: set to true")

# --- Per-backend fs.* configuration ---
for backend in active_backends:
    sect = fs.setdefault(backend, {}) if backend != "nvme" else None

    if backend == "weka":
        if sect.get("rdma_write_support") is not True:
            sect["rdma_write_support"] = True
            changes.append("fs.weka.rdma_write_support: set to true")

    elif backend == "lustre":
        if "posix_gds_min_kb" not in sect:
            sect["posix_gds_min_kb"] = 0
            changes.append("fs.lustre.posix_gds_min_kb: set to 0")

    elif backend == "nfs":
        pass  # NFS config is mount_table only

    elif backend == "beegfs":
        if "posix_gds_min_kb" not in sect:
            sect["posix_gds_min_kb"] = 0
            changes.append("fs.beegfs.posix_gds_min_kb: set to 0")

    elif backend == "gpfs":
        if sect.get("gds_write_support") is not True:
            sect["gds_write_support"] = True
            changes.append("fs.gpfs.gds_write_support: set to true")
        if sect.get("gds_async_support") is not True:
            sect["gds_async_support"] = True
            changes.append("fs.gpfs.gds_async_support: set to true")

    elif backend == "scatefs":
        if "posix_gds_min_kb" not in sect:
            sect["posix_gds_min_kb"] = 0
            changes.append("fs.scatefs.posix_gds_min_kb: set to 0")

    elif backend == "nvme":
        if props.get("use_pci_p2pdma") is not True:
            props["use_pci_p2pdma"] = True
            changes.append("properties.use_pci_p2pdma: set to true")

    # Mount table entries (for backends that need them)
    if backend in mount_table and mount_table[backend]:
        mt = sect.setdefault("mount_table", {})
        for mpoint in mount_table[backend]:
            entry = mt.setdefault(mpoint, {})
            existing_mt = entry.get("rdma_dev_addr_list", [])
            if not isinstance(existing_mt, list):
                existing_mt = []
            merged_mt = list(existing_mt)
            for d in new_devs:
                if d not in merged_mt:
                    merged_mt.append(d)
            if merged_mt != existing_mt:
                entry["rdma_dev_addr_list"] = merged_mt
                changes.append(f"fs.{backend}.mount_table.{mpoint}: updated rdma_dev_addr_list")

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

        # Build jq filter dynamically based on active backends
        local jq_filter=""

        # RDMA properties if any RDMA backend
        if any_backend_needs_rdma; then
            jq_filter+='.properties.rdma_dev_addr_list = ((.properties.rdma_dev_addr_list // []) + $new_devs | unique) | '
            jq_filter+='.properties.use_compat_mode = false | '
            jq_filter+='.properties.allow_compat_mode = false | '
            jq_filter+='.properties.gds_rdma_write_support = true | '
        fi

        # Per-backend fs.* config
        local b
        for b in "${ACTIVE_BACKENDS[@]}"; do
            case "$b" in
                weka)    jq_filter+='.fs.weka.rdma_write_support = true | ' ;;
                lustre)  jq_filter+='.fs.lustre.posix_gds_min_kb = (.fs.lustre.posix_gds_min_kb // 0) | ' ;;
                beegfs)  jq_filter+='.fs.beegfs.posix_gds_min_kb = (.fs.beegfs.posix_gds_min_kb // 0) | ' ;;
                gpfs)    jq_filter+='.fs.gpfs.gds_write_support = true | .fs.gpfs.gds_async_support = true | ' ;;
                scatefs) jq_filter+='.fs.scatefs.posix_gds_min_kb = (.fs.scatefs.posix_gds_min_kb // 0) | ' ;;
                nvme)    jq_filter+='.properties.use_pci_p2pdma = true | ' ;;
                nfs)     ;; # NFS config is mount_table only
            esac
        done

        # Remove trailing " | "
        jq_filter="${jq_filter% | }"
        # Default to identity if empty
        [[ -z "$jq_filter" ]] && jq_filter="."

        NEW_JSON=$(jq --argjson new_devs "$devs_json" "$jq_filter" "$TMPJSON") \
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

    # Check that GDS (nvidia-fs) is installed; install if missing
    ensure_gds_installed

    # Step 2: GPU detection (needed by all backends for PCIe topology)
    detect_gpu_numa_nodes || exit 1

    # Step 3: Determine active backends
    declare -ga ACTIVE_BACKENDS=()
    if [[ "$BACKEND_TYPE" == "auto" ]]; then
        auto_detect_backends
        if [[ ${#ACTIVE_BACKENDS[@]} -eq 0 ]]; then
            echo "[ERROR] No storage backends detected. Use -t to specify one."
            exit 1
        fi
    else
        # Split comma-separated backend list
        local b
        for b in ${BACKEND_TYPE//,/ }; do
            ACTIVE_BACKENDS+=("$b")
        done
        echo "[INFO]  Configured backend(s): ${ACTIVE_BACKENDS[*]}"
    fi

    # Step 4: RDMA path (only if any active backend needs it)
    if any_backend_needs_rdma; then
        if [[ ! -d /sys/class/infiniband ]]; then
            echo "[ERROR] /sys/class/infiniband does not exist — no RDMA subsystem found"
            echo "[ERROR] RDMA is required for backend(s): ${ACTIVE_BACKENDS[*]}"
            exit 1
        fi

        detect_mlx_devices || exit 1

        find_gpu_resident_devs
        if [[ ${#GPU_RESIDENT_DEVS[@]} -eq 0 ]]; then
            echo "[WARN]  No GPU-resident RDMA devices found"
        else
            resolve_rdma_addresses
        fi

        ensure_nvidia_peermem
        ensure_rdma_library
    fi

    # Step 5: NVMe P2P path (only if nvme backend is active)
    local b
    for b in "${ACTIVE_BACKENDS[@]}"; do
        if [[ "$b" == "nvme" ]]; then
            detect_nvme_p2p_devices
            break
        fi
    done

    # Step 6: Discover mount points for backends that need mount tables
    for b in "${ACTIVE_BACKENDS[@]}"; do
        if [[ $(backend_${b}_needs_mount_table) == "true" ]]; then
            discover_mounts "$b"
            # Export discovered mounts for Python merge via env var
            if [[ ${#DISCOVERED_MOUNTS[@]} -gt 0 ]]; then
                local mounts_joined
                mounts_joined=$(IFS=:; echo "${DISCOVERED_MOUNTS[*]}")
                export "MOUNTS_${b^^}=${mounts_joined}"
            fi
        fi
    done

    # Step 7: Read/create cufile.json
    read_or_create_cufile

    # Step 8: Merge and write
    merge_and_write || exit 1

    exit 0
}

main_remote

REMOTE_SCRIPT
}

# ---------------------------------------------------------------------------
# Validation script body (emitted by get_validate_script)
# ---------------------------------------------------------------------------
get_validate_script() {
    cat <<'VALIDATE_SCRIPT'

HAS_FAIL=false

pass() { echo "[PASS]  $*"; }
fail() { echo "[FAIL]  $*"; HAS_FAIL=true; }
warn() { echo "[WARN]  $*"; }
info() { echo "[INFO]  $*"; }

# --- System checks ---
echo "=== System Checks ==="

# nvidia_fs module
if lsmod | grep -q nvidia_fs; then
    pass "nvidia_fs module: loaded"
else
    fail "nvidia_fs module: not loaded"
fi

# nvidia_peermem module
if lsmod | grep -q nvidia_peermem; then
    pass "nvidia_peermem module: loaded"
else
    fail "nvidia_peermem module: not loaded"
fi

# libcufile_rdma.so
if ldconfig -p 2>/dev/null | grep -q libcufile_rdma.so; then
    pass "libcufile_rdma.so: found in linker cache"
else
    fail "libcufile_rdma.so: not in linker cache"
fi

# /etc/cufile.json exists
if [[ -f /etc/cufile.json ]]; then
    pass "/etc/cufile.json: exists"
else
    fail "/etc/cufile.json: missing"
fi

echo ""

# --- gdscheck -p ---
GDSCHECK=""
for candidate in \
    /usr/local/cuda/gds/tools/gdscheck \
    /usr/local/cuda/tools/gdscheck \
    $(command -v gdscheck 2>/dev/null || true); do
    if [[ -x "$candidate" ]]; then
        GDSCHECK="$candidate"
        break
    fi
done

if [[ -z "$GDSCHECK" ]]; then
    fail "gdscheck: not found (checked /usr/local/cuda/gds/tools/ and PATH)"
    if [[ "$HAS_FAIL" == true ]]; then exit 1; else exit 0; fi
fi

info "Running: $GDSCHECK -p"
echo ""

GDS_OUTPUT=$("$GDSCHECK" -p 2>&1) || true

echo "=== GDS Validation ==="

# Helper: extract a value from gdscheck output
gds_val() {
    local pattern="$1"
    # Use [^:]* (first colon only) — greedy .* would eat into values like "Up: 16 Down: 0"
    echo "$GDS_OUTPUT" | grep -i "$pattern" | head -1 | sed 's/^[^:]*: *//'
}

# Determine active backends for validation
ACTIVE_BACKENDS=()
if [[ "$BACKEND_TYPE" == "auto" ]]; then
    # In validation mode, detect from cufile.json fs.* sections
    if [[ -f /etc/cufile.json ]] && command -v python3 &>/dev/null; then
        detected=$(python3 -c "
import json, re
try:
    text = open('/etc/cufile.json').read()
    # Strip comments
    result = []; i = 0; in_str = False
    while i < len(text):
        if in_str:
            if text[i] == '\\\\' and i+1 < len(text): result.append(text[i:i+2]); i += 2; continue
            if text[i] == '\"': in_str = False
            result.append(text[i]); i += 1
        else:
            if text[i] == '\"': in_str = True; result.append(text[i]); i += 1
            elif i+1 < len(text) and text[i:i+2] == '//':
                while i < len(text) and text[i] != '\n': i += 1
            elif i+1 < len(text) and text[i:i+2] == '/*':
                i += 2
                while i+1 < len(text) and text[i:i+2] != '*/': i += 1
                if i+1 < len(text): i += 2
            else: result.append(text[i]); i += 1
    d = json.loads(''.join(result))
    backends = []
    fs = d.get('fs', {})
    for b in ('weka','lustre','nfs','beegfs','gpfs','scatefs'):
        if b in fs: backends.append(b)
    props = d.get('properties', {})
    if props.get('use_pci_p2pdma'): backends.append('nvme')
    print(','.join(backends) if backends else '')
except: pass
" 2>/dev/null)
        if [[ -n "$detected" ]]; then
            for b in ${detected//,/ }; do ACTIVE_BACKENDS+=("$b"); done
            info "Auto-detected configured backends: ${ACTIVE_BACKENDS[*]}"
        fi
    fi
    # Fallback: validate common checks only
    if [[ ${#ACTIVE_BACKENDS[@]} -eq 0 ]]; then
        info "Could not auto-detect backends, running universal checks only"
    fi
else
    for b in ${BACKEND_TYPE//,/ }; do ACTIVE_BACKENDS+=("$b"); done
fi

# Determine if any active backend needs RDMA
RDMA_BACKENDS="weka lustre nfs beegfs gpfs scatefs"
HAS_RDMA_BACKEND=false
for b in ${ACTIVE_BACKENDS[@]+"${ACTIVE_BACKENDS[@]}"}; do
    for rb in $RDMA_BACKENDS; do
        if [[ "$b" == "$rb" ]]; then HAS_RDMA_BACKEND=true; break 2; fi
    done
done

# --- Per-backend filesystem support check ---
for b in ${ACTIVE_BACKENDS[@]+"${ACTIVE_BACKENDS[@]}"}; do
    case "$b" in
        weka)
            val=$(gds_val "WekaFS")
            if [[ "$val" == *"Supported"* ]]; then pass "WekaFS: $val"
            else fail "WekaFS: $val (expected: Supported)"; fi
            val=$(gds_val "weka.rdma_write_support")
            if [[ "$val" == *"true"* ]]; then pass "fs.weka.rdma_write_support: $val"
            else fail "fs.weka.rdma_write_support: $val (expected: true)"; fi
            ;;
        lustre)
            val=$(gds_val "Lustre")
            if [[ "$val" == *"Supported"* ]]; then pass "Lustre: $val"
            else fail "Lustre: $val (expected: Supported)"; fi
            ;;
        nfs)
            val=$(gds_val "NFS")
            if [[ "$val" == *"Supported"* ]]; then pass "NFS: $val"
            else fail "NFS: $val (expected: Supported)"; fi
            ;;
        beegfs)
            val=$(gds_val "BeeGFS")
            if [[ "$val" == *"Supported"* ]]; then pass "BeeGFS: $val"
            else fail "BeeGFS: $val (expected: Supported)"; fi
            ;;
        gpfs)
            val=$(gds_val "IBM Spectrum Scale")
            if [[ "$val" == *"Supported"* ]]; then pass "GPFS: $val"
            else fail "GPFS: $val (expected: Supported)"; fi
            val=$(gds_val "gpfs.gds_write_support")
            if [[ "$val" == *"true"* ]]; then pass "fs.gpfs.gds_write_support: $val"
            else fail "fs.gpfs.gds_write_support: $val (expected: true)"; fi
            ;;
        scatefs)
            val=$(gds_val "ScaTeFS")
            if [[ "$val" == *"Supported"* ]]; then pass "ScaTeFS: $val"
            else fail "ScaTeFS: $val (expected: Supported)"; fi
            ;;
        nvme)
            val=$(gds_val "NVMe P2PDMA")
            if [[ "$val" == *"Supported"* ]]; then pass "NVMe P2PDMA: $val"
            else warn "NVMe P2PDMA: $val"; fi
            val=$(gds_val "use_pci_p2pdma")
            if [[ "$val" == *"true"* ]]; then pass "use_pci_p2pdma: $val"
            else fail "use_pci_p2pdma: $val (expected: true)"; fi
            ;;
    esac
done

# --- RDMA checks (only if an RDMA backend is active) ---
if [[ "$HAS_RDMA_BACKEND" == true ]]; then
    echo ""
    echo "=== RDMA Checks ==="

    val=$(gds_val "Userspace RDMA")
    if [[ "$val" == *"Supported"* ]]; then pass "Userspace RDMA: $val"
    else fail "Userspace RDMA: $val (expected: Supported)"; fi

    val=$(gds_val "Mellanox PeerDirect")
    if [[ "$val" == *"Enabled"* ]]; then pass "Mellanox PeerDirect: $val"
    else fail "Mellanox PeerDirect: $val (expected: Enabled)"; fi

    val=$(gds_val "rdma library")
    if [[ "$val" == *"Loaded"* ]]; then pass "RDMA library: $val"
    else fail "RDMA library: $val (expected: Loaded)"; fi

    val=$(gds_val "rdma devices")
    if [[ "$val" == *"Configured"* ]]; then pass "RDMA devices: $val"
    else fail "RDMA devices: $val (expected: Configured)"; fi

    status_line=$(gds_val "rdma_device_status")
    up_count=$(echo "$status_line" | grep -o 'Up: *[0-9]*' | grep -o '[0-9]*' || echo "0")
    down_count=$(echo "$status_line" | grep -o 'Down: *[0-9]*' | grep -o '[0-9]*' || echo "0")
    if [[ "$down_count" -eq 0 && "$up_count" -gt 0 ]]; then
        pass "RDMA device status: Up: $up_count Down: $down_count"
    elif [[ "$down_count" -gt 0 && "$up_count" -gt 0 ]]; then
        warn "RDMA device status: Up: $up_count Down: $down_count"
    else
        fail "RDMA device status: Up: $up_count Down: $down_count (no devices up)"
    fi

    val=$(gds_val "use_compat_mode")
    if [[ "$val" == *"false"* ]]; then pass "use_compat_mode: $val"
    else fail "use_compat_mode: $val (expected: false)"; fi

    val=$(gds_val "gds_rdma_write_support")
    if [[ "$val" == *"true"* ]]; then pass "gds_rdma_write_support: $val"
    else fail "gds_rdma_write_support: $val (expected: true)"; fi
fi

# Platform verification
platform_line=$(echo "$GDS_OUTPUT" | grep -i "Platform verification" | tail -1)
if [[ "$platform_line" == *"succeeded"* ]]; then
    pass "Platform verification: succeeded"
else
    fail "Platform verification: failed"
fi

# GPU GDS support
echo ""
echo "=== GPU Status ==="
gpu_count=$(echo "$GDS_OUTPUT" | grep -c "supports GDS" || true)
gpu_total=$(echo "$GDS_OUTPUT" | grep -c "GPU index" || true)
if [[ "$gpu_total" -eq 0 ]]; then
    fail "No GPUs detected"
elif [[ "$gpu_count" -eq "$gpu_total" ]]; then
    pass "All $gpu_total GPU(s) support GDS"
else
    warn "Only $gpu_count / $gpu_total GPU(s) support GDS"
fi

echo ""
if [[ "$HAS_FAIL" == true ]]; then
    echo "RESULT: FAIL — one or more checks failed"
    exit 1
else
    echo "RESULT: PASS — GDS is properly configured"
    exit 0
fi

VALIDATE_SCRIPT
}

# ---------------------------------------------------------------------------
# Parallel phase runner
# ---------------------------------------------------------------------------
run_phase() {
    local dry_run_val="$1"  # "true" or "false"
    local phase="$2"        # "plan" or "apply"

    local tmpdir
    tmpdir=$(mktemp -d /tmp/gds_config.XXXXXX)
    local -a pids=()
    local i=0

    # Build the remote command — use sudo when not connecting as root
    local remote_cmd="DRY_RUN=${dry_run_val} BACKEND_TYPE=${BACKEND_TYPE} bash -s"
    if [[ "$LOCAL_MODE" != true && "$SSH_USER" != "root" ]]; then
        remote_cmd="sudo DRY_RUN=${dry_run_val} BACKEND_TYPE=${BACKEND_TYPE} bash -s"
    fi

    # Launch all hosts in parallel
    for host in "${HOSTS[@]}"; do
        (
            set +e  # prevent errexit from hiding results in subshell
            local ec=0 output
            if [[ "$LOCAL_MODE" == true ]]; then
                output=$(get_remote_script | DRY_RUN="${dry_run_val}" bash -s 2>&1) || ec=$?
            else
                output=$(get_remote_script | ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
                    "$remote_cmd" 2>&1) || ec=$?
            fi
            printf '%s\n' "$output" > "$tmpdir/${i}.out"
            echo "$ec" > "$tmpdir/${i}.exit"
        ) &
        pids+=($!)
        i=$(( i + 1 ))
    done

    # Wait for all background jobs
    wait "${pids[@]}" 2>/dev/null || true

    # Collect and display results
    i=0
    for host in "${HOSTS[@]}"; do
        local label="$host"
        if [[ "$LOCAL_MODE" == true ]]; then label="local"; fi

        local output exit_code has_changes=false
        output=$(cat "$tmpdir/${i}.out" 2>/dev/null || echo "")
        exit_code=$(cat "$tmpdir/${i}.exit" 2>/dev/null || echo "1")

        echo "[$label]"

        # Display output lines based on verbosity and phase
        local show_all=false
        [[ "$exit_code" != "0" ]] && show_all=true

        local line
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            if [[ "$VERBOSE" == true || "$show_all" == true ]]; then
                echo "  $line"
            elif [[ "$line" == *"[ERROR]"* || "$line" == *"[WARN]"* || \
                    "$line" == *"[CHANGE]"* || "$line" == *"[DRY-RUN]"* || \
                    "$line" == *"No changes needed"* ]]; then
                echo "  $line"
            fi
            if [[ "$line" == *"[DRY-RUN]"* || "$line" == *"[CHANGE]"* ]]; then
                has_changes=true
            fi
        done <<< "$output"

        # Record host status
        if [[ "$LOCAL_MODE" != true && "$exit_code" == "255" ]]; then
            HOST_STATUS["$host"]="FAILED"
            HOST_MESSAGE["$host"]="SSH connection failed (exit 255)"
            echo "  -> FAILED (SSH connection)"
        elif [[ "$exit_code" != "0" ]]; then
            HOST_STATUS["$host"]="FAILED"
            HOST_MESSAGE["$host"]="Script exited with code $exit_code"
            echo "  -> FAILED (exit $exit_code)"
        elif [[ "$has_changes" == true ]]; then
            if [[ "$phase" == "plan" ]]; then
                HOST_STATUS["$host"]="CHANGES"
            else
                HOST_STATUS["$host"]="SUCCESS"
            fi
            HOST_MESSAGE["$host"]="OK"
        else
            HOST_STATUS["$host"]="OK"
            HOST_MESSAGE["$host"]="No changes needed"
        fi
        echo ""

        i=$(( i + 1 ))
    done

    rm -rf "$tmpdir"
}

# ---------------------------------------------------------------------------
# Validation runner — parallel, uses get_validate_script
# ---------------------------------------------------------------------------
run_validate() {
    local tmpdir
    tmpdir=$(mktemp -d /tmp/gds_validate.XXXXXX)
    local -a pids=()
    local i=0

    # Build remote command — use sudo when not root, pass BACKEND_TYPE
    local remote_cmd="BACKEND_TYPE=${BACKEND_TYPE} bash -s"
    if [[ "$LOCAL_MODE" != true && "$SSH_USER" != "root" ]]; then
        remote_cmd="sudo BACKEND_TYPE=${BACKEND_TYPE} bash -s"
    fi

    for host in "${HOSTS[@]}"; do
        (
            set +e
            local ec=0 output
            if [[ "$LOCAL_MODE" == true ]]; then
                output=$(get_validate_script | BACKEND_TYPE="${BACKEND_TYPE}" bash -s 2>&1) || ec=$?
            else
                output=$(get_validate_script | ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
                    "$remote_cmd" 2>&1) || ec=$?
            fi
            printf '%s\n' "$output" > "$tmpdir/${i}.out"
            echo "$ec" > "$tmpdir/${i}.exit"
        ) &
        pids+=($!)
        i=$(( i + 1 ))
    done

    wait "${pids[@]}" 2>/dev/null || true

    # Display results
    i=0
    for host in "${HOSTS[@]}"; do
        local label="$host"
        if [[ "$LOCAL_MODE" == true ]]; then label="local"; fi

        local output exit_code
        output=$(cat "$tmpdir/${i}.out" 2>/dev/null || echo "")
        exit_code=$(cat "$tmpdir/${i}.exit" 2>/dev/null || echo "1")

        echo "========== [$label] =========="

        # Show all output for failed hosts so SSH errors etc. are visible
        local show_all=false
        [[ "$exit_code" != "0" ]] && show_all=true

        local line
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            if [[ "$VERBOSE" == true || "$show_all" == true ]]; then
                echo "  $line"
            elif [[ "$line" == *"[PASS]"* || "$line" == *"[FAIL]"* || \
                    "$line" == *"[WARN]"* || "$line" == *"RESULT:"* || \
                    "$line" == "==="* ]]; then
                echo "  $line"
            fi
        done <<< "$output"

        if [[ "$LOCAL_MODE" != true && "$exit_code" == "255" ]]; then
            HOST_STATUS["$host"]="FAILED"
            HOST_MESSAGE["$host"]="SSH connection failed (exit 255)"
        elif [[ "$exit_code" != "0" ]]; then
            HOST_STATUS["$host"]="FAILED"
            HOST_MESSAGE["$host"]="Validation failed"
        else
            HOST_STATUS["$host"]="OK"
            HOST_MESSAGE["$host"]="All checks passed"
        fi
        echo ""

        i=$(( i + 1 ))
    done

    rm -rf "$tmpdir"
}

print_validate_summary() {
    local total=${#HOSTS[@]}
    local passed=0 failed=0

    for host in "${HOSTS[@]}"; do
        case "${HOST_STATUS[$host]:-UNKNOWN}" in
            OK)     passed=$(( passed + 1 )) ;;
            FAILED) failed=$(( failed + 1 )) ;;
        esac
    done

    echo "=== Validation Summary ==="
    echo "Passed:  $passed / $total"
    echo "Failed:  $failed"

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
# Summary
# ---------------------------------------------------------------------------
print_summary() {
    local total=${#HOSTS[@]}
    local succeeded=0 failed=0 up_to_date=0

    for host in "${HOSTS[@]}"; do
        case "${HOST_STATUS[$host]:-UNKNOWN}" in
            SUCCESS) succeeded=$(( succeeded + 1 )) ;;
            OK)      up_to_date=$(( up_to_date + 1 )) ;;
            FAILED)  failed=$(( failed + 1 )) ;;
        esac
    done

    echo "=== Summary ==="
    echo "Applied:     $succeeded"
    echo "Up to date:  $up_to_date"
    echo "Failed:      $failed"

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

    # Validate mode — run checks and exit
    if [[ "$VALIDATE_MODE" == true ]]; then
        if [[ "$LOCAL_MODE" == true ]]; then
            log "Validating GDS configuration on local system..."
        else
            log "Validating GDS configuration on ${#HOSTS[@]} host(s) in parallel..."
        fi
        echo ""

        run_validate
        print_validate_summary

        # Exit with failure if any host failed validation
        local vfail
        for vfail in "${HOSTS[@]}"; do
            if [[ "${HOST_STATUS[$vfail]:-}" == "FAILED" ]]; then
                return 1
            fi
        done
        return 0
    fi

    # Phase 1: Plan — dry-run all hosts in parallel to discover changes
    if [[ "$LOCAL_MODE" == true ]]; then
        log "Querying local system for needed changes..."
    else
        log "Querying ${#HOSTS[@]} host(s) in parallel..."
    fi
    echo ""

    run_phase "true" "plan"

    # Tally plan results
    local changes=0 up_to_date=0 failed=0
    for host in "${HOSTS[@]}"; do
        case "${HOST_STATUS[$host]:-}" in
            CHANGES) changes=$(( changes + 1 )) ;;
            OK)      up_to_date=$(( up_to_date + 1 )) ;;
            FAILED)  failed=$(( failed + 1 )) ;;
        esac
    done

    echo "=== Plan ==="
    echo "  Changes needed:    $changes"
    echo "  Already up to date: $up_to_date"
    if [[ $failed -gt 0 ]]; then
        echo "  Failed to query:   $failed"
    fi

    # If -d (dry-run only), stop after showing the plan
    if [[ "$DRY_RUN" == true ]]; then
        echo ""
        return 0
    fi

    # Nothing to apply
    if [[ $changes -eq 0 ]]; then
        echo ""
        echo "Nothing to apply."
        return 0
    fi

    # Prompt before applying (unless -f)
    confirm_apply
    echo ""

    # Phase 2: Apply — run all hosts in parallel for real
    HOST_STATUS=()
    HOST_MESSAGE=()

    if [[ "$LOCAL_MODE" == true ]]; then
        log "Applying changes locally..."
    else
        log "Applying changes to ${#HOSTS[@]} host(s) in parallel..."
    fi
    echo ""

    run_phase "false" "apply"

    print_summary
}

main "$@"
