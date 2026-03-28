# configure_gds.sh

Automated NVIDIA GPU Direct Storage (GDS) configuration for WEKA filesystems. Configures one or more Linux hosts — locally or via SSH — so that `gdscheck -p` reports a fully working GDS + RDMA setup.

## What it does

The script runs in two phases:

1. **Plan** — dry-runs all targets in parallel, showing what would change
2. **Apply** — after user confirmation, applies changes in parallel

On each host it:

- Installs GDS packages (`nvidia-gds`) if missing, matched to the installed CUDA version
- Loads `nvidia_fs` and `nvidia_peermem` kernel modules
- Ensures `libcufile_rdma.so` is in the linker path
- Detects GPU-resident Mellanox (mlx5) RDMA interfaces via NUMA topology
- Resolves those interfaces to their IPv4 addresses
- Excludes management interfaces (those carrying the default route)
- Updates `/etc/cufile.json`:
  - `properties.rdma_dev_addr_list` — data-plane IPs
  - `properties.use_compat_mode` / `allow_compat_mode` — set to `false`
  - `properties.gds_rdma_write_support` — set to `true`
  - `fs.weka.rdma_write_support` — set to `true`
- Handles NVIDIA's stock `cufile.json` with C-style `//` comments
- Backs up the original config before writing (`/etc/cufile.json.bak.<timestamp>`)

## Usage

```
configure_gds.sh [-l] [ip_file|ip ...] [-u user] [-k keyfile] [-p port] [-d] [-v] [-f]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-l` | — | Run on the local system (no SSH) |
| `-u user` | `root` | SSH username (uses `sudo` automatically if not root) |
| `-k file` | — | SSH private key file |
| `-p port` | `22` | SSH port |
| `-d` | — | Dry-run: plan only, make no changes |
| `-v` | — | Verbose: show `[INFO]` detection lines per host |
| `-f` | — | Force: skip the confirmation prompt between plan and apply |

### Targets

Hosts can be supplied as a file, inline IPs, or both:

```
ip_file       Text file with one IP/hostname per line (# comments and blank lines ok)
ip [ip ...]   One or more IPs/hostnames directly on the command line
```

## Examples

```bash
# Configure the local machine
sudo ./configure_gds.sh -l

# Dry-run locally with verbose output
sudo ./configure_gds.sh -l -d -v

# Configure remote hosts from a file
./configure_gds.sh hosts.txt -u ubuntu -k ~/.ssh/id_rsa

# Configure specific hosts inline
./configure_gds.sh 10.0.0.1 10.0.0.2 10.0.0.3 -u ubuntu

# Skip confirmation (for automation)
./configure_gds.sh hosts.txt -u ubuntu -f

# Plan only — see what would change without applying
./configure_gds.sh hosts.txt -u ubuntu -d -v
```

## Example output

```
[09:15:22] Querying 3 host(s) in parallel...

[10.0.68.50]
  [DRY-RUN] Would load nvidia_fs kernel module
  [DRY-RUN] Would backup /etc/cufile.json to /etc/cufile.json.bak.20260328_091522
  [DRY-RUN] Would write new config to /etc/cufile.json
  [DRY-RUN] New rdma_dev_addr_list: ['10.224.4.50', '10.224.20.50', ...]

[10.0.68.51]
  [INFO]  No changes needed — configuration already up to date

[10.0.68.52]
  [DRY-RUN] Would install GDS packages for CUDA 12.6
  [DRY-RUN] Would write new config to /etc/cufile.json

=== Plan ===
  Changes needed:    2
  Already up to date: 1

Apply these changes? [y/N] y

[09:15:30] Applying changes to 3 host(s) in parallel...
...
=== Summary ===
Applied:     2
Up to date:  1
Failed:      0
```

## Requirements

**Controller (where you run the script):**
- bash 4.2+
- ssh client

**Target hosts:**
- NVIDIA GPU driver installed (`nvidia-smi` in PATH)
- RDMA-capable NICs (`/sys/class/infiniband/` present)
- python3 or jq (for JSON manipulation; python3 preferred)
- Passwordless sudo if connecting as a non-root user

## How GPU-resident detection works

The script identifies which RDMA NICs are co-located with GPUs on the same PCIe domain:

1. Gets GPU PCIe bus IDs via `nvidia-smi` and maps them to NUMA nodes via sysfs
2. Gets mlx5 device NUMA nodes from `/sys/class/infiniband/*/device/numa_node`
3. Matches by NUMA node — mlx5 devices on the same NUMA node as a GPU are GPU-resident
4. Falls back to PCIe root complex matching if NUMA info is unavailable (all nodes report -1)
5. Resolves matched mlx5 devices to network interface IPv4 addresses
6. Excludes the management interface (the one carrying the default route)
