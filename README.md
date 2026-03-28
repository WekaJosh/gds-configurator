# configure_gds.sh

Automated NVIDIA GPU Direct Storage (GDS) configuration for any supported storage backend. Configures one or more Linux hosts — locally or via SSH — so that `gdscheck -p` reports a fully working GDS setup.

## Supported backends

| Backend | Type | Description |
|---------|------|-------------|
| `weka` | RDMA | WekaFS with GPU-direct RDMA |
| `lustre` | RDMA | Lustre / DDN EXAScaler with per-mount RDMA |
| `nfs` | RDMA | NFS RDMA (includes VAST Data) with per-mount config |
| `beegfs` | RDMA | BeeGFS with per-mount RDMA |
| `gpfs` | RDMA | IBM Spectrum Scale / GPFS with per-mount RDMA |
| `scatefs` | RDMA | ScaTeFS with per-mount RDMA |
| `nvme` | P2P | Local NVMe via PCIe peer-to-peer DMA (no RDMA) |
| `auto` | — | Auto-detect from active mounts and hardware (default) |

Multiple backends can be configured simultaneously (e.g., `-t lustre,nvme`).

## What it does

The script runs in two phases:

1. **Plan** — dry-runs all targets in parallel, showing what would change
2. **Apply** — after user confirmation, applies changes in parallel

On each host it:

- Installs GDS packages (`nvidia-gds`) if missing, matched to the installed CUDA version
- Loads `nvidia_fs` kernel module
- Detects active backends (or uses the one specified with `-t`)
- **For RDMA backends** (weka, lustre, nfs, beegfs, gpfs, scatefs):
  - Loads `nvidia_peermem` kernel module
  - Ensures `libcufile_rdma.so` is in the linker path
  - Detects GPU-resident Mellanox (mlx5) RDMA interfaces via NUMA topology
  - Resolves those interfaces to their IPv4 addresses
  - Excludes management interfaces (those carrying the default route)
  - Discovers mount points for backends that need per-mount config
  - Updates `/etc/cufile.json`:
    - `properties.rdma_dev_addr_list` — data-plane IPs
    - `properties.use_compat_mode` / `allow_compat_mode` — set to `false`
    - `properties.gds_rdma_write_support` — set to `true`
    - Backend-specific `fs.<backend>.*` settings
    - Per-mount `mount_table` entries (lustre, nfs, beegfs, gpfs, scatefs)
- **For NVMe backend**:
  - Detects NVMe controllers on GPU PCIe buses (P2P capable)
  - Sets `properties.use_pci_p2pdma` to `true`
- Handles NVIDIA's stock `cufile.json` with C-style `//` comments
- Backs up the original config before writing (`/etc/cufile.json.bak.<timestamp>`)

## Usage

```
configure_gds.sh [-l] [-c] [-t type] [ip_file|ip ...] [-u user] [-k keyfile] [-p port] [-d] [-v] [-f]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-l` | — | Run on the local system (no SSH) |
| `-c` | — | Validate: check GDS config and parse `gdscheck -p` output |
| `-t type` | `auto` | Storage backend: `weka`, `lustre`, `nfs`, `beegfs`, `gpfs`, `scatefs`, `nvme`, `auto`. Comma-separated for multiple. |
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
# Auto-detect backend and configure locally
sudo ./configure_gds.sh -l

# Configure local system for WEKA specifically
sudo ./configure_gds.sh -l -t weka

# Configure remote hosts for Lustre
./configure_gds.sh hosts.txt -u ubuntu -t lustre

# Configure for NVMe P2P (no RDMA needed)
sudo ./configure_gds.sh -l -t nvme

# Multi-backend: Lustre + NVMe on same hosts
./configure_gds.sh hosts.txt -u ubuntu -t lustre,nvme

# Auto-detect and configure remote hosts
./configure_gds.sh hosts.txt -u ubuntu

# Dry-run with verbose output
./configure_gds.sh hosts.txt -u ubuntu -d -v

# Skip confirmation (for automation)
./configure_gds.sh hosts.txt -u ubuntu -f

# Validate local GDS configuration
sudo ./configure_gds.sh -l -c

# Validate remote hosts
./configure_gds.sh hosts.txt -u ubuntu -c

# Configure then validate
./configure_gds.sh hosts.txt -u ubuntu -t weka -f && ./configure_gds.sh hosts.txt -u ubuntu -c
```

## Example output

### Plan and apply

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

### Validation (`-c`)

```
[09:20:00] Validating GDS configuration on 2 host(s) in parallel...

========== [10.0.68.50] ==========
  === System Checks ===
  [PASS]  nvidia_fs module: loaded
  [PASS]  nvidia_peermem module: loaded
  [PASS]  libcufile_rdma.so: found in linker cache
  [PASS]  /etc/cufile.json: exists
  === GDS Validation ===
  [PASS]  WekaFS: Supported
  [PASS]  fs.weka.rdma_write_support: true
  === RDMA Checks ===
  [PASS]  Userspace RDMA: Supported
  [PASS]  Mellanox PeerDirect: Enabled
  [PASS]  RDMA library: Loaded (libcufile_rdma.so)
  [PASS]  RDMA devices: Configured
  [PASS]  RDMA device status: Up: 16 Down: 0
  [PASS]  use_compat_mode: false
  [PASS]  gds_rdma_write_support: true
  [PASS]  Platform verification: succeeded
  === GPU Status ===
  [PASS]  All 8 GPU(s) support GDS
  RESULT: PASS — GDS is properly configured

=== Validation Summary ===
Passed:  2 / 2
Failed:  0
```

## Per-backend cufile.json configuration

Each backend configures specific fields in `/etc/cufile.json`:

| Backend | `fs.*` settings | `properties.*` settings | Mount table? |
|---------|----------------|------------------------|-------------|
| weka | `fs.weka.rdma_write_support = true` | RDMA common* | No |
| lustre | `fs.lustre.posix_gds_min_kb = 0` | RDMA common* | Yes |
| nfs | *(none beyond mount table)* | RDMA common* | Yes |
| beegfs | `fs.beegfs.posix_gds_min_kb = 0` | RDMA common* | Yes |
| gpfs | `fs.gpfs.gds_write_support = true`, `gds_async_support = true` | RDMA common* | Yes |
| scatefs | `fs.scatefs.posix_gds_min_kb = 0` | RDMA common* | Yes |
| nvme | *(none)* | `use_pci_p2pdma = true` | No |

*RDMA common = `rdma_dev_addr_list`, `use_compat_mode = false`, `allow_compat_mode = false`, `gds_rdma_write_support = true`

**Mount table** backends auto-discover active mount points and create per-mount `rdma_dev_addr_list` entries:

```json
{
  "fs": {
    "lustre": {
      "posix_gds_min_kb": 0,
      "mount_table": {
        "/mnt/lustre1": {
          "rdma_dev_addr_list": ["10.224.4.50", "10.224.20.50"]
        }
      }
    }
  }
}
```

## Requirements

**Controller (where you run the script):**
- bash 4.2+
- ssh client

**Target hosts:**
- NVIDIA GPU driver installed (`nvidia-smi` in PATH)
- RDMA-capable NICs for RDMA backends (`/sys/class/infiniband/` present)
- NVMe controllers for NVMe backend (`/sys/class/nvme/` present)
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

For NVMe, a similar PCIe topology match identifies NVMe controllers on the same PCIe root buses as GPUs for P2P DMA capability.

## Auto-detection

When no `-t` flag is given (default: `auto`), the script scans the target system:

- **Mounts**: checks `mount -t <type>` for lustre, nfs, nfs4, beegfs, fuse.beegfs, gpfs, scatefs, wekafs
- **WEKA agent**: checks if the `weka` CLI is available
- **NVMe**: checks for NVMe controllers in `/sys/class/nvme/`

All detected backends are configured in a single pass. Use `-t` to restrict to specific backends.
