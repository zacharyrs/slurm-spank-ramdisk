# SLURM SPANK RAM Disk

This plugin adds support for the automatic creation of job or job-step RAM disks to SLURM.

## Usage

The plugin adds an option `--ramdisk` to `sbatch`/`srun`, which sequesters a portion of the job's (or step's) memory allocation for a temporary filesystem.
The value specified can be suffixed with `M` for megabytes (default), or `G` for gigabytes.

The plugin relies on your SLURM installation using CGroups for memory constraints.
It creates the temporary filesystem within the CGroup, which means any RAM disk usage is counted as memory usage from your allocation.
It is thus important to increase the job memory requested alongside the `--ramdisk` option.

During runtime, the path is stored under the environment variable `SLURM_JOB_RAMDISK`.
At job (or step) completion, the temporary filesystem is removed and all data within it is discarded.

## Compilation and Installation

The plugin can be compiled easily with `gcc`, and should be copied somewhere in your SLURM libs folder:

```bash
gcc -shared -fPIC -o ramdisk.so ramdisk.c
sudo mkdir -p /usr/local/lib/slurm/spank
sudo cp ramdisk.so /usr/local/lib/slurm/spank/ramdisk.so
```

You must also edit `plugstack.conf` to include the plugin:

```text
required /usr/local/lib/slurm/spank/ramdisk.so
```
