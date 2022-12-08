/*
 * ramdisk.c : Adds `--ramdisk` functionality to SLURM
 *
 * gcc -shared -fPIC -o ramdisk.so ramdisk.c
 *
 * plugstack.conf:
 * required /usr/local/lib/slurm/spank/ramdisk.so
 */
#include <inttypes.h>
#include <slurm/slurm.h>
#include <slurm/spank.h>
#include <stdlib.h>
#include <sys/mount.h>
#include <sys/stat.h>

#define DIRECTORY_PATH_LEN 255
#define INITIAL_DIR_MODE_RWX 0700

#define MOUNT_OPTION_LEN 255
#define MOUNT_SOURCE_VIRTUAL "none"
#define MOUNT_TYPE_TEMP "tmpfs"
#define MOUNT_FLAGS_NONE 0

#define UNIT_MEGABYTES 'M'
#define UNIT_GIGABYTES 'G'

#define SPANK_PLUGIN_NAME "ramdisk"
#define SPANK_OPTION_NAME "ramdisk"

// in plugstack.h, but not spank.h
#define SPANK_OPTION_ENV_PREFIX "_SLURM_SPANK_OPTION_"

// this one is hardcoded in the SLURM source...
#define SPANK_PROPAGATION_PREFIX "SLURM_SPANK_"

// plugin definition for SPANK - we can't use the `SPANK_PLUGIN_NAME` definition
// as `SPANK_PLUGIN` is also a macro
SPANK_PLUGIN("ramdisk", 1);

static uint64_t ramdisk_size;

static int parse_ramdisk_size(int val, const char *optarg, int remote);
static int get_directory(spank_t sp, char directory[]);

// TODO: doxygen, mention hook for SPANK
int slurm_spank_init(spank_t sp, int ac, char **av) {
  // drop ramdisk size environment variable - don't want to pass it to `srun`
  spank_unsetenv(sp, SPANK_PROPAGATION_PREFIX SPANK_OPTION_ENV_PREFIX
                 "_" SPANK_PLUGIN_NAME "__" SPANK_OPTION_NAME);

  // drop the ramdisk path environment variable - it'll be set if we have one
  spank_unsetenv(sp, "SLURM_JOB_RAMDISK");

  spank_context_t context = spank_context();

  if (context == S_CTX_ALLOCATOR || context == S_CTX_REMOTE ||
      context == S_CTX_LOCAL) {
    // register the `--ramdisk` option for allocator (sbatch), remote (compute
    // node steps), and local (srun, prior to offloading to remote)
    struct spank_option ramdisk_option = {
        .name = SPANK_OPTION_NAME,
        .arginfo = "N[MG]",
        .usage = "Create a RAM disk of N (MB, GB), allocating as "
                 "a portion of the memory requested.",
        .has_arg = 1,
        .val = 0,
        .cb = (spank_opt_cb_f)parse_ramdisk_size};

    return spank_option_register(sp, &ramdisk_option);
  }

  return ESPANK_SUCCESS;
}

// TODO: doxygen, mention hook for SPANK
int slurm_spank_init_post_opt(spank_t sp, int ac, char **av) {
  if (spank_context() != S_CTX_REMOTE) {
    // ensure we only perform mounts on the remote - i.e., on the compute node
    return ESPANK_SUCCESS;
  }

  if (ramdisk_size == 0) {
    // we've been called without the `--ramdisk` argument
    slurm_verbose("ramdisk.c: called without the ramdisk argument");
    return ESPANK_SUCCESS;
  }

  // check memory allocation exceeds ramdisk size
  // the ramdisk debits from the memory allocation, hence if greater or equal
  // there will be no memory for the job itself
  uint64_t step_memory_allocation;
  if (spank_get_item(sp, S_STEP_ALLOC_MEM, &step_memory_allocation) !=
      ESPANK_SUCCESS) {
    slurm_error("ramdisk.c: failed to get step memory allocation");
    return ESPANK_ERROR;
  }
  if (step_memory_allocation <= ramdisk_size) {
    // perhaps require ramdisk to be at least 1G less than allocation?
    slurm_error("ramdisk.c: cannot create ramdisk of size %" PRIu64
                "M when allocated %" PRIu64 "M",
                ramdisk_size, step_memory_allocation);
    return ESPANK_ERROR;
  }

  // get directory path
  char directory[DIRECTORY_PATH_LEN];
  if (get_directory(sp, directory) != EXIT_SUCCESS) {
    return ESPANK_ERROR;
  }
  slurm_verbose("ramdisk.c: using directory %s", directory);

  // set environment variable for access within the compute job
  if (spank_setenv(sp, "SLURM_JOB_RAMDISK", directory, 1) != ESPANK_SUCCESS) {
    slurm_error("ramdisk.c: unable to set SLURM_JOB_RAMDISK=%s", directory);
  }

  // get UID and GID for our mount (before we start doing actual filesystem
  // operations)
  uid_t uid;
  gid_t gid;
  if (spank_get_item(sp, S_JOB_UID, &uid) != ESPANK_SUCCESS) {
    slurm_error("ramdisk.c: failed to get job UID");
    return ESPANK_ERROR;
  }
  if (spank_get_item(sp, S_JOB_GID, &gid) != ESPANK_SUCCESS) {
    slurm_error("ramdisk.c: failed to get job GID");
    // don't kill the job here - we should be able to survive with only the UID
    // defined for the mount, assumes no other users access the same share
    gid = -1;
  }

  // now we do the actual filesystem operations
  slurm_info("ramdisk.c: creating a ramdisk - %" PRIu64 "M at %s", ramdisk_size,
             directory);

  // check if the directory exists - if it does assume we're done
  struct stat sb;
  if (stat(directory, &sb) == 0) {
    if (!S_ISDIR(sb.st_mode)) {
      slurm_error("ramdisk.c: directory path exists but is not dir");
      return ESPANK_ERROR;
    }
    slurm_verbose(
        "ramdisk.c: directory path exists, assuming we've already mounted it");
    return ESPANK_SUCCESS;
  }

  // create the ramdisk directory
  if (mkdir(directory, INITIAL_DIR_MODE_RWX) != 0) {
    slurm_error("ramdisk.c: failed to create directory");
    return ESPANK_ERROR;
  }

  // mount tmpfs
  char mount_options[MOUNT_OPTION_LEN];
  snprintf(mount_options, MOUNT_OPTION_LEN,
           "size=%" PRIu64 "M,uid=%d,gid=%d,mode=700", ramdisk_size, uid, gid);
  if (mount(MOUNT_SOURCE_VIRTUAL, directory, MOUNT_TYPE_TEMP, MOUNT_FLAGS_NONE,
            mount_options) != 0) {
    slurm_error("ramdisk.c: failed to mount tmpfs");
    return ESPANK_ERROR;
  }

  return ESPANK_SUCCESS;
}

// TODO: doxygen, mention hook for SPANK
int slurm_spank_exit(spank_t sp, int ac, char **av) {
  if (spank_context() != S_CTX_REMOTE) {
    // ensure we only perform mounts on the remote - i.e., on the compute node
    return ESPANK_SUCCESS;
  }

  if (ramdisk_size == 0) {
    // we've been called without the `--ramdisk` argument
    slurm_verbose("ramdisk.c: called without the ramdisk argument");
    return ESPANK_SUCCESS;
  }

  // get directory path
  char directory[DIRECTORY_PATH_LEN];
  if (get_directory(sp, directory) != EXIT_SUCCESS) {
    return ESPANK_ERROR;
  }
  slurm_verbose("ramdisk.c: using directory %s", directory);

  slurm_info("ramdisk.c: deleting the ramdisk - %s", directory);

  // check if the directory exists - if it doesn't assume we're done
  struct stat sb;
  if (stat(directory, &sb) == -1) {
    slurm_verbose(
        "ramdisk.c: directory path missing, assuming we've already deleted it");
    return ESPANK_SUCCESS;
  }

  // unmount tmpfs
  if (umount(directory) != 0) {
    slurm_error("ramdisk.c: failed to unmount tmpfs, attempting to drain node");
    // ideally need a nicer way to drain the node...
    system("scontrol update nodename=$(hostname -s) state=DRAIN reason='failed "
           "to unmount ramdisk'");
    return ESPANK_ERROR;
  }

  // delete directory path
  if (rmdir(directory) != 0) {
    slurm_error("ramdisk.c: failed to delete tmpfs directory");
  }

  return ESPANK_SUCCESS;
}

// TODO: doxygen
static int parse_ramdisk_size(int val, const char *optarg, int remote) {
  char ramdisk_unit;
  int n_args = sscanf(optarg, "%" PRIu64 "%c", &ramdisk_size, &ramdisk_unit);

  if (n_args == 1) {
    slurm_verbose("ramdisk.c: unit undefined, defaulting to megabytes");
  } else if (n_args == 2) {
    if (ramdisk_unit == UNIT_GIGABYTES) {
      ramdisk_size *= 1024;
      ramdisk_unit = UNIT_MEGABYTES;
      slurm_verbose("ramdisk.c: converting size to megabytes");
    } else if (ramdisk_unit == UNIT_MEGABYTES) {
      slurm_verbose("ramdisk.c: size in megabytes already");
    } else {
      slurm_error("ramdisk.c: invalid --ramdisk unit '%c'", ramdisk_unit);
      return ESPANK_ERROR;
    }
  } else {
    return ESPANK_ERROR;
  }

  if (ramdisk_size == 0) {
    slurm_error("ramdisk.c: cannot have a ramdisk of 0M");
    return ESPANK_ERROR;
  }

  slurm_verbose("ramdisk.c: ramdisk size is %" PRIu64 "M", ramdisk_size);
  return ESPANK_SUCCESS;
}

// TODO: doxygen
static int get_directory(spank_t sp, char directory[]) {
  // get job ID and job step ID
  uint32_t job_id;
  uint32_t job_stepid;
  if (spank_get_item(sp, S_JOB_ID, &job_id) != ESPANK_SUCCESS) {
    slurm_error("ramdisk.c: failed to get job ID");
    return EXIT_FAILURE;
  }
  if (spank_get_item(sp, S_JOB_STEPID, &job_stepid) != ESPANK_SUCCESS) {
    slurm_error("ramdisk.c: failed to get job step ID");
    return EXIT_FAILURE;
  }

  if (job_stepid > SLURM_MAX_NORMAL_STEP_ID) {
    if (job_stepid == SLURM_PENDING_STEP) {
      slurm_error("ramdisk.c: cannot create ramdisk for pending step");
      return EXIT_FAILURE;
    } else if (job_stepid == SLURM_EXTERN_CONT) {
      snprintf(directory, DIRECTORY_PATH_LEN,
               "/ramdisks/%" PRIu32 ".extern.ramdisk", job_id);
    } else if (job_stepid == SLURM_BATCH_SCRIPT) {
      snprintf(directory, DIRECTORY_PATH_LEN,
               "/ramdisks/%" PRIu32 ".batch.ramdisk", job_id);
    } else if (job_stepid == SLURM_INTERACTIVE_STEP) {
      snprintf(directory, DIRECTORY_PATH_LEN,
               "/ramdisks/%" PRIu32 ".interactive.ramdisk", job_id);
    } else {
      slurm_error("ramdisk.c: invalid job step id: %" PRIu32, job_stepid);
      return EXIT_FAILURE;
    }
  } else {
    snprintf(directory, DIRECTORY_PATH_LEN,
             "/ramdisks/%" PRIu32 ".%" PRIu32 ".ramdisk", job_id, job_stepid);
  }
  return EXIT_SUCCESS;
}
