/*
 * ramdisk.c : Adds `--ramdisk` functionality to SLURM
 *
 * gcc -shared -fPIC -o ramdisk.so ramdisk.c
 *
 * plugstack.conf:
 * required /usr/local/lib/slurm/spank/ramdisk.so
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <slurm/spank.h>
#include <slurm/slurm.h>
#include <sys/mount.h>
#include <sys/stat.h>

SPANK_PLUGIN (ramdisk, 1);

uint64_t ramdisk_size;

int parse_ramdisk_size(int val, const char *optarg, int remote) {
    char ramdisk_unit[2];
    sscanf(optarg, "%lu%1s", &ramdisk_size, ramdisk_unit);

    if (strcmp(ramdisk_unit, "G") == 0) {
        ramdisk_size *= 1024;
        strcpy(ramdisk_unit, "M");
        slurm_verbose("ramdisk.c: converting size to megabytes");
    } else if ((strcmp(ramdisk_unit, "M") == 0) || (strcmp(ramdisk_unit, "") == 0)) {
        slurm_verbose("ramdisk.c: size in M or unspecified (default to M)");
    } else {
        slurm_error("ramdisk.c: invalid --ramdisk unit '%s'", ramdisk_unit);
        return ESPANK_ERROR;
    }

    slurm_verbose("ramdisk.c: ramdisk size is %luM", ramdisk_size);
    return ESPANK_SUCCESS;
}

struct spank_option spank_options[] = {
    {
        "ramdisk",
        "N[MG]",
        "Create a RAM disk of N (MB, GB), allocating as a portion of the memory requested.",
        1,
        0,
        (spank_opt_cb_f) parse_ramdisk_size
    },
    SPANK_OPTIONS_TABLE_END
};

int get_directory(uint32_t job_id, uint32_t job_stepid, char** directory) {
    if (job_stepid > SLURM_MAX_NORMAL_STEP_ID) {
        if (job_stepid == SLURM_PENDING_STEP) {
            slurm_error("ramdisk.c: cannot create ramdisk for pending step");
            return -1;
        } else if (job_stepid == SLURM_EXTERN_CONT) {
            asprintf(directory, "/ramdisks/%u.extern.ramdisk", job_id);
        } else if (job_stepid == SLURM_BATCH_SCRIPT){
            asprintf(directory, "/ramdisks/%u.batch.ramdisk", job_id);
        } else if (job_stepid == SLURM_INTERACTIVE_STEP) {
            asprintf(directory, "/ramdisks/%u.interactive.ramdisk", job_id);
        } else{
            slurm_error("ramdisk.c: invalid job step id: %u", job_stepid);
            return -1;
        }
    } else {
        asprintf(directory, "/ramdisks/%u.%u.ramdisk", job_id, job_stepid);
    }
    return 0;
}

int slurm_spank_init_post_opt (spank_t sp, int ac, char **av) {
    if (spank_context() != S_CTX_REMOTE) {
        return ESPANK_SUCCESS;
    }

    // get job ID and job step ID
    uint32_t job_id;
    uint32_t job_stepid;
    if (spank_get_item(sp, S_JOB_ID, &job_id) != ESPANK_SUCCESS) {
        slurm_error("ramdisk.c: failed to get job ID");
        return ESPANK_ERROR;
    }
    if (spank_get_item(sp, S_JOB_STEPID, &job_stepid) != ESPANK_SUCCESS) {
        slurm_error("ramdisk.c: failed to get job step ID");
        return ESPANK_ERROR;
    }

    // check memory allocation exceeds ramdisk size
    uint64_t step_memory_allocation;
    if (spank_get_item(sp, S_STEP_ALLOC_MEM, &step_memory_allocation) != ESPANK_SUCCESS) {
        slurm_error("ramdisk.c: failed to get step memory allocation");
        return ESPANK_ERROR;
    }
    if (step_memory_allocation <= ramdisk_size) {
        slurm_error("ramdisk.c: cannot create ramdisk of size %luM when allocated %luM", ramdisk_size, step_memory_allocation);
        return ESPANK_ERROR;
    }

    // get directory path
    char *directory;
    if (get_directory(job_id, job_stepid, &directory) != 0) {
        return ESPANK_ERROR;
    }
    slurm_verbose("ramdisk.c: using directory %s", directory);

    // set environment variable
    if (spank_setenv(sp, "SLURM_JOB_RAMDISK", directory, 1) != ESPANK_SUCCESS){
        slurm_error("ramdisk.c: unable to set SLURM_JOB_RAMDISK=%s", directory);
    }

    // get UID and GID for our mount (before we start doing actual filesystem operations)
    uid_t uid;
    gid_t gid;
    if (spank_get_item(sp, S_JOB_UID, &uid) != ESPANK_SUCCESS) {
        slurm_error("ramdisk.c: failed to get job UID");
        return ESPANK_ERROR;
    }
    if (spank_get_item(sp, S_JOB_GID, &gid) != ESPANK_SUCCESS) {
        slurm_error("ramdisk.c: failed to get job GID");
        gid = -1;
    }

    // Now we do the actual filesystem operations
    slurm_info("ramdisk.c: creating a ramdisk - %luM at %s", ramdisk_size, directory);

    // check if the directory exists - if it does assume we're done
    struct stat sb;
    if (stat(directory, &sb) == 0) {
        if (!S_ISDIR(sb.st_mode)) {
            slurm_error("ramdisk.c: directory path exists but is not dir");
            return ESPANK_ERROR;
        }
        slurm_verbose("ramdisk.c: directory path exists, assuming we've already mounted it");
        return ESPANK_SUCCESS;
    }

    // create the ramdisk directory
    if (mkdir(directory, 0700) != 0) {
        slurm_error("ramdisk.c: failed to create directory");
        return ESPANK_ERROR;
    }

    // mount tmpfs
    char *mount_options;
    asprintf(&mount_options, "size=%luM,uid=%d,gid=%d,mode=700", ramdisk_size, uid, gid);
    if (mount("none", directory, "tmpfs", 0, mount_options) != 0) {
        slurm_error("ramdisk.c: failed to mount tmpfs");
        return ESPANK_ERROR;
    }

    return ESPANK_SUCCESS;
}

int slurm_spank_exit (spank_t sp, int ac, char **av) {
    if (spank_context() != S_CTX_REMOTE) {
        return ESPANK_SUCCESS;
    }

    // get job ID and job step ID
    uint32_t job_id;
    uint32_t job_stepid;
    if (spank_get_item(sp, S_JOB_ID, &job_id) != ESPANK_SUCCESS) {
        slurm_error("ramdisk.c: failed to get job ID");
        return ESPANK_ERROR;
    }
    if (spank_get_item(sp, S_JOB_STEPID, &job_stepid) != ESPANK_SUCCESS) {
        slurm_error("ramdisk.c: failed to get job step ID");
        return ESPANK_ERROR;
    }

    // get directory path
    char *directory;
    if (get_directory(job_id, job_stepid, &directory) != 0) {
        return ESPANK_ERROR;
    }
    slurm_verbose("ramdisk.c: using directory %s", directory);

    slurm_info("ramdisk.c: deleting the ramdisk - %s", directory);

    // check if the directory exists - if it doesn't assume we're done
    struct stat sb;
    if (stat(directory, &sb) == -1) {
        slurm_verbose("ramdisk.c: directory path missing, assuming we've already deleted it");
        return ESPANK_SUCCESS;
    }

    // unmount tmpfs
    if (umount(directory) != 0) {
        slurm_error("ramdisk.c: failed to unmount tmpfs, attempting to drain node");
        // ideally need a nicer way to drain the node...
        system("scontrol update nodename=$(hostname -s) state=DRAIN reason='failed to unmount ramdisk'");
        return ESPANK_ERROR;
    }

    // delete directory path
    if (rmdir(directory) != 0) {
        slurm_error("ramdisk.c: failed to delete tmpfs directory");
    }

    return ESPANK_SUCCESS;
}
