#include <sys/time.h>
#include "common.c"

#define WRITESZ 2*1048576
//#define WRITESZ 32768
#define STARTSZ 256
#define ENDSZ 32768

#define TIMING(t1,t2) \
  (double) (t2.tv_usec - t1.tv_usec)/1000000 + \
  (double) (t2.tv_sec - t1.tv_sec)

void test_write_n_bytes_nosleep(sqlfs_t *sqlfs, int testsize)
{
    char testfilename[PATH_MAX];
    char randomdata[testsize];
    struct fuse_file_info fi = { 0 };
    randomfilename(testfilename, PATH_MAX, "write_n_bytes");
    sqlfs_proc_write(sqlfs, testfilename, randomdata, testsize, 0, &fi);
}

void test_read_n_bytes_nosleep(sqlfs_t *sqlfs, const char* testfilename, int testsize)
{
    char randomdata[testsize];
    struct fuse_file_info fi = { 0 };
    sqlfs_proc_read(sqlfs, testfilename, randomdata, sizeof(randomdata), 0, &fi);
}

int main(int argc, char *argv[])
{
    char *database_filename = "c_perf.db";
    int rc, size, i;
    sqlfs_t *sqlfs = 0;
    struct timeval tstart, tstop;
    char testfilename[PATH_MAX];
    char randomdata[WRITESZ];
    struct fuse_file_info fi = { 0 };

    if(argc > 1)
      database_filename = argv[1];
    if(exists(database_filename))
       printf("%s exists.\n", database_filename);
    printf("Opening %s\n", database_filename);
    sqlfs_init(database_filename);
    printf("Running tests:\n");

    rc = sqlfs_open(database_filename, &sqlfs);
    printf("Opening database...");
    assert(rc);
    printf("passed\n");

    randomfilename(testfilename, PATH_MAX, "read_n_bytes");
    sqlfs_proc_write(sqlfs, testfilename, randomdata, WRITESZ, 0, &fi);
    printf("reads without transactions ------------------------------\n");
    for (size=STARTSZ; size <= ENDSZ ; size *= 2) {
        gettimeofday(&tstart, NULL);
        for(i = 0; i < WRITESZ / size; i++) {
            test_read_n_bytes_nosleep(sqlfs, testfilename, size);
        }
        gettimeofday(&tstop, NULL);
        printf("* read %d bytes in %d %d byte chunks in \t%f seconds\n",
               WRITESZ, WRITESZ / size, size, TIMING(tstart,tstop));
    }
    printf("reads with transactions ------------------------------\n");
    for (size=STARTSZ; size <= ENDSZ ; size *= 2) {
        gettimeofday(&tstart, NULL);
        sqlfs_begin_transaction(sqlfs);
        for(i = 0; i < WRITESZ / size; i++) {
            test_read_n_bytes_nosleep(sqlfs, testfilename, size);
        }
        sqlfs_complete_transaction(sqlfs,1);
        gettimeofday(&tstop, NULL);
        printf("* read %d bytes in %d %d byte chunks in \t%f seconds\n",
               WRITESZ, WRITESZ / size, size, TIMING(tstart,tstop));
    }
    printf("writes without transactions ------------------------------\n");
    for (size=STARTSZ; size <= ENDSZ ; size *= 2) {
        gettimeofday(&tstart, NULL);
        for(i = 0; i < WRITESZ / size; i++) {
          test_write_n_bytes_nosleep(sqlfs, size);
        }
        gettimeofday(&tstop, NULL);
        printf("* wrote %d bytes in %d %d byte chunks in \t%f seconds\n",
          WRITESZ, WRITESZ / size, size, TIMING(tstart,tstop));
    }
    printf("writes with transactions ------------------------------\n");
    for (size=STARTSZ; size <= ENDSZ ; size *= 2) {
        gettimeofday(&tstart, NULL);
        sqlfs_begin_transaction(sqlfs);
        for(i = 0; i < WRITESZ / size; i++) {
          test_write_n_bytes_nosleep(sqlfs, size);
        }
        sqlfs_complete_transaction(sqlfs,1);
        gettimeofday(&tstop, NULL);
        printf("* wrote %d bytes in %d %d byte chunks in \t%f seconds\n",
          WRITESZ, WRITESZ / size, size, TIMING(tstart,tstop));
    }

    printf("Closing database...");
    sqlfs_close(sqlfs);
    printf("done\n");
    return 0;
}

