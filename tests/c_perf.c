#include <sys/time.h>
#include "common.c"

#define WRITESZ 2*1048576

int main(int argc, char *argv[])
{
    char *database_filename = "c_perf.db";
    int rc;
    sqlfs_t *sqlfs = 0;

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

    run_perf_tests(sqlfs, WRITESZ);

    printf("Closing database...");
    sqlfs_close(sqlfs);
    printf("done\n");

    printf("\n------------------------------------------------------------------------\n");
    printf("Running tests using the thread API, i.e. sqlfs == 0:\n");
    run_perf_tests(0, WRITESZ);

    return 0;
}

