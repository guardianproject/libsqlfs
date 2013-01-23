#include <sys/time.h>
#include "common.c"

#define WRITESZ 2*1048576

int main(int argc, char *argv[])
{
    char *database_filename = "c_perf_key.db";
    int rc;
    sqlfs_t *sqlfs = 0;

    if(argc > 1)
      database_filename = argv[1];
    if(exists(database_filename))
       printf("%s exists.\n", database_filename);
    printf("Opening %s\n", database_filename);
    sqlfs_init(database_filename);
    printf("Running tests:\n");

    rc = sqlfs_open_key(database_filename, "mysupersafepassword", &sqlfs);
    printf("Opening database...");
    assert(rc);
    printf("passed\n");

    run_perf_tests(sqlfs, WRITESZ);

    printf("Closing database...");
    sqlfs_close(sqlfs);
    printf("done\n");
    return 0;
}

