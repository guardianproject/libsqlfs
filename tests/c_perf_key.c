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

    printf("Opening %s...", database_filename);
    rc = sqlfs_open_password(database_filename, "mysupersafepassword", &sqlfs);
    assert(rc);
    printf("passed\n");

    run_perf_tests(sqlfs, WRITESZ);

    printf("Closing database...");
    rc = sqlfs_close(sqlfs);
    assert(rc);
    printf("done\n");


    printf("\n------------------------------------------------------------------------\n");
    printf("Running tests using the thread API, i.e. sqlfs == 0:\n");

    printf("Initing %s\n", database_filename);
    rc = sqlfs_init_password(database_filename, "mysupersafepassword");
    assert(rc == 0);

    run_perf_tests(0, WRITESZ);

    printf("Destroying:\n");
    rc = sqlfs_destroy();
    assert(rc == 0);

    rc++; // silence ccpcheck

    return 0;
}


/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
