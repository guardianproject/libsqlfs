/******************************************************************************
Copyright 2006 Palmsource, Inc (an ACCESS company).

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

*****************************************************************************/

#include <assert.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include "sqlfs.h"

#ifdef HAVE_LIBSQLCIPHER
# include "sqlcipher/sqlite3.h"
#else
# include "sqlite3.h"
#endif

#define BLOCK_SIZE 8192

char *data = "this is a string";

struct sqlfs_t
{
    sqlite3 *db;
    int transaction_level;
    int in_transaction;
    mode_t default_mode;
    
    sqlite3_stmt *stmts[200];
#ifndef HAVE_LIBFUSE
    uid_t uid;
    gid_t gid;
#endif
};

/* support functions -------------------------------------------------------- */

int exists(char *filename)
{
    FILE *file;
    if ((file = fopen(filename, "r")) == NULL) {
        if (errno == ENOENT) {
            return 0;
        } else {
            // Check for other errors too, like EACCES and EISDIR
            printf("Error occured: %d", errno);
        }
        return 0;
    } else {
        fclose(file);
    }
    return 1;
}

int create_test_file(sqlfs_t* sqlfs, char* filename, int size)
{
    int i;
    char randomdata[size];
    struct fuse_file_info fi = { 0 };
    fi.flags |= ~0;
    for (i=0; i<size; ++i)
        randomdata[i] = (i % 90) + 32;
    randomdata[size-1] = '\0';
    return sqlfs_proc_write(sqlfs, filename, randomdata, size, 0, &fi);
}

void randomfilename(char* buf, int size, char* prefix)
{
    snprintf(buf, size, "/%s-random-%i", prefix, rand());
}


/* tests -------------------------------------------------------------------- */

void test_mkdir_with_sleep(sqlfs_t *sqlfs)
{
    printf("Testing mkdir with sleep...");
    char* testfilename = "/mkdir-with-sleep0";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    sleep(1);
    assert(sqlfs_is_dir(sqlfs, testfilename));
    testfilename = "/mkdir-with-sleep1";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    sleep(1);
    assert(sqlfs_is_dir(sqlfs, testfilename));
    printf("passed\n");
}

void test_mkdir_without_sleep(sqlfs_t *sqlfs)
{
    printf("Testing mkdir without sleep...");
    char* testfilename = "/mkdir-without-sleep0";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    assert(sqlfs_is_dir(sqlfs, testfilename));
    printf("passed\n");

    printf("Testing whether mkdir does not make nested dirs...");
    testfilename = "/a/b/c/d/e/f/g";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    assert(!sqlfs_is_dir(sqlfs, testfilename));
    printf("passed\n");
}

void test_mkdir_to_make_nested_dirs(sqlfs_t *sqlfs)
{
    printf("Testing mkdir to make nested dirs one at a time...");
    sqlfs_proc_mkdir(sqlfs, "/test", 0777);
    assert(sqlfs_is_dir(sqlfs, "/test"));
    sqlfs_proc_mkdir(sqlfs, "/test/1", 0777);
    assert(sqlfs_is_dir(sqlfs, "/test/1"));
    sqlfs_proc_mkdir(sqlfs, "/test/1/2", 0777);
    assert(sqlfs_is_dir(sqlfs, "/test/1/2"));
    printf("passed\n");
}

void test_rmdir(sqlfs_t *sqlfs)
{
    printf("Testing rmdir...");
    char* testfilename = "/mkdir-to-rmdir";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    assert(sqlfs_is_dir(sqlfs, testfilename));
    sqlfs_proc_rmdir(sqlfs, testfilename);
    assert(!sqlfs_is_dir(sqlfs, testfilename));
    printf("passed\n");
}

void test_create_file_with_small_string(sqlfs_t *sqlfs)
{
    printf("Testing creating a file with a small string...");
    int i;
    char buf[200];
    struct fuse_file_info fi = { 0 };
    fi.flags |= ~0;
    sqlfs_proc_mkdir(sqlfs, "/bufdir", 0777);
    sqlfs_proc_write(sqlfs, "/bufdir/file", data, strlen(data), 0, &fi);
    assert(!sqlfs_is_dir(sqlfs, "/bufdir/file"));
    i = sqlfs_proc_read(sqlfs, "/bufdir/file", buf, sizeof(buf), 0, &fi);
    buf[i] = 0;
    assert(!strcmp(buf, data));
    printf("passed\n");
}

void test_write_n_bytes(sqlfs_t *sqlfs, int testsize)
{
    printf("Testing writing %d bytes of data...", testsize);
    int i;
    char testfilename[PATH_MAX];
    char randombuf[testsize];
    char randomdata[testsize];
    struct fuse_file_info fi = { 0 };
    randomfilename(testfilename, PATH_MAX, "write_n_bytes");
    for (i=0; i<testsize; ++i)
        randomdata[i] = (i % 90) + 32;
    randomdata[testsize-1] = 0;
    sqlfs_proc_write(sqlfs, testfilename, randomdata, testsize, 0, &fi);
    sleep(1);
    i = sqlfs_proc_read(sqlfs, testfilename, randombuf, testsize, 0, &fi);
    randombuf[i] = 0;
    assert(!strcmp(randombuf, randomdata));
    printf("passed\n");
}

void test_read_bigger_than_buffer(sqlfs_t *sqlfs)
{
    printf("Testing reading while requesting more bytes than will fit in the buffer...");
    int bufsize = 200;
    int filesize = bufsize * 4;
    char testfilename[PATH_MAX];
    char buf[bufsize];
    struct fuse_file_info fi = { 0 };
    memset(buf, 0, bufsize);
    randomfilename(testfilename, PATH_MAX, "read_bigger_than_buffer");
    create_test_file(sqlfs, testfilename, filesize);
    assert(sqlfs_proc_read(sqlfs, testfilename, buf, sizeof(buf), bufsize, &fi) == sizeof(buf));
    snprintf(testfilename, PATH_MAX, "%s", buf); // silence cppcheck
    printf("passed\n");
}

void test_read_byte_with_offset(sqlfs_t *sqlfs, int testsize)
{
    printf("Testing reading a byte with offset 10000 times...");
    int y;
    char buf[200];
    char testfilename[PATH_MAX];
    struct fuse_file_info fi = { 0 };
    memset(buf, 0, 200);
    randomfilename(testfilename, PATH_MAX, "read_byte_with_offset");
    create_test_file(sqlfs, testfilename, testsize);
    fi.flags |= ~0;
    for(y=0; y<10000; y++)
    {
        int readloc = (float)rand() / RAND_MAX * (testsize-1);
        assert(sqlfs_proc_read(sqlfs, testfilename, buf, 1, readloc, &fi) == 1);
        assert(buf[0] == (readloc % 90) + 32);
        readloc++; // silence cppcheck
    }
    snprintf(testfilename, PATH_MAX, "%s", buf); // silence cppcheck
    printf("passed\n");
}

void test_create_file_and_read(sqlfs_t *sqlfs)
{
    printf("Testing creating a file and reading it...");
    char buf[200];
    struct fuse_file_info fi = { 0 };
    fi.flags |= O_CREAT | O_TRUNC | O_RDWR;
    assert(sqlfs_proc_open(sqlfs, "/file", &fi) >= 0);
    assert(sqlfs_proc_access(sqlfs, "/file", R_OK) == 0);
    assert(sqlfs_proc_read(sqlfs, "/file", buf, sizeof(buf), 0, &fi) == 0);
    printf("passed\n");
}

void test_truncate(sqlfs_t *sqlfs, int testsize)
{
    printf("Testing truncating...");
    struct stat sb;
    char testfilename[PATH_MAX];
    randomfilename(testfilename, PATH_MAX, "truncate");
    create_test_file(sqlfs, testfilename, testsize);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == testsize);
    sqlfs_proc_truncate(sqlfs, testfilename, 0);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == 0);
    printf("passed\n");
}

void test_truncate_existing_file(sqlfs_t *sqlfs, int testsize)
{
    printf("Testing opening existing file truncation...");
    char testfilename[PATH_MAX];
    struct stat sb;
    struct fuse_file_info ffi;
    randomfilename(testfilename, PATH_MAX, "truncate");
    create_test_file(sqlfs, testfilename, testsize);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == testsize);
    ffi.flags = O_WRONLY | O_CREAT | O_TRUNC;
    ffi.direct_io = 0;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == 0);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == 0);
    printf("passed\n");
}

void test_getattr_create_truncate_truncate_truncate(sqlfs_t *sqlfs)
{
    printf("Testing getattr create truncate truncate truncate...");
    int rc;
    struct stat sb;
    struct fuse_file_info fi = { 0 };
    char basefile[PATH_MAX];
    char goodfile[PATH_MAX];
    char logfile[PATH_MAX];
    randomfilename(basefile, PATH_MAX, "testfile-single");
    randomfilename(goodfile, PATH_MAX, "testfile-single.fsxgood");
    randomfilename(logfile, PATH_MAX, "testfile-single.fsxlog");

    rc = sqlfs_proc_getattr(sqlfs, basefile, &sb);
    assert(rc == -ENOENT);
    sqlfs_proc_create(sqlfs, basefile, 0100644, &fi);
    sqlfs_proc_getattr(sqlfs, basefile, &sb);
    assert(sb.st_size == 0);

    rc = sqlfs_proc_getattr(sqlfs, goodfile, &sb);
    assert(rc == -ENOENT);
    sqlfs_proc_create(sqlfs, goodfile, 0100644, &fi);
    sqlfs_proc_getattr(sqlfs, goodfile, &sb);
    assert(sb.st_size == 0);

    sqlfs_proc_getattr(sqlfs, logfile, &sb);
    assert(rc == -ENOENT);
    sqlfs_proc_create(sqlfs, logfile, 0100644, &fi);
    sqlfs_proc_getattr(sqlfs, logfile, &sb);
    assert(sb.st_size == 0);

    sqlfs_proc_truncate(sqlfs, basefile, 0);
    sqlfs_proc_getattr(sqlfs, basefile, &sb);
    assert(sb.st_size == 0);

    sqlfs_proc_truncate(sqlfs, basefile, 100000);
    sqlfs_proc_getattr(sqlfs, basefile, &sb);
    assert(sb.st_size == 100000);

    sqlfs_proc_truncate(sqlfs, basefile, 0);
    sqlfs_proc_getattr(sqlfs, basefile, &sb);
    assert(sb.st_size == 0);
    printf("passed\n");

    rc++; // silence ccpcheck
}

void test_write_seek_write(sqlfs_t *sqlfs)
{
    printf("Testing write/seek/write...");
    char* testfilename = "/skipwrite";
    char buf[1000001];
    int skip_offset;
    const char *skip1 = "it was the best of times";
    const int sz_skip1 = strlen(skip1);
    const char *skip2 = "it was the worst of times";
    const int sz_skip2 = strlen(skip2);
    struct stat sb;
    struct fuse_file_info fi = { 0 };
    fi.flags |= O_RDWR | O_CREAT;
    for(skip_offset = 100; skip_offset < 1000001; skip_offset *= 100)
    {
        assert(sqlfs_proc_write(sqlfs, testfilename, skip1, sz_skip1, 0, &fi));
        assert(sqlfs_proc_write(sqlfs, testfilename, skip2, sz_skip2, skip_offset, &fi));
        sqlfs_proc_getattr(sqlfs, testfilename, &sb);
        assert(sb.st_size == (skip_offset+sz_skip2));
        assert(sqlfs_proc_read(sqlfs, testfilename, buf, sz_skip1, 0, &fi) == sz_skip1);
        buf[sz_skip1] = 0;
        assert(!strcmp(buf, skip1));
        assert(sqlfs_proc_read(sqlfs, testfilename, buf, sz_skip2, skip_offset, &fi) == sz_skip2);
        buf[sz_skip2] = 0;
        assert(!strcmp(buf, skip2));
    }
    printf("passed\n");
}

static void wbb_helper(sqlfs_t *sqlfs, int testsize)
{
    char testfilename[PATH_MAX];
    randomfilename(testfilename, PATH_MAX, "skip_write_boundaries");
    char buf[testsize+1];
    struct stat sb;
    struct fuse_file_info fi = { 0 };
    fi.flags |= O_RDWR | O_CREAT;

    char *data = calloc(testsize, sizeof(char));
    int i;
    for (i=0; i<testsize; ++i)
        data[i] = (i % 90) + 32;

    assert(sqlfs_proc_write(sqlfs, testfilename, data, testsize, 0, &fi));
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == testsize);

    assert(sqlfs_proc_read(sqlfs, testfilename, buf, testsize, 0, &fi) == testsize);
    buf[testsize] = 0;
    assert(!strcmp(buf, data));
    free(data);
}

void test_write_block_boundaries(sqlfs_t *sqlfs)
{
    printf("Testing write block boundaries...");
    int i;
    for(i=1;i<5;++i)
    {
        wbb_helper(sqlfs, i*BLOCK_SIZE);
        wbb_helper(sqlfs, i*BLOCK_SIZE-1);
        wbb_helper(sqlfs, i*BLOCK_SIZE+1);
    }
    printf("passed\n");
}

void test_o_append_existing_file(sqlfs_t *sqlfs)
{
    printf("Testing opening existing file O_APPEND and writing...");
    int i, testsize=200;
    char buf[testsize];
    char buf2[testsize];
    char testfilename[PATH_MAX];
    struct stat sb;
    struct fuse_file_info ffi;
    struct fuse_file_info fi = { 0 };
    fi.flags |= O_RDONLY;
    memset(buf, 0, testsize);
    memset(buf2, 0, testsize);
    randomfilename(testfilename, PATH_MAX, "append_existing_file");
    create_test_file(sqlfs, testfilename, testsize);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == testsize);
    i = sqlfs_proc_read(sqlfs, testfilename, buf, testsize, 0, &fi);
    assert(i == testsize);
    ffi.flags = O_WRONLY | O_APPEND;
    ffi.direct_io = 0;
    int rc = sqlfs_proc_open(sqlfs, testfilename, &ffi);
    assert(rc == 0);
    sqlfs_proc_write(sqlfs, testfilename, buf, testsize, 0, &ffi);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == testsize*2);
    assert(sqlfs_proc_read(sqlfs, testfilename, buf2, testsize, 0, &fi) > 0);
    assert(!strcmp(buf, buf2));
    assert(sqlfs_proc_read(sqlfs, testfilename, buf2, testsize, testsize, &fi) > 0);
    assert(!strcmp(buf, buf2));
    printf("passed\n");
    i++; rc++; // silence cppcheck
}

void test_open_non_existent(sqlfs_t *sqlfs)
{
    printf("Testing open non-existent file without O_CREAT...");
    char testfilename[PATH_MAX];
    struct stat sb;
    struct fuse_file_info ffi;
    randomfilename(testfilename, PATH_MAX, "open_non_existent");
    ffi.direct_io = 0;
    ffi.flags = O_RDONLY;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == -ENOENT);
    assert(sqlfs_proc_getattr(sqlfs, testfilename, &sb) == -ENOENT);
    ffi.flags = O_WRONLY;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == -ENOENT);
    assert(sqlfs_proc_getattr(sqlfs, testfilename, &sb) == -ENOENT);
    ffi.flags = O_RDWR;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == -ENOENT);
    assert(sqlfs_proc_getattr(sqlfs, testfilename, &sb) == -ENOENT);
    ffi.flags = O_WRONLY | O_TRUNC;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == -ENOENT);
    assert(sqlfs_proc_getattr(sqlfs, testfilename, &sb) == -ENOENT);
    ffi.flags = O_RDWR | O_TRUNC;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == -ENOENT);
    assert(sqlfs_proc_getattr(sqlfs, testfilename, &sb) == -ENOENT);
    printf("passed\n");
}

void test_open_creat(sqlfs_t *sqlfs)
{
    printf("Testing creating file with open (O_RDWR|O_CREAT)...");
    char testfilename[PATH_MAX];
    struct stat sb;
    struct fuse_file_info ffi;
    randomfilename(testfilename, PATH_MAX, "open_creat");
    ffi.flags = O_RDWR | O_CREAT;
    ffi.direct_io = 0;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == 0);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == 0);
    printf("passed\n");
}

void test_open_creat_trunc(sqlfs_t *sqlfs)
{
    printf("Testing creating file with open(O_WRONLY|O_CREAT|O_TRUNC)...");
    char testfilename[PATH_MAX];
    struct stat sb;
    struct fuse_file_info ffi;
    randomfilename(testfilename, PATH_MAX, "open_creat_trunc");
    ffi.flags = O_WRONLY | O_CREAT | O_TRUNC;
    ffi.direct_io = 0;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == 0);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == 0);
    printf("passed\n");
}

void test_open_creat_trunc_existing(sqlfs_t *sqlfs)
{
    printf("Testing opening file with open(O_WRONLY|O_CREAT|O_TRUNC)...");
    int testsize=123;
    char testfilename[PATH_MAX];
    struct stat sb;
    struct fuse_file_info ffi;
    randomfilename(testfilename, PATH_MAX, "open");
    create_test_file(sqlfs, testfilename, testsize);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == testsize);
    ffi.flags = O_WRONLY | O_CREAT | O_TRUNC;
    ffi.direct_io = 0;
    assert(sqlfs_proc_open(sqlfs, testfilename, &ffi) == 0);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == 0);
    printf("passed\n");
}

void run_standard_tests(sqlfs_t* sqlfs)
{
    int size;

    printf("Running standard tests:\n");
    test_getattr_create_truncate_truncate_truncate(sqlfs);
    test_mkdir_with_sleep(sqlfs);
    test_mkdir_without_sleep(sqlfs);
    test_mkdir_to_make_nested_dirs(sqlfs);
    test_rmdir(sqlfs);
    test_create_file_with_small_string(sqlfs);
    test_create_file_and_read(sqlfs);
    test_write_seek_write(sqlfs);
    test_write_block_boundaries(sqlfs);
    test_read_bigger_than_buffer(sqlfs);
    test_o_append_existing_file(sqlfs);
    test_open_non_existent(sqlfs);
    test_open_creat(sqlfs);
    test_open_creat_trunc(sqlfs);
    test_open_creat_trunc_existing(sqlfs);

    for (size=10; size < 1000001; size *= 10) {
        test_write_n_bytes(sqlfs, size);
        test_read_byte_with_offset(sqlfs, size);
        test_truncate(sqlfs, size);
        test_truncate_existing_file(sqlfs, size);
    }
}


/* performance measures ----------------------------------------------------- */

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

#define START_BLOCK_SIZE 256
#define END_BLOCK_SIZE 32768

void run_perf_tests(sqlfs_t *sqlfs, int testsize)
{
    int i, size;
    struct timeval tstart, tstop;
    char testfilename[PATH_MAX];
    char randomdata[testsize];
    struct fuse_file_info fi = { 0 };

    printf("Running performance tests:\n");

    randomfilename(testfilename, PATH_MAX, "read_n_bytes");
    sqlfs_proc_write(sqlfs, testfilename, randomdata, testsize, 0, &fi);
    printf("reads without transactions ------------------------------\n");
    for (size=START_BLOCK_SIZE; size <= END_BLOCK_SIZE ; size *= 2) {
        gettimeofday(&tstart, NULL);
        for(i = 0; i < testsize / size; i++) {
            test_read_n_bytes_nosleep(sqlfs, testfilename, size);
        }
        gettimeofday(&tstop, NULL);
        printf("* read %d bytes in %d %d byte chunks in \t%f seconds\n",
               testsize, testsize / size, size, TIMING(tstart,tstop));
    }
    printf("reads with transactions ------------------------------\n");
    for (size=START_BLOCK_SIZE; size <= END_BLOCK_SIZE ; size *= 2) {
        gettimeofday(&tstart, NULL);
        sqlfs_begin_transaction(sqlfs);
        for(i = 0; i < testsize / size; i++) {
            test_read_n_bytes_nosleep(sqlfs, testfilename, size);
        }
        sqlfs_complete_transaction(sqlfs,1);
        gettimeofday(&tstop, NULL);
        printf("* read %d bytes in %d %d byte chunks in \t%f seconds\n",
               testsize, testsize / size, size, TIMING(tstart,tstop));
    }
    printf("writes without transactions ------------------------------\n");
    for (size=START_BLOCK_SIZE; size <= END_BLOCK_SIZE ; size *= 2) {
        gettimeofday(&tstart, NULL);
        for(i = 0; i < testsize / size; i++) {
          test_write_n_bytes_nosleep(sqlfs, size);
        }
        gettimeofday(&tstop, NULL);
        printf("* wrote %d bytes in %d %d byte chunks in \t%f seconds\n",
          testsize, testsize / size, size, TIMING(tstart,tstop));
    }
    printf("writes with transactions ------------------------------\n");
    for (size=START_BLOCK_SIZE; size <= END_BLOCK_SIZE ; size *= 2) {
        gettimeofday(&tstart, NULL);
        sqlfs_begin_transaction(sqlfs);
        for(i = 0; i < testsize / size; i++) {
          test_write_n_bytes_nosleep(sqlfs, size);
        }
        sqlfs_complete_transaction(sqlfs,1);
        gettimeofday(&tstop, NULL);
        printf("* wrote %d bytes in %d %d byte chunks in \t%f seconds\n",
          testsize, testsize / size, size, TIMING(tstart,tstop));
    }
}


/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
