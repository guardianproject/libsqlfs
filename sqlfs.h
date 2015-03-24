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
/*!
 * @file sqlfs.h
 *
 * @brief File public header for file system on top of a SQL database library
 *  APIs
 *
 *****************************************************************************/


#ifndef __SQLFS__H__
#define __SQLFS__H__

#ifdef __cplusplus
extern "C" {
#endif

#ifdef HAVE_LIBFUSE
# include "fuse.h"
#else
# include <sys/stat.h>
# include <stdint.h>

    /* the following struct derived from the FUSE header file

        FUSE: Filesystem in Userspace
        Copyright (C) 2001-2006  Miklos Szeredi <miklos@szeredi.hu>

        This program can be distributed under the terms of the GNU LGPL.
    */

    /**
     * Information about open files
     *
     * Changed in version 2.5
     */
    struct fuse_file_info
    {
        /** Open flags.  Available in open() and release() */
        int flags;

        /** Old file handle, don't use */
        unsigned long fh_old;

        /** In case of a write operation indicates if this was caused by a
            writepage */
        int writepage;

        /** Can be filled in by open, to use direct I/O on this file.
            Introduced in version 2.4 */
        unsigned int direct_io : 1;

        /** Can be filled in by open, to indicate, that cached file data
            need not be invalidated.  Introduced in version 2.4 */
        unsigned int keep_cache : 1;

        /** Padding.  Do not use*/
        unsigned int padding : 30;

        /** File handle.  May be filled in by filesystem in open().
            Available in all other file operations */
        uint64_t fh;
    };


    /** Function to add an entry in a readdir() operation
     *
     * @param buf the buffer passed to the readdir() operation
     * @param name the file name of the directory entry
     * @param stat file attributes, can be NULL
     * @param off offset of the next entry or zero
     * @return 1 if buffer is full, zero otherwise
     */
    typedef int (*fuse_fill_dir_t) (void *buf, const char *name,
                                    const struct stat *stbuf, off_t off);

#endif /* HAVE_LIBFUSE */
#include "sqlfs_internal.h"

/* There is a distinction between "init" and "open/close" mode.  FUSE uses
 * "init" mode, where sqlfs instances are created on the fly as needed and
 * stored using pthread_setspecific().  "open/close" mode is really no
 * different, but it provides open and close methods as a way to keep track of
 * whether the filesystem is mounted". */

    int sqlfs_init(const char *);
    int sqlfs_destroy();
    int sqlfs_instance_count(); /* number of active threads */
    int sqlfs_open(const char *db_file, sqlfs_t **psqlfs);
    int sqlfs_close(sqlfs_t *);
    void sqlfs_detach_thread();
    /* since the password gets cooked down to 256 bits, 512 chars is plenty */
#   define MAX_PASSWORD_LENGTH 512
#ifdef HAVE_LIBSQLCIPHER
    /* the raw key format is 32 bytes/256 bits of raw key data */
#   define REQUIRED_KEY_LENGTH 32
    /* The SQLCipher key can be a password or a raw AES key. Refer to
     * SQLCipher's documentation on the keying process.
     * http://sqlcipher.net/sqlcipher-api/#key  */

    /* This is the password format, it needs a UTF-8 string */
    int sqlfs_init_password(const char *db_file, const char *password);
    int sqlfs_open_password(const char *db_file, const char *password, sqlfs_t **sqlfs);
    int sqlfs_change_password(const char *db_file_name, const char *old_password, const char *new_password);
    /* This is the raw key format, it needs 32 bytes of raw key data */
    int sqlfs_init_key(const char *db_file, const uint8_t *key, size_t keylen);
    int sqlfs_open_key(const char *db_file, const uint8_t *key, size_t keylen, sqlfs_t **psqlfs);
    int sqlfs_rekey(const char *db_file_name, const uint8_t *old_key, size_t old_key_len,
                    const void *new_key, size_t new_key_len);
#endif
#ifdef HAVE_LIBFUSE
    int sqlfs_fuse_main(int argc, char **argv);
#endif



#ifdef __cplusplus
}       // extern "C"
#endif

#endif



/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
