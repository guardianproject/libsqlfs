Libsqlfs

Copyright 2011-2013, various developers for Guardian Project
Copyright 2006, Palmsource, Inc., an ACCESS company.

Libsqlfs is free/open source software distributed under the GNU Lesser
General Public License, version 2 or later versions as published by the Free
Software Foundation.  See the file COPYING for the complete licensing terms.


Introduction
============

The libsqlfs library implements a POSIX style file system on top of an
SQLite database.  It allows applications to have access to a full read/write
file system in a single file, complete with its own file hierarchy and name
space.  This is useful for applications which needs structured storage, such
as embedding documents within documents, or management of configuration
data or preferences.  Libsqlfs can be used as an shared library, or it can be
built as a FUSE (Linux File System in User Space) module to allow a libsqlfs
database to be accessed via OS level file system interfaces by normal
applications.
 

Rationale
=========

PalmSource software developers originally created libsqlfs.  This library is
an adjunct to the very popular open source SQLite database software.  Libsqlfs
was created as part of PalmSource's ALP mobile phone platform, but it is
useful in many other applications too.

Guardian Project adopted libsqlfs to use in conjunction with SQLCipher, a
custom version of SQLite3 that includes support for encrypting the database
contents.  This makes it into a self-contained, encrypted filesystem.
IOCipher is a project based on top of libsqlfs that provides a virtual
encrypted file system using the java.io API.

The libsqlfs library provides an easy way for applications to put an
entire read/write file system into a relational database as a single file
in the host file system.  Such a file system can easily be moved around,
backed up or restored as a single file.  But the file system can also be
accessed as individual files.  This provides great flexibility and
convenience.

We concluded that a simpler way to meet our needs was to write a library
that supported  the POSIX file system semantics on an SQL database.  This
brings the benefits of a real database, such as transactions and
concurrency control, and allows us to have complete control over the
schema of the preferences, so we can allow additional metadata such as
value types, permissions and access control lists. Our libsqlfs registry
can accommodate small preference values such as a number, and large
binary objects such as an video clip.   The library provides a generic
file system layer that maps a file system onto a SQLite database, and
supports a POSIX file system semantics.

To speed development, we built our file system mapping layer as a File
System In User Space (FUSE) module.  FUSE is another open source
project.  It is a kernel module that supports user-level implementations
of file systems. Our design allows libsqlfs to implement a real file
system at the OS level, and apply real file system operations on it.  We 
tested the complete build process of gcc and the Linux kernel on top of
libsqlfs, and we successfully executed fsx.c, the Apple file system test
tool, against libsqlfs.

Today the ALP Global Settings component uses libsqlfs as the storage
back-end.  Libsqlfs provides an easy way for applications to support a
read/write file system totally contained in a relational database as a single
file in the host file system, without using SQL statements.  Libsqlfs
provides a superset of the storage features of GConf, and can be used as the
storage back end of other desktop preference services.  Libsqlfs is also
useful wherever developers need to organize data, and sometimes treat it as
one file, and at other times treat it as a collection of individually
writable files.


Installation
============

* As a Library

Libsqlfs provides a GNU autoconf/automake based build system for building as
an application library.  To build, please follow the normal GNU configure
conventions.  Normally, the following command is all what's needed:

./configure --prefix=<install dir>
make && make install

<install dir> defaults to /usr/local if not specified.

You have to be root for installing into system directories such as
/usr/local.

Both a static library and a shared library are built, unless you specify
otherwise via options to configure.

* As an FUSE module

If you want to build it as a FUSE module, you need to have libfuse
installed on your system.  This is less tested than the direct API.

After running the script you shall have an executable called fuse_sqlfs.
Run it as root to start a FUSE session on top of libsqlfs:

fuse_sqlfs <mnt point> 

then you shall see the libsqlfs file space exposed, and
can be accessed by normal applications,  via the <mnt point>.

example:

fuse_sqlfs /mnt/sqlfs &

ls /mnt/sqlfs

The location of the SQLite database is hard-coded in fuse_main.c.  Change the
argument to sqlfs_init() to suit your needs.

The database file that it opens is currently hard-coded in fuse_sqlfs.c as
/tmp/fsdata.  If you want to use a different database file, or provide a
key to an encrypted file, then just edit fuse_sqlfs.c and rebuild.

For a sample application showing the usage of libsqlfs, see the test
programs in the tests/ directory.


Operating Modes
===============

There are two modes of operation for libsqlfs: "init/destroy" and
"open/close".  "init/destroy" requires sqlfs_init() to be called before any
operations, then each thread dynamically allocates a sqlfs_t based on need.
This is the mode used by FUSE.  sqlfs_destroy() must be called after all
operation is over to clean things up.

"open/close" is more like opening up a file.  It is used when the logic of the
program using this requires an "open" or "mounted" state.  This is the mode
that is used by IOCipher.


API
===

Libsqlfs started as an FUSE module so it implements the primitives as defined
by FUSE version 2.5.3.  A libsqlfs session is represented by an object of type
sqlfs_t.  All APIs require an explicit reference to a valid sqlfs_t.
Specifically, the following file system primitives are implemented:


int sqlfs_proc_getattr(sqlfs_t *, const char *path, struct stat *stbuf);
int sqlfs_proc_access(sqlfs_t *, const char *path, int mask);
int sqlfs_proc_readlink(sqlfs_t *, const char *path, char *buf, size_t size);
int sqlfs_proc_readdir(sqlfs_t *, const char *path, void *buf, fuse_fill_dir_t filler, 
                  off_t offset, struct fuse_file_info *fi);
int sqlfs_proc_mknod(sqlfs_t *, const char *path, mode_t mode, dev_t rdev);
int sqlfs_proc_mkdir(sqlfs_t *, const char *path, mode_t mode);
int sqlfs_proc_unlink(sqlfs_t *, const char *path);
int sqlfs_proc_rmdir(sqlfs_t *, const char *path);
int sqlfs_proc_symlink(sqlfs_t *, const char *path, const char *to);
int sqlfs_proc_rename(sqlfs_t *, const char *from, const char *to);
int sqlfs_proc_link(sqlfs_t *, const char *from, const char *to);
int sqlfs_proc_chmod(sqlfs_t *, const char *path, mode_t mode);
int sqlfs_proc_chown(sqlfs_t *, const char *path, uid_t uid, gid_t gid);
int sqlfs_proc_truncate(sqlfs_t *, const char *path, off_t size);
int sqlfs_proc_utime(sqlfs_t *, const char *path, struct utimbuf *buf);
int sqlfs_proc_open(sqlfs_t *, const char *path, struct fuse_file_info *fi);
int sqlfs_proc_read(sqlfs_t *, const char *path, char *buf, size_t size, off_t offset, struct
    fuse_file_info *fi);
int sqlfs_proc_write(sqlfs_t *, const char *path, const char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi);
int sqlfs_proc_statfs(sqlfs_t *, const char *path, struct statvfs *stbuf);
int sqlfs_proc_release(sqlfs_t *, const char *path, struct fuse_file_info *fi);
int sqlfs_proc_fsync(sqlfs_t *, const char *path, int isfdatasync, struct fuse_file_info *fi);


Their semantics are as defined by the FUSE documentation and the
corresponding Unix file system calls.  Following the FUSE conventions, all
file or key paths must be absolute and start with a '/'.  Applications can
provide their own logic for relative paths before passing the "normalized"
absolute paths to these FUSE primitive routines.

In addition, other APIs provide environment setup, support for
transaction and convenience functions: 

int sqlfs_init(const char *)
    initialize the libsqlfs library and sets the default database file name.

int sqlfs_destroy()
    clean up after sqlfs_init() when all operation is over.

int sqlfs_open(const char *db, sqlfs_t **);
    creates a new connection to the libsqlfs database.  The first argument,
    if not NULL, specifies a different database file from the default.

int sqlfs_open_key(const char *db_file, const char *key, sqlfs_t **sqlfs);
    creates a new connection to an encrypted libsqlfs database and unlocks it
    using the password provided.  The first argument, if not NULL, specifies a
    different database file from the default.

int sqlfs_close(sqlfs_t *);
    closes and frees a libsqlfs connection.


Low-level API
=============

You can operate on the filesystem on a level lower than the FUSE API with
these functions:

int sqlfs_del_tree(sqlfs_t *sqlfs, const char *key);
    deletes a whole subtree.

int sqlfs_get_value(sqlfs_t *sqlfs, const char *key, key_value *value, 
    size_t begin, size_t end); 
    reads contents of a file contained in a range
    (between offsets begin and end)

int sqlfs_set_value(sqlfs_t *sqlfs, const char *key, const key_value *value, 
    size_t begin,  size_t end);
    writes contents of value to a file within the specified range
    (between offsets begin and end)

int sqlfs_get_attr(sqlfs_t *sqlfs, const char *key, key_attr *attr);
    reads the metadata of a file
    
int sqlfs_set_attr(sqlfs_t *sqlfs, const char *key, const key_attr *attr);
    write the metadata of a file

int sqlfs_set_type(sqlfs_t *sqlfs, const char *key, const char *type);
    sets the "type" of the file content. 
      
int sqlfs_begin_transaction(sqlfs_t *sqlfs);
    begins a SQLite transaction
    
int sqlfs_complete_transaction(sqlfs_t *sqlfs, int i);
    ends a SQLite transaction


Implementation
==============

The filesystem is implemented using the common pattern of blocks allocated to
a file.  The file system is stored in a SQLite table, with the following
columns:

full key path | type | inode   | uid     | gid     | mode    | acl  | attributes | atime   | mtime   | ctime   | size    | block_size
text          | text | integer | integer | integer | integer | text | text       | integer | integer | integer | integer | integer

The key path must be an absolute path using "/" as the path separators.  The
path is case sensitive.  The type of data associated with the key path can be
one of these: "int", "double", "string", "dir", "sym link" and "blob".
Generally, data is allocated as 8k blobs representing filesystem blocks.
Using "int", "double" and "string" for a file's data should be avoided since
its not generalizable.  Each block occupies an BLOB object in database indexed
by a block number which starts from 0.

The table rows are created using:

 CREATE TABLE meta_data(key text, type text, inode integer, uid integer,
                        gid integer, mode integer, acl text,
                        attribute text, atime integer, mtime integer,
                        ctime integer, size integer, block_size integer,
                        primary key (key), unique(key));
 CREATE TABLE value_data (key text, block_no integer, data_block blob, unique(key, block_no));
 CREATE INDEX meta_index ON meta_data (key);
 CREATE INDEX value_index ON value_data (key, block_no);

SQL transactions are used throughout the code to improve efficiency.  Note the
transaction supports "levels"; that is, transaction calls can be nested and
libsqlfs maintains an internal level count of the current transaction level.
The actual SQLite transaction are only started when the level goes above 0 and
only ended when the level falls to zero.

A libsqlfs session is represented by an object of type sqlfs_t.  All APIs
require an explicit reference to a valid sqlfs_t. Each file is a "key" in the
internal libsqlfs vocabulary.  File metadata are represented as objects of the
sturct key_attr.  File contents are represented by the struct key_value.

File metadata are the normal POSIX file attributes as expected except an
additional "type" which can not be visible via the normal file attribute
functions.  The "type" is used to support the specific needs of the setting
registry application and can be one of the following:

Null
Dir
Integer (32-bit)
Double (a C double)
String (a C zero-terminated string)
Sym_link (symbolic link)
Bool  (a boolean)
List (a Glib list of values)
Blob (a binary object)

Note all other file system primitives do not make use of the "type"; to them
all files are blobs. At this point the "type" is meant for use by higher up
application logic in applications using libsqlfs.

Some things are not currently implemented:

* permission control due to the current directory
* extended attributes

In order to fix locking issues but improve overall performance,
begin_transaction obtains a reserved lock immediately. This reduces contention
for write locks that were occuring with deferred transactions, and performs
much better than exclusive transactions with immediate exclusive locking.

There were originally a few different locking techniques in the code,
some commented out, and really only one in use: the sqlite 'begin
exclusive'.  There was a pthread mutex lock below that is quite large
grained. Then in sqlfs_t_init, there was the sqlite3_busy_timeout(), which
was there to help ensure that the call to create "/" if it doesn't exist
doesn't fail.

Originally, 'begin exclusive' was only used in LIBFUSE mode, and not in
standalone library mode, where 'begin' was used.  But we found it too
unreliable so we switched standalone mode to also use 'begin exclusive'.

 https://www.sqlite.org/lockingv3.html
 https://www.sqlite.org/lang_transaction.html
 https://www.sqlite.org/c3ref/busy_handler.html


Tests
=====

There is an included test suite in the tests/ subfolder.  They are a
combination of C programs and bash scripts.  There are a number of ways to run
the tests.  Here is to run them all:

 make check

If you want to see all of the messages, turn on verbose mode:

 make check V=1

You can also select which tests you want to run:

 make check TESTS=fuse_sqlfs.test


Supported Platforms
===================

To date, libsqlfs is tested on 32-bit i386, 64-bit amd64 and ARM (Android
and Palm Treo 650 phones).  It runs on GNU/Linux (Debian, Ubuntu,
Mint, and Fedora) and Android, and most likely any UNIX.

Currently libsqlfs, when used as a library, has been tested on GNU/Linux
(Debian, Mint, Ubuntu, Red Hat, and Fedora) although it should be usable on any
UNIX like platforms where SQLite runs with at most minor changes.  It should
also work on the Cygwin enviroment but this is not tested.  Patches for
different platform support are welcome.

For use at the OS level, libsqlfs only supports the FUSE on the Linux kernel.
It should be possible to make it work on Mac OS X using fuse4x., and also
FreeBSD or Solaris provided they have a FUSE that is compatible with the Linux
FUSE.


Supported Database
==================

To date, only SQLite and SQLCipher are supported.  SQLCipher is a
version of SQLite that provides page-by-page AES-256 encryption.


Notes on the Code
=================

There is a macro INDEX used in the implementation in sqlfs.c.  It is
re-defined for each function that writes to the database using an
index number for that particular function.  This index number is then
used in the macros PREPARE_STMT and DONE_PREPARE for interacting with
the database.



For more information, please contact:
guardian-dev@lists.mayfirst.org

The original authors are:
Peter van der Linden  peter.vanderlinden@palmsource.com 
Andy Tai, andy.tai@palmsource.com
