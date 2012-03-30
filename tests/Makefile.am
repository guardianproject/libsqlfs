AM_CFLAGS = -I$(top_srcdir) @DEFS@
AM_LDFLAGS = $(top_builddir)/libsqlfs-1.0.la -lpthread @SQLITE@ @LIBFUSE@
noinst_PROGRAMS = sqlfs_test

sqlfs_test_SOURCES = test.c

if WITH_SQLCIPHER
noinst_PROGRAMS += sqlfs_test_key
sqlfs_test_key_SOURCES = test_key.c
endif
