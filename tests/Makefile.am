AM_CFLAGS = -I$(top_srcdir) @DEFS@
AM_LDFLAGS = $(top_builddir)/libsqlfs-1.0.la -lpthread @SQLITE@ @LIBFUSE@
noinst_PROGRAMS = test

test_SOURCES = test.c

if WITH_SQLCIPHER
noinst_PROGRAMS += test_key
test_key_SOURCES = test_key.c
endif
