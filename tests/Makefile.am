AM_CFLAGS = -I$(top_srcdir) @DEFS@
AM_LDFLAGS = $(top_builddir)/libsqlfs-1.0.la -lpthread @SQLITE@ @LIBFUSE@

EXTRA_DIST = defs

TESTS_ENVIRONMENT = top_builddir=$(top_builddir)
testsubdir = .tests

TESTS = \
	test \
	fuse_sqlfs.test

# scripted tests
check_SCRIPTS = \
	fuse_sqlfs.test

# compiled tests
check_PROGRAMS = \
	test \
	fsx

test_SOURCES = test.c
fsx_SOURCES = fsx.c

if WITH_SQLCIPHER
TESTS += test_key
check_PROGRAMS += test_key
test_key_SOURCES = test_key.c
endif

distclean-local:
	-rm -rf $(testsubdir)

