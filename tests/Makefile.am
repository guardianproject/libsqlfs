include $(top_srcdir)/makerules

noinst_PROGRAMS = sqlfs_test

sqlfs_test_SOURCES = test.c
sqlfs_test_LDADD = $(top_builddir)/libsqlfs.la $(DEPS_LIBS) -lpthread
