include $(top_srcdir)/makerules

noinst_PROGRAMS = sqlfs_test sqlfs_test_key

sqlfs_test_SOURCES = test.c
sqlfs_test_LDADD = $(top_builddir)/libsqlfs-1.0.la $(DEPS_LIBS) -lpthread

sqlfs_test_key_SOURCES = test_key.c
sqlfs_test_key_LDADD = $(top_builddir)/libsqlfs-1.0.la $(DEPS_LIBS) -lpthread
