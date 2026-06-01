#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/replication/logical
#
# IDENTIFICATION
#    src/backend/replication/logical/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/replication/logical
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global


override CPPFLAGS := -I$(srcdir) -I/usr/local/include $(CPPFLAGS)

# 增加 librdkafka 库路径 + 链接库
override LDFLAGS := -L/usr/local/lib $(LDFLAGS)
override LIBS := -lrdkafka $(LIBS)

# override CPPFLAGS := -fsanitize=address $(CPPFLAGS)

# override LDFLAGS := -L/usr/local/lib -fsanitize=address $(LDFLAGS)

override CPPFLAGS := -I$(srcdir) $(CPPFLAGS)




OBJS = \
	applyparallelworker.o \
	conflict.o \
	decode.o \
	launcher.o \
	logical.o \
	logicalfuncs.o \
	message.o \
	origin.o \
	proto.o \
	relation.o \
	reorderbuffer.o \
	slotsync.o \
	snapbuild.o \
	tablesync.o \
	worker.o 
SHLIB_LINK += -L/usr/local/lib -lrdkafka
include $(top_srcdir)/src/backend/common.mk
