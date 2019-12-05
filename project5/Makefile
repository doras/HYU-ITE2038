.SUFFIXES: .c .o .cc

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# Include more files if you write another source file.
# SRCS_FOR_LIB:=$(SRCDIR)bpt.c $(SRCDIR)disk_based_bpt.c $(SRCDIR)file_manager.c
C_SRCS_FOR_LIB:=$(SRCDIR)file_manager.c
CPP_SRCS_FOR_LIB:=$(SRCDIR)disk_based_bpt.cc $(SRCDIR)lock_manager.cc $(SRCDIR)buffer_manager.cc
C_OBJS_FOR_LIB:=$(C_SRCS_FOR_LIB:.c=.o)
CPP_OBJS_FOR_LIB:=$(CPP_SRCS_FOR_LIB:.cc=.o)

CPPFLAGS+= -g -fPIC -I $(INC)
CXXFLAGS += -std=c++17

all:
	$(foreach SRC,$(C_SRCS_FOR_LIB),gcc $(CPPFLAGS) -o $(SRC:.c=.o) -c $(SRC);)
	$(foreach SRC,$(CPP_SRCS_FOR_LIB),g++ $(CPPFLAGS) $(CXXFLAGS) -o $(SRC:.cc=.o) -c $(SRC);)
	make static_library

clean:
	rm $(C_OBJS_FOR_LIB) $(CPP_OBJS_FOR_LIB) $(LIBS)*

static_library:
	ar cr $(LIBS)libbpt.a $(C_OBJS_FOR_LIB) $(CPP_OBJS_FOR_LIB)
