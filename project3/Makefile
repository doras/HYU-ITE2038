.SUFFIXES: .c .o

CC=gcc

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# Include more files if you write another source file.
# SRCS_FOR_LIB:=$(SRCDIR)bpt.c $(SRCDIR)disk_based_bpt.c $(SRCDIR)file_manager.c
SRCS_FOR_LIB:=$(SRCDIR)disk_based_bpt.c $(SRCDIR)file_manager.c $(SRCDIR)buffer_manager.c
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.c=.o)

CFLAGS+= -g -fPIC -I $(INC)

all:
	$(foreach SRC,$(SRCS_FOR_LIB),$(CC) $(CFLAGS) -o $(SRC:.c=.o) -c $(SRC);)
	make static_library

clean:
	rm $(OBJS_FOR_LIB) $(LIBS)*

library:
	gcc -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
