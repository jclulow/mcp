

CFLAGS =	-Wall -Wextra -Werror -gdwarf-2 \
		-Wno-unused-parameter \
		-Wno-unused-function \
		-std=c99 \
		-D__EXTENSIONS__ -D_REENTRANT

#CFLAGS +=	-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS +=	-m64

OBJ =		mcp.o \
		copyfile.o \
		strset.o

HEADERS =	copyfile.h

PROG =		mcp

.PHONY: all
all: $(PROG)

%.o: %.c $(HEADERS)
	gcc $(CFLAGS) -c -o $@ $<

$(PROG): $(OBJ)
	gcc $(CFLAGS) -o $@ $^ -lavl

.PHONY: clean
clean:
	rm -f *.o
	rm -f $(PROG)

