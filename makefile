
CC = gcc
CFLAGS = -O2 -Wall -g -std=c99
LIBS =

all: server libaflush.a example

server: server.c aflush.h
	$(CC) $(CFLAGS) $< -o $@ $(LIBS) -lpthread

example: example.c aflush.h
	$(CC) $(CFLAGS) $< -o $@ $(LIBS)

aflush.o: aflush.h
	sed 's/static __attribute__ /extern __attribute__ /' aflush.h > aflush.c; \
	$(CC) $(CFLAGS) aflush.c -c; \
    $(RM) -f aflush.c
    
libaflush.a: aflush.o
	$(AR) rcs $@ aflush.o

clean:
	$(RM) -f server example aflush.c aflush.o libaflush.a

