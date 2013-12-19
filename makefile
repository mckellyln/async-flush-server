
CC = gcc
CFLAGS = -O2 -Wall -g -std=c99
LIBS =

all: server example

server: server.c aflush.h
	$(CC) $(CFLAGS) $< -o $@ $(LIBS) -lpthread

example: example.c aflush.h
	$(CC) $(CFLAGS) $< -o $@ $(LIBS)

clean:
	$(RM) server example

