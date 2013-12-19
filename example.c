/*
 * example
 *
 * extern int aflush(int fd, int fsync, off_t pos, off_t len);
 * extern int aclose(int fd);
 *
 * asyncronously flush/close file if able to contact server
 * mck - 12/18/13
 */
#include "aflush.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

int main(int argc,char *argv[])
{
    char tfile[200] = { "testfile" };

    unlink(tfile);
    int fd = open(tfile, O_CREAT|O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr,"unable to open test file %s, errno = %d\n", tfile, errno);
        return 0;
    }

    char buf[200] = { "Now is the time for all good men ...\n" };
    int len = strlen(buf);
    if (write(fd, buf, len) != len) {
        fprintf(stderr,"unable to write out test file %s correctly\n", tfile);
        unlink(tfile);
        return 0;
    }

    // flush already written out dirty pages in range from page cache ...
    int ret = aflush(fd, 0, 0, 10);
    if (ret != 0)
        fprintf(stderr,"unable to aflush 0 testfile, errno = %d\n", errno);

    // write out dirty pages in range and flush from page cache ...
    ret = aflush(fd, 1, 0, 10);
    if (ret != 0)
        fprintf(stderr,"unable to aflush 1 testfile, errno = %d\n", errno);

    // write out all dirty pages, flush from page cache and close ...
    ret = aclose(fd);
    if (ret != 0)
        fprintf(stderr,"unable to aclose testfile, errno = %d\n", errno);

    fd = open(tfile, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr,"unable to open test file %s, errno = %d\n", tfile, errno);
        return 0;
    }

    char buf2[200] = { "" };
    if (read(fd, buf2, len) != len) {
        fprintf(stderr,"unable to read in test file %s correctly\n", tfile);
        unlink(tfile);
        return 0;
    }

    if (strcmp(buf,buf2))
        fprintf(stderr,"error - file contents do not match\n");

    ret = aclose(fd);
    if (ret != 0)
        fprintf(stderr,"unable to aclose testfile, errno = %d\n", errno);

    unlink(tfile);
    return 0;
}
