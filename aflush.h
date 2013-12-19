/*
 * client
 * int aflush(int fd, int fsync, off_t pos, off_t len)
 * int aclose(int fd)
 * asyncronously flush/close file if able to contact server
 * mck - 12/18/13
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <errno.h>

static char servportpath[256] = { "^/tmp/.async_io_server" };

static __attribute__ ((used)) int aflush(int fd, int fsync, off_t pos, off_t len)
{
    int sock = -1;
    socklen_t servlen = 0;
    struct sockaddr_un servaddr;
    struct msghdr message;
    struct iovec iov[4];
    struct cmsghdr *control_message = NULL;
    char ctrl_buf[CMSG_SPACE(sizeof(int))];
    char data[200];
    char dmeth[200];
    off_t dpos = pos;
    off_t dlen = len;

    if (fd < 0)
        return -1;

    if ( (fsync != 0) && (fsync != 1) )
        return -1;

    int flags = fcntl(fd, F_GETFL);
    if (flags < 0)
        return -1;

    sock = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (sock < 0) {
        if (fsync)
            sync_file_range(fd, pos, len, SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
        posix_fadvise(fd, pos, len, POSIX_FADV_DONTNEED);
        return 0;
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sun_family = AF_UNIX;
    strcpy(servaddr.sun_path, servportpath);
    servlen = strlen(servaddr.sun_path) + sizeof(servaddr.sun_family);
    servaddr.sun_path[0] = 0;

    memset(&message, 0, sizeof(struct msghdr));
    memset(ctrl_buf, 0, CMSG_SPACE(sizeof(int)));
    
    data[0] = 'f';
    dmeth[0] = (char)fsync;
    iov[0].iov_base = data;
    iov[0].iov_len = 1;
    iov[1].iov_base = dmeth;
    iov[1].iov_len = 1;
    iov[2].iov_base = &dpos;
    iov[2].iov_len = sizeof(dpos);
    iov[3].iov_base = &dlen;
    iov[3].iov_len = sizeof(dlen);
    
    message.msg_name = &servaddr;
    message.msg_namelen = servlen;
    message.msg_control = ctrl_buf;
    message.msg_controllen = CMSG_SPACE(sizeof(int));
    message.msg_iov = iov;
    message.msg_iovlen = 4;
    
    control_message = CMSG_FIRSTHDR(&message);
    control_message->cmsg_level = SOL_SOCKET;
    control_message->cmsg_type = SCM_RIGHTS;
    control_message->cmsg_len = CMSG_LEN(sizeof(int));

    int *fd_ptr = (int *)CMSG_DATA(control_message);
    *fd_ptr = fd;

    int n = (int)sendmsg(sock, &message, 0);
    if (n != (iov[0].iov_len + iov[1].iov_len + iov[2].iov_len + iov[3].iov_len)) {
        if (fsync)
            sync_file_range(fd, pos, len, SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
        posix_fadvise(fd, pos, len, POSIX_FADV_DONTNEED);
    }

    return 0;
}

static __attribute__ ((used)) int aclose(int fd)
{
    int sock = -1;
    socklen_t servlen = 0;
    struct sockaddr_un servaddr;
    struct msghdr message;
    struct iovec iov[1];
    struct cmsghdr *control_message = NULL;
    char ctrl_buf[CMSG_SPACE(sizeof(int))];
    char data[200];

    if (fd < 0)
        return -1;

    int flags = fcntl(fd, F_GETFL);
    if (flags < 0)
        return close(fd);
    
    sock = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (sock < 0)
        return close(fd);

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sun_family = AF_UNIX;
    strcpy(servaddr.sun_path, servportpath);
    servlen = strlen(servaddr.sun_path) + sizeof(servaddr.sun_family);
    servaddr.sun_path[0] = 0;

    memset(&message, 0, sizeof(struct msghdr));
    memset(ctrl_buf, 0, CMSG_SPACE(sizeof(int)));
    
    data[0] = 'c';
    iov[0].iov_base = data;
    iov[0].iov_len = 1;
    
    message.msg_name = &servaddr;
    message.msg_namelen = servlen;
    message.msg_control = ctrl_buf;
    message.msg_controllen = CMSG_SPACE(sizeof(int));
    message.msg_iov = iov;
    message.msg_iovlen = 1;
    
    control_message = CMSG_FIRSTHDR(&message);
    control_message->cmsg_level = SOL_SOCKET;
    control_message->cmsg_type = SCM_RIGHTS;
    control_message->cmsg_len = CMSG_LEN(sizeof(int));

    int *fd_ptr = (int *)CMSG_DATA(control_message);
    *fd_ptr = fd;

    (void)sendmsg(sock, &message, 0);

    return close(fd);
}

