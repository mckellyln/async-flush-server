/*
 * server
 * asyncronously flush/close file if able to contact server
 * mck - 12/18/13
 */
#include "aflush.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <signal.h>
#include <pthread.h>

typedef struct{
    int type;
    int used;
    int fd;
    int meth;
    off_t pos;
    off_t len;
}POOL;
#define MAX_POOLS 128
static POOL pool[MAX_POOLS];
static int num_pools = 32;

static int lifespan = 30000;
static int interval = 100;
static int blk_size = 67108864;

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

static int debug = 0;

static int read_fd(int sock, int *recvfd, int *meth, off_t *pos, off_t *len)
{
    struct msghdr message;
    struct iovec iov[4];
    struct cmsghdr *control_message = NULL;
    char ctrl_buf[CMSG_SPACE(sizeof(int))];
    char data[200];
    char dmeth[200];
    off_t dpos = -1;
    off_t dlen = -1;

    memset(&message, 0, sizeof(struct msghdr));
    memset(ctrl_buf, 0, CMSG_SPACE(sizeof(int)));
    
    iov[0].iov_base = data;
    iov[0].iov_len = 1;
    iov[1].iov_base = dmeth;
    iov[1].iov_len = 1;
    iov[2].iov_base = &dpos;
    iov[2].iov_len = sizeof(dpos);
    iov[3].iov_base = &dlen;
    iov[3].iov_len = sizeof(dlen);
    
    message.msg_name = NULL;
    message.msg_namelen = 0;
    message.msg_control = ctrl_buf;
    message.msg_controllen = CMSG_SPACE(sizeof(int));
    message.msg_iov = iov;
    message.msg_iovlen = 4;

    data[0] = 0;
    dmeth[0] = 0;
    int n = -1;
    if ( (n = (int)recvmsg(sock, &message, MSG_CMSG_CLOEXEC)) <= 0) {
        fprintf(stderr,"n = %d errno = %d\n", n, errno);
        return -1;
    }

    if((message.msg_flags & MSG_CTRUNC) == MSG_CTRUNC) {
        fprintf(stderr,"bad data len (MSG_CTRUNC)\n");
        return -1;
    }

    data[1] = '\0';
    dmeth[1] = '\0';

#if 0
    printf("n = %d data = <%s>\n", n, data);
    printf("dpos = %ld\n", dpos);
    printf("dlen = %ld\n", dlen);
#endif
 
    if ( (data[0] != 'c') && (data[0] != 'f') ){
        if (debug)
            fprintf(stderr,"bad data type (%c)\n", (int)data[0]);
        return -1;
    }

    if (data[0] == 'c') {
        if (n != iov[0].iov_len) {
            if (debug)
                fprintf(stderr,"bad data len (c) n = %d\n", n);
            return -1;
        }
    } else {
        if (n != (iov[0].iov_len + iov[1].iov_len + iov[2].iov_len + iov[3].iov_len)) {
            if (debug)
                fprintf(stderr,"bad data len (f) n = %d\n", n);
            return -1;
        }
    }

    int newfd = -1;
    for(control_message = CMSG_FIRSTHDR(&message); control_message != NULL; control_message = CMSG_NXTHDR(&message, control_message)) {
        if( (control_message->cmsg_level == SOL_SOCKET) && (control_message->cmsg_type == SCM_RIGHTS) ) {
            int *fd_ptr = (int *)CMSG_DATA(control_message);
            newfd = *fd_ptr;
            break;
        }
    }

    if (newfd < 0)
        return -1;

    *recvfd = newfd;
    *meth = (int)dmeth[0];
    *pos = dpos;
    *len = dlen;

    return n;
}

static void *handle_fd(void *lindx)
{
    int indx = (int)(long)lindx;
    if ( (indx < 0) || (indx >= num_pools) )
        return NULL;

    pthread_mutex_lock(&lock);
    int type = pool[indx].type;
    int newfd = pool[indx].fd;
    int meth = pool[indx].meth;
    off_t pos = pool[indx].pos;
    off_t len = pool[indx].len;
    pthread_mutex_unlock(&lock);

#if 0
    fprintf(stderr,"newfd = %d\n", newfd);
    fprintf(stderr,"type  = %d\n", type);
    fprintf(stderr,"meth  = %d\n", meth);
#endif

    if (newfd < 0)
        goto end;

    int flags = fcntl(newfd, F_GETFL);
    if (flags < 0) {
        if (errno != EBADF)
            close(newfd);
        goto end;
    }

    int ret = -1;

    // flush

    if (type == 2) {
        if (meth == 1) {
            ret = sync_file_range(newfd, pos, len, SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
            if (debug)
                fprintf(stderr,"flush: %d sync_file_range(%d,%ld,%ld) returns %d\n", indx, newfd, pos, len, ret);
        }
        ret = posix_fadvise(newfd, pos, len, POSIX_FADV_DONTNEED);

        if (debug)
            fprintf(stderr,"flush: %d posix_fadvise(%d,%ld,%ld) returns %d\n", indx, newfd, pos, len, ret);

        close(newfd);
        goto end;
    }

    // close

#if 1
    int lifesecs = lifespan / 1000;

    if (debug > 1)
        fprintf(stderr,"close: %d fd %d waiting lifespan %d secs\n", indx, newfd, lifesecs);

    time_t time_start = time(NULL);

    sync_file_range(newfd, 0, 0, SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);

    time_t time_sync = time(NULL) - time_start;

    if (time_sync < lifesecs) {
        struct timespec ts;
        ts.tv_sec = lifesecs - time_sync;
        ts.tv_nsec = 0;
        nanosleep(&ts, NULL);
    }

    posix_fadvise(newfd, 0, 0, POSIX_FADV_DONTNEED);

    close(newfd);
    goto end;
#else
    struct stat sbuf;
    ret = fstat(newfd, &sbuf);
    if (ret < 0) {
        if (errno != EBADF)
            close(newfd);
        goto end;
    }
    off_t fsz = sbuf.st_size;

    struct timespec ts;
    ts.tv_sec = lifespan / 1000;
    ts.tv_nsec = (long)((double)lifespan - ((double)ts.tv_sec * 1000.0)) * 1000000L;
    if (debug > 1)
        fprintf(stderr,"close: %d fd %d waiting lifespan %ld:%ld secs\n", indx, newfd, ts.tv_sec, ts.tv_nsec);
    nanosleep(&ts, NULL);

    // throttled write-out and page cache drop

    pos = 0;
    len = blk_size;
    while (pos < fsz) {
        if ((pos + len) > fsz)
            len = fsz - pos;
        if ((flags & O_ACCMODE) != O_RDONLY) {
            ret = sync_file_range(newfd, pos, len, SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
            if (debug)
                fprintf(stderr,"close: %d sync_file_range(%d,%ld,%ld) returns %d\n", indx, newfd, pos, len, ret);
        }
        ret = posix_fadvise(newfd, pos, len, POSIX_FADV_DONTNEED);
        if (debug)
            fprintf(stderr,"close: %d posix_fadvise(%d,%ld,%ld) returns %d\n", indx, newfd, pos, len, ret);
        pos += len;
        ts.tv_sec = interval / 1000;
        ts.tv_nsec = (long)((double)interval - ((double)ts.tv_sec * 1000.0)) * 1000000L;
        if (debug > 1)
            fprintf(stderr,"close: %d waiting interval %ld:%ld secs\n", indx, ts.tv_sec, ts.tv_nsec);
        nanosleep(&ts, NULL);
    }

    close(newfd);
    goto end;
#endif

end:
    if (debug)
        fprintf(stderr,"close: %d final closing fd %d\n", indx, newfd);
    pthread_mutex_lock(&lock);
    pool[indx].used = 0;
    pthread_mutex_unlock(&lock);
    return NULL;
}

static void sig_hdlr(int sig)
{
    exit(0);
    return;
}

int main(int argc,char *argv[])
{
    int sock = -1;
    socklen_t servlen = 0;
    struct sockaddr_un servaddr;
    struct sigaction sact;
    struct sigaction sign;
    int background = 0;

    if (argc > 1) {
        int i = 1;
        while(i < argc) {
            if ( (strncmp(argv[i], "-h", 2) == 0) || (strncmp(argv[i], "--h", 3) == 0) ) {
                fprintf(stderr,"\nusage: %s [-help] [-debug] [-bg (daemon)] [-life <> (30000 usec)] [-int <> (3000 usec)] [-tmax <> (32)] [-m <> (blksiz MB (64))]\n\n", argv[0]);
                return 0;
            }
            if ( (strncmp(argv[i], "-b", 2) == 0) || (strncmp(argv[i], "--b", 3) == 0) ) {
                background = 1;
            } else if ( (strncmp(argv[i], "-d", 2) == 0) || (strncmp(argv[i], "--d", 3) == 0) ) {
                debug++;
            } else if ( (strncmp(argv[i], "-l", 2) == 0) || (strncmp(argv[i], "--l", 3) == 0) ) {
                if (++i < argc)
                    lifespan = atoi(argv[i]);
            } else if ( (strncmp(argv[i], "-i", 2) == 0) || (strncmp(argv[i], "--i", 3) == 0) ) {
                if (++i < argc)
                    interval = atoi(argv[i]);
            } else if ( (strncmp(argv[i], "-t", 2) == 0) || (strncmp(argv[i], "--t", 3) == 0) ) {
                if (++i < argc)
                    num_pools = atoi(argv[i]);
            } else if ( (strncmp(argv[i], "-m", 2) == 0) || (strncmp(argv[i], "--m", 3) == 0) ) {
                if (++i < argc)
                    blk_size = atoi(argv[i]) * 1024 * 1024;
            }
            ++i;
        }
    }

    if (num_pools <= 0)
        num_pools = 1;

    if (num_pools > MAX_POOLS)
        num_pools = MAX_POOLS;

    if (lifespan < 0)
        lifespan = 0;

    if (interval < 0)
        interval = 0;

    if (blk_size < 0)
        blk_size = 4194304;

    if (debug) {
        fprintf(stderr,"debug       = %d\n", debug);
        fprintf(stderr,"max threads = %d\n", num_pools);
        fprintf(stderr,"lifespan    = %d\n", lifespan);
        fprintf(stderr,"interval    = %d\n", interval);
        fprintf(stderr,"blocksize   = %d\n", blk_size);
    }

    sigfillset(&sact.sa_mask);
    sact.sa_handler = (void (*)(int))sig_hdlr;
    sact.sa_flags = 0;
    sigaction(SIGINT,&sact,NULL);
    sigaction(SIGQUIT,&sact,NULL);
    sigaction(SIGTERM,&sact,NULL);

    sigfillset(&sign.sa_mask);
    sign.sa_handler = SIG_IGN;
    sign.sa_flags = 0;
    sigaction(SIGHUP,&sign,NULL);

    sock = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (sock < 0) {
        fprintf(stderr,"socket() failure, errno = %d\n", errno);
        return 0;
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sun_family = AF_UNIX;
    strcpy(servaddr.sun_path, servportpath);
    servlen = strlen(servaddr.sun_path) + sizeof(servaddr.sun_family);
    servaddr.sun_path[0] = 0;

    if(bind(sock, (struct sockaddr *)&servaddr, servlen) < 0) {
        if (errno == EADDRINUSE)
            fprintf(stderr,"address in use - is another server running ?\n");
        else
            fprintf(stderr,"bind() failure, errno = %d\n", errno);
        close(sock);
        return 0;
    }

    if (background) {
        debug = 0;
        if (daemon(0, 0) < 0) {
            fprintf(stderr,"unable to become daemon, errno = %d\n", errno);
            close(sock);
            return 0;
        }
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, 65536);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    for (int i=0;i<num_pools;i++)
        pool[i].used = 0;

    while(1) {
        int newfd = -1;
        int meth = -1;
        off_t pos = -1;
        off_t len = -1;
        int ret = read_fd(sock, &newfd, &meth, &pos, &len);
        if ( (ret > 0) && (newfd >= 0) ){
            pthread_mutex_lock(&lock);
            int indx = -1;
            for (int i=0;i<num_pools;i++) {
                if (pool[i].used == 0) {
                    pool[i].used = 1;
                    indx = i;
                    break;
                }
            }
            pthread_mutex_unlock(&lock);
            if (indx >= 0) {
                pool[indx].fd = newfd;
                pool[indx].meth = meth;
                if (ret == 1)
                    pool[indx].type = 1;
                else
                    pool[indx].type = 2;
                pool[indx].pos = pos;
                pool[indx].len = len;
                pthread_t tid;
                int ret = pthread_create(&tid, &attr, (void *(*)(void *))handle_fd, (void *)(long)indx);
                if (ret) {
                        pthread_mutex_lock(&lock);
                        pool[indx].used = 0;
                        pthread_mutex_unlock(&lock);
                }
            } else if (newfd >= 0) {
                close(newfd);
                if (debug)
                    fprintf(stderr,"note: thread pool exhausted ...\n");
            }
        }
    }

    pthread_attr_destroy(&attr);
    close(sock);

    return 0;
}
