/* Linux io_uring based ae.c module
 *
 * This multiplexer uses io_uring for polling, and provides hooks for proactor
 * asynchronous completion routing.
 */

#include <liburing.h>
#include <string.h>

#define AE_IOURING_OP_POLL 0
#define AE_IOURING_OP_PROACTOR 1

/* A tagged request to identify CQEs */
typedef struct aeUringReq {
    int op_type;
    int fd;
    int mask;
    void *client_data;
    void (*completion_handler)(void *client_data, int res);
} aeUringReq;

typedef struct aeApiState {
    struct io_uring ring;
    int entries;
    aeUringReq *reqs; /* an array of reqs based on fd, for standard poll */
} aeApiState;

static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    
    if (io_uring_queue_init(eventLoop->setsize, &state->ring, 0) < 0) {
        zfree(state);
        return -1;
    }
    
    state->entries = eventLoop->setsize;
    state->reqs = zcalloc(sizeof(aeUringReq) * eventLoop->setsize);
    eventLoop->apidata = state;
    return 0;
}

static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;

    /* A true resize of io_uring requires creating a new ring and migrating, 
     * but we will just re-init or resize the reqs array for now. */
    state->reqs = zrealloc(state->reqs, sizeof(aeUringReq) * setsize);
    /* zero out the new part */
    if (setsize > state->entries) {
        memset(state->reqs + state->entries, 0, sizeof(aeUringReq) * (setsize - state->entries));
    }
    state->entries = setsize;
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;
    io_uring_queue_exit(&state->ring);
    zfree(state->reqs);
    zfree(state);
}

static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&state->ring);
    if (!sqe) {
        /* flush and get a new one */
        io_uring_submit(&state->ring);
        sqe = io_uring_get_sqe(&state->ring);
        if (!sqe) return -1;
    }

    int poll_mask = 0;
    if (mask & AE_READABLE) poll_mask |= POLLIN;
    if (mask & AE_WRITABLE) poll_mask |= POLLOUT;

    io_uring_prep_poll_add(sqe, fd, poll_mask);
    
    aeUringReq *req = &state->reqs[fd];
    req->op_type = AE_IOURING_OP_POLL;
    req->fd = fd;
    req->mask = mask;
    
    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(&state->ring);
    return 0;
}

static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    (void)mask;
    aeApiState *state = eventLoop->apidata;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&state->ring);
    
    if (!sqe) {
        io_uring_submit(&state->ring);
        sqe = io_uring_get_sqe(&state->ring);
        if (!sqe) return;
    }
    
    /* We just cancel the previous poll request */
    aeUringReq *req = &state->reqs[fd];
    io_uring_prep_poll_remove(sqe, (uintptr_t)req);
    io_uring_submit(&state->ring);
    
    /* Clean up the mask if necessary, though typical aeApiDelEvent tracks
     * the mask via eventLoop->events[fd].mask so we rely on that on re-poll if needed. */
}

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;
    struct __kernel_timespec ts;

    if (tvp) {
        ts.tv_sec = tvp->tv_sec;
        ts.tv_nsec = tvp->tv_usec * 1000;
    }

    struct io_uring_cqe *cqe;
    unsigned head;
    
    bool wait = (tvp == NULL || (tvp->tv_sec != 0 || tvp->tv_usec != 0));
    
    if (wait) {
        retval = io_uring_wait_cqe_timeout(&state->ring, &cqe, tvp ? &ts : NULL);
    } else {
        retval = io_uring_peek_cqe(&state->ring, &cqe);
    }

    if (retval == 0 || retval == -ETIME) {
        /* Count how many cqes we harvest */
        unsigned count = 0;
        io_uring_for_each_cqe(&state->ring, head, cqe) {
            aeUringReq *req = io_uring_cqe_get_data(cqe);
            if (!req) {
                count++;
                continue;
            }

            if (req->op_type == AE_IOURING_OP_POLL) {
                int mask = 0;
                int revents = cqe->res;
                
                if (revents > 0) {
                    if (revents & POLLIN) mask |= AE_READABLE;
                    if (revents & POLLOUT) mask |= AE_WRITABLE;
                    if (revents & POLLERR) mask |= AE_WRITABLE | AE_READABLE;
                    if (revents & POLLHUP) mask |= AE_WRITABLE | AE_READABLE;
                    
                    eventLoop->fired[numevents].fd = req->fd;
                    eventLoop->fired[numevents].mask = mask;
                    numevents++;
                }
                
                /* one-shot, so re-add if needed */
                if (eventLoop->events[req->fd].mask != AE_NONE && revents >= 0) {
                    aeApiAddEvent(eventLoop, req->fd, eventLoop->events[req->fd].mask);
                }
            } else if (req->op_type == AE_IOURING_OP_PROACTOR) {
                if (req->completion_handler) {
                    req->completion_handler(req->client_data, cqe->res);
                }
                zfree(req);
            }
            count++;
        }
        if (count > 0) {
            io_uring_cq_advance(&state->ring, count);
        }
    } else if (retval != -EAGAIN && retval != -EINTR && retval != -ETIME) {
        panic("aeApiPoll: io_uring_wait_cqe error %d", retval);
    }

    return numevents;
}

static char *aeApiName(void) {
    return "io_uring";
}

/* 
 * Proactor hooks 
 */
void aeApiSubmitRead(aeEventLoop *eventLoop, int fd, void *buf, size_t len, void (*completion)(void*, int), void *client_data) {
    aeApiState *state = eventLoop->apidata;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&state->ring);
    if (!sqe) {
        io_uring_submit(&state->ring);
        sqe = io_uring_get_sqe(&state->ring);
    }
    
    aeUringReq *req = zmalloc(sizeof(aeUringReq));
    req->op_type = AE_IOURING_OP_PROACTOR;
    req->fd = fd;
    req->completion_handler = completion;
    req->client_data = client_data;
    
    io_uring_prep_read(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(&state->ring);
}

void aeApiSubmitWrite(aeEventLoop *eventLoop, int fd, void *buf, size_t len, void (*completion)(void*, int), void *client_data) {
    aeApiState *state = eventLoop->apidata;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&state->ring);
    if (!sqe) {
        io_uring_submit(&state->ring);
        sqe = io_uring_get_sqe(&state->ring);
    }
    
    aeUringReq *req = zmalloc(sizeof(aeUringReq));
    req->op_type = AE_IOURING_OP_PROACTOR;
    req->fd = fd;
    req->completion_handler = completion;
    req->client_data = client_data;
    
    io_uring_prep_write(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(&state->ring);
}
