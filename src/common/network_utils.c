/*
 * network_utils.c - Socket helpers (non-blocking, TCP options, connect/listen)
 *
 * IT4062 - Network Programming - Project 16: Publish/Subscribe Framework
 * Owner: Common module (shared by server + clients)
 *
 * Why this file exists
 * --------------------
 * The broker is a single-threaded epoll server, so every socket it owns must
 * be NON-BLOCKING: one slow peer must never stall the event loop. The same
 * helpers are reused by the clients, which keeps the socket setup code in one
 * place instead of being copy-pasted into main.c / publisher.c / subscriber.c.
 *
 * Address resolution goes through getaddrinfo(), so "localhost", "0.0.0.0",
 * "::1" and real DNS names all work without hand-written inet_pton() branches.
 */

#include "network_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Socket options                                                     */
/* ------------------------------------------------------------------ */

bool set_nonblocking(int fd)
{
    if (fd < 0) {
        return false;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return false;
    }
    return true;
}

/*
 * Nagle's algorithm batches small writes to save bandwidth, but for a message
 * broker that adds latency to every DELIVER. Disabled per connection.
 */
bool set_tcp_nodelay(int fd)
{
    if (fd < 0) {
        return false;
    }

    int one = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
        return false;
    }
    return true;
}

/*
 * A non-zero value is only a hint: the kernel clamps it to its own limits and
 * doubles it internally. Used to absorb publish bursts before write() returns
 * EAGAIN. A zero/negative value leaves the default untouched.
 */
bool set_socket_buffer_size(int fd, int sndbuf_size, int rcvbuf_size)
{
    if (fd < 0) {
        return false;
    }

    if (sndbuf_size > 0 &&
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, sizeof(sndbuf_size)) < 0) {
        return false;
    }
    if (rcvbuf_size > 0 &&
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, sizeof(rcvbuf_size)) < 0) {
        return false;
    }
    return true;
}

/* ------------------------------------------------------------------ */
/* Server side                                                        */
/* ------------------------------------------------------------------ */

/*
 * Returns a bound + listening socket (already non-blocking), or -1 on error.
 * SO_REUSEADDR lets the broker restart immediately after a crash instead of
 * waiting out TIME_WAIT.
 */
int create_server_socket(const char *host, int port, int backlog)
{
    if (port <= 0 || port > 65535) {
        return -1;
    }
    if (backlog <= 0) {
        backlog = SOMAXCONN;
    }

    /* getaddrinfo() wants the service as a string. */
    char port_str[8];
    (void)snprintf(port_str, sizeof(port_str), "%d", port);

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;   /* IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;  /* we are the listener */

    struct addrinfo *results = NULL;
    if (getaddrinfo(host, port_str, &hints, &results) != 0) {
        return -1;
    }

    int listen_fd = -1;

    for (struct addrinfo *rp = results; rp != NULL; rp = rp->ai_next) {
        int fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) {
            continue;
        }

        int one = 1;
        (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        /* Accept both families on one socket when possible. */
        if (rp->ai_family == AF_INET6) {
            int off = 0;
            (void)setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
        }

        if (bind(fd, rp->ai_addr, rp->ai_addrlen) < 0) {
            (void)close(fd);
            continue;
        }
        if (listen(fd, backlog) < 0) {
            (void)close(fd);
            continue;
        }
        if (!set_nonblocking(fd)) {
            (void)close(fd);
            continue;
        }

        listen_fd = fd;
        break;
    }

    freeaddrinfo(results);
    return listen_fd;
}

/* ------------------------------------------------------------------ */
/* Client side                                                        */
/* ------------------------------------------------------------------ */

/* Returns a connected socket, or -1 on error. */
int connect_to_server(const char *host, int port)
{
    if (host == NULL || port <= 0 || port > 65535) {
        return -1;
    }

    char port_str[8];
    (void)snprintf(port_str, sizeof(port_str), "%d", port);

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *results = NULL;
    if (getaddrinfo(host, port_str, &hints, &results) != 0) {
        return -1;
    }

    int sock_fd = -1;

    for (struct addrinfo *rp = results; rp != NULL; rp = rp->ai_next) {
        int fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) {
            continue;
        }

        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            sock_fd = fd;
            break;
        }

        (void)close(fd);
    }

    freeaddrinfo(results);

    if (sock_fd < 0) {
        return -1;
    }

    /* Low latency for the interactive CLI publish -> subscriber demo. */
    (void)set_tcp_nodelay(sock_fd);
    return sock_fd;
}
