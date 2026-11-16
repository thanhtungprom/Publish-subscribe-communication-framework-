#ifndef NETWORK_UTILS_H
#define NETWORK_UTILS_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Set socket to non-blocking mode */
bool set_nonblocking(int fd);

/* Disable Nagle's algorithm for low-latency dispatch */
bool set_tcp_nodelay(int fd);

/* Set socket send and receive buffer sizes */
bool set_socket_buffer_size(int fd, int sndbuf_size, int rcvbuf_size);

/* Create and bind server listening TCP socket */
int create_server_socket(const char *host, int port, int backlog);

/* Connect to remote server socket */
int connect_to_server(const char *host, int port);

#ifdef __cplusplus
}
#endif

#endif /* NETWORK_UTILS_H */
