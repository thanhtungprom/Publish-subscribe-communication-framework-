#ifndef SERVER_EPOLL_H
#define SERVER_EPOLL_H

#include "protocol.h"
#include "client_session.h"
#include "topic_manager.h"
#include <signal.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_EPOLL_EVENTS 128

typedef struct server_epoll {
    int                     listen_fd;
    int                     epoll_fd;
    int                     port;
    char                    bind_ip[64];
    volatile sig_atomic_t   running;

    topic_manager_t        *topic_mgr;
    client_session_t       *clients_head;
    size_t                  active_clients;
} server_epoll_t;

/* Server functions */
server_epoll_t *server_create(const char *bind_ip, int port);
void            server_free(server_epoll_t *server);
int             server_run(server_epoll_t *server);
void            server_stop(server_epoll_t *server);

/* Helper to enable/disable EPOLLOUT interest */
void            server_update_client_events(server_epoll_t *server, client_session_t *client, bool want_write);

#ifdef __cplusplus
}
#endif

#endif /* SERVER_EPOLL_H */
