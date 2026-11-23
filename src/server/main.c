#include "server_epoll.h"
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>

static server_epoll_t *g_server = NULL;

static void sig_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        printf("\n[SIGNAL] Received signal %d (%s), terminating gracefully...\n",
               sig, sig == SIGINT ? "SIGINT" : "SIGTERM");
        if (g_server) {
            server_stop(g_server);
        }
    }
}

int main(int argc, char *argv[]) {
    int port = 9000;
    const char *bind_ip = "0.0.0.0";

    if (argc >= 2) {
        port = atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Invalid port: %s. Using default 9000.\n", argv[1]);
            port = 9000;
        }
    }
    if (argc >= 3) {
        bind_ip = argv[2];
    }

    /* Ignore SIGPIPE so writing to disconnected client doesn't kill the server process */
    signal(SIGPIPE, SIG_IGN);

    /* Setup signal handlers for graceful shutdown */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    g_server = server_create(bind_ip, port);
    if (!g_server) {
        fprintf(stderr, "Failed to initialize server on %s:%d\n", bind_ip, port);
        return 1;
    }

    int rc = server_run(g_server);

    server_free(g_server);
    g_server = NULL;

    return rc;
}
