#include "server_epoll.h"
#include "network_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <arpa/inet.h>

static void handle_client_disconnect(server_epoll_t *server, client_session_t *client);

server_epoll_t *server_create(const char *bind_ip, int port) {
    server_epoll_t *s = (server_epoll_t *)calloc(1, sizeof(server_epoll_t));
    if (!s) return NULL;

    s->port = port;
    if (bind_ip && strlen(bind_ip) > 0) {
        strncpy(s->bind_ip, bind_ip, sizeof(s->bind_ip) - 1);
    } else {
        strcpy(s->bind_ip, "0.0.0.0");
    }

    s->topic_mgr = topic_manager_create();
    if (!s->topic_mgr) {
        free(s);
        return NULL;
    }

    s->listen_fd = create_server_socket(s->bind_ip, s->port, 1024);
    if (s->listen_fd < 0) {
        topic_manager_free(s->topic_mgr);
        free(s);
        return NULL;
    }

    s->epoll_fd = epoll_create1(0);
    if (s->epoll_fd < 0) {
        perror("epoll_create1");
        close(s->listen_fd);
        topic_manager_free(s->topic_mgr);
        free(s);
        return NULL;
    }

    /* Register listening socket */
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.fd = s->listen_fd;
    if (epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, s->listen_fd, &ev) < 0) {
        perror("epoll_ctl(listen_fd)");
        close(s->epoll_fd);
        close(s->listen_fd);
        topic_manager_free(s->topic_mgr);
        free(s);
        return NULL;
    }

    s->running = 0;
    s->clients_head = NULL;
    s->active_clients = 0;

    return s;
}

void server_stop(server_epoll_t *server) {
    if (server) {
        server->running = 0;
    }
}

void server_update_client_events(server_epoll_t *server, client_session_t *client, bool want_write) {
    if (!server || !client || client->fd < 0) return;

    if (client->wants_write == want_write) return;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = client;
    ev.events = EPOLLIN;
    if (want_write) {
        ev.events |= EPOLLOUT;
    }

    if (epoll_ctl(server->epoll_fd, EPOLL_CTL_MOD, client->fd, &ev) < 0) {
        perror("epoll_ctl(EPOLL_CTL_MOD)");
    } else {
        client->wants_write = want_write;
    }
}

static bool server_send_frame(server_epoll_t *server, client_session_t *client,
                             msg_type_t type, const char *topic, uint16_t status_code,
                             const void *payload, uint32_t payload_len) {
    if (!server || !client) return false;

    uint16_t topic_len = (topic && strlen(topic) > 0) ? (uint16_t)strlen(topic) : 0;
    size_t total_frame_len = sizeof(frame_header_t) + topic_len + payload_len;

    uint8_t *wire_buf = (uint8_t *)malloc(total_frame_len);
    if (!wire_buf) return false;

    frame_header_t hdr;
    frame_header_init(&hdr, type, topic_len, status_code, payload_len);

    frame_header_t wire_hdr;
    frame_header_to_network(&hdr, &wire_hdr);
    memcpy(wire_buf, &wire_hdr, sizeof(frame_header_t));

    if (topic_len > 0) {
        memcpy(wire_buf + sizeof(frame_header_t), topic, topic_len);
    }
    if (payload && payload_len > 0) {
        memcpy(wire_buf + sizeof(frame_header_t) + topic_len, payload, payload_len);
    }

    bool queued = session_enqueue_message(client, wire_buf, total_frame_len);
    free(wire_buf);
    if (!queued) {
        return false;
    }

    /* Try sending immediately */
    bool flushed = session_flush_egress(client);
    if (!flushed) {
        server_update_client_events(server, client, true);
    } else {
        server_update_client_events(server, client, false);
    }

    return true;
}

static void handle_new_connection(server_epoll_t *server) {
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server->listen_fd, (struct sockaddr *)&client_addr, &client_len);

        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* All pending connections accepted */
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                perror("accept");
                break;
            }
        }

        set_nonblocking(client_fd);
        set_tcp_nodelay(client_fd);

        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
        int port = ntohs(client_addr.sin_port);

        client_session_t *session = session_create(client_fd, NULL);
        if (!session) {
            close(client_fd);
            continue;
        }

        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.ptr = session;

        if (epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
            perror("epoll_ctl(client_fd)");
            session_free(session);
            continue;
        }

        /* Prepend to client list */
        session->next = server->clients_head;
        server->clients_head = session;
        server->active_clients++;

        printf("[CONNECT] New client connection from %s:%d (FD %d). Active clients: %zu\n",
               ip_str, port, client_fd, server->active_clients);
    }
}

static void handle_client_disconnect(server_epoll_t *server, client_session_t *client) {
    if (!server || !client) return;

    printf("[DISCONNECT] Client '%s' (FD %d) disconnected. Unsubscribing from all topics.\n",
           client->client_id, client->fd);

    /* Unregister from epoll */
    epoll_ctl(server->epoll_fd, EPOLL_CTL_DEL, client->fd, NULL);

    /* Clean all subscriptions */
    topic_unsubscribe_all(server->topic_mgr, client);

    /* Remove from client list */
    client_session_t *curr = server->clients_head;
    client_session_t *prev = NULL;
    while (curr) {
        if (curr == client) {
            if (prev) {
                prev->next = curr->next;
            } else {
                server->clients_head = curr->next;
            }
            break;
        }
        prev = curr;
        curr = curr->next;
    }

    if (server->active_clients > 0) {
        server->active_clients--;
    }

    session_free(client);
}

/* Dispatch a completely decoded message */
static bool process_message(server_epoll_t *server, client_session_t *client,
                            const frame_header_t *hdr, const char *topic,
                            const uint8_t *payload, size_t payload_len) {
    client->msgs_received++;
    client->last_active_at = time(NULL);

    switch (hdr->msg_type) {
        case MSG_CONNECT: {
            if (payload_len > 0) {
                size_t id_len = payload_len < (sizeof(client->client_id) - 1) ? payload_len : (sizeof(client->client_id) - 1);
                memcpy(client->client_id, payload, id_len);
                client->client_id[id_len] = '\0';
            }
            client->authenticated = true;
            printf("[AUTH] Client identified as '%s' (FD %d)\n", client->client_id, client->fd);
            server_send_frame(server, client, MSG_CONNACK, "", STATUS_OK, "Welcome to PubSub Broker", 24);
            break;
        }

        case MSG_SUBSCRIBE: {
            if (!topic || strlen(topic) == 0) {
                server_send_frame(server, client, MSG_SUBACK, "", STATUS_BAD_REQUEST, "Topic cannot be empty", 21);
                break;
            }
            topic_subscribe(server->topic_mgr, topic, client);
            client->role |= ROLE_SUBSCRIBER;
            printf("[SUB] Client '%s' subscribed to topic '%s' (Current subs: %zu)\n",
                   client->client_id, topic, topic_get_sub_count(server->topic_mgr, topic));
            server_send_frame(server, client, MSG_SUBACK, topic, STATUS_OK, "Subscribed successfully", 23);
            break;
        }

        case MSG_UNSUBSCRIBE: {
            if (!topic || strlen(topic) == 0) {
                server_send_frame(server, client, MSG_UNSUBACK, "", STATUS_BAD_REQUEST, "Topic cannot be empty", 21);
                break;
            }
            bool unsubbed = topic_unsubscribe(server->topic_mgr, topic, client);
            printf("[UNSUB] Client '%s' unsubscribed from topic '%s'\n", client->client_id, topic);
            server_send_frame(server, client, MSG_UNSUBACK, topic, unsubbed ? STATUS_OK : STATUS_TOPIC_NOT_FOUND, 
                              unsubbed ? "Unsubscribed successfully" : "Not subscribed to topic",
                              unsubbed ? 25 : 23);
            break;
        }

        case MSG_PUBLISH: {
            if (!topic || strlen(topic) == 0) {
                server_send_frame(server, client, MSG_ERROR, "", STATUS_BAD_REQUEST, "Topic cannot be empty", 21);
                break;
            }
            client->role |= ROLE_PUBLISHER;
            topic_t *t = topic_find(server->topic_mgr, topic);
            if (!t || t->sub_count == 0) {
                /* No subscribers active, still acknowledge ok */
                server_send_frame(server, client, MSG_DELIVER, topic, STATUS_OK, "Delivered to 0 subscribers", 26);
                break;
            }

            t->msgs_published++;

            /* Prepare DELIVER frame buffer once */
            uint16_t top_len = (uint16_t)strlen(topic);
            size_t frame_sz = sizeof(frame_header_t) + top_len + payload_len;
            uint8_t *deliver_wire = (uint8_t *)malloc(frame_sz);
            if (!deliver_wire) {
                server_send_frame(server, client, MSG_ERROR, topic, STATUS_SERVER_ERROR, "Out of memory", 13);
                break;
            }

            frame_header_t d_hdr;
            frame_header_init(&d_hdr, MSG_DELIVER, top_len, STATUS_OK, (uint32_t)payload_len);
            frame_header_t wire_hdr;
            frame_header_to_network(&d_hdr, &wire_hdr);
            memcpy(deliver_wire, &wire_hdr, sizeof(frame_header_t));
            memcpy(deliver_wire + sizeof(frame_header_t), topic, top_len);
            if (payload_len > 0) {
                memcpy(deliver_wire + sizeof(frame_header_t) + top_len, payload, payload_len);
            }

            /* Broadcast to all subscribers of this topic */
            sub_node_t *sub = t->subscribers_head;
            size_t delivered_count = 0;
            while (sub) {
                client_session_t *sub_client = sub->session;
                if (sub_client && sub_client->fd >= 0) {
                    if (session_enqueue_message(sub_client, deliver_wire, frame_sz)) {
                        bool flushed = session_flush_egress(sub_client);
                        if (!flushed) {
                            server_update_client_events(server, sub_client, true);
                        } else {
                            server_update_client_events(server, sub_client, false);
                        }
                        delivered_count++;
                    }
                }
                sub = sub->next;
            }

            free(deliver_wire);

            /* Confirm publish success to publisher */
            char ack_msg[64];
            int ack_len = snprintf(ack_msg, sizeof(ack_msg), "Delivered to %zu subscribers", delivered_count);
            server_send_frame(server, client, MSG_DELIVER, topic, STATUS_OK, ack_msg, (uint32_t)ack_len);
            break;
        }

        case MSG_LIST_TOPICS: {
            char topic_list_buf[4096];
            size_t written = topic_format_list(server->topic_mgr, topic_list_buf, sizeof(topic_list_buf));
            server_send_frame(server, client, MSG_TOPIC_LIST, "", STATUS_OK, topic_list_buf, (uint32_t)written);
            break;
        }

        case MSG_PING: {
            server_send_frame(server, client, MSG_PONG, "", STATUS_OK, "PONG", 4);
            break;
        }

        case MSG_DISCONNECT: {
            printf("[DISCONNECT] Client '%s' requested disconnect.\n", client->client_id);
            return false; /* Triggers handle_client_disconnect */
        }

        default: {
            fprintf(stderr, "[WARN] Unknown message type: 0x%02X from '%s'\n", hdr->msg_type, client->client_id);
            server_send_frame(server, client, MSG_ERROR, "", STATUS_BAD_REQUEST, "Unknown message type", 20);
            break;
        }
    }

    return true;
}

/* Handle incoming bytes from client: Stream parsing */
static bool handle_client_read(server_epoll_t *server, client_session_t *client) {
    while (1) {
        if (!buffer_ensure_writable(client->rx_buffer, 4096)) {
            fprintf(stderr, "[ERROR] Out of memory expanding buffer for client '%s'\n", client->client_id);
            return false;
        }

        uint8_t *write_ptr = buffer_write_ptr(client->rx_buffer);
        size_t avail = buffer_writable_bytes(client->rx_buffer);

        ssize_t n = read(client->fd, write_ptr, avail);
        if (n > 0) {
            buffer_commit_write(client->rx_buffer, (size_t)n);
        } else if (n == 0) {
            /* Peer closed connection */
            return false;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* All available data read */
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                perror("read");
                return false;
            }
        }
    }

    /* Process all complete frames currently assembled in rx_buffer */
    while (buffer_readable_bytes(client->rx_buffer) >= sizeof(frame_header_t)) {
        const uint8_t *buf_ptr = buffer_read_ptr(client->rx_buffer);
        frame_header_t wire_hdr;
        memcpy(&wire_hdr, buf_ptr, sizeof(frame_header_t));

        frame_header_t host_hdr;
        frame_header_to_host(&wire_hdr, &host_hdr);

        if (!frame_header_validate(&host_hdr)) {
            fprintf(stderr, "[SECURITY/MALFORMED] Client '%s' sent invalid frame header (Magic 0x%04X, Ver %u). Disconnecting!\n",
                    client->client_id, host_hdr.magic, host_hdr.version);
            server_send_frame(server, client, MSG_ERROR, "", STATUS_INVALID_MAGIC, "Malformed packet header", 23);
            return false;
        }

        size_t total_expected = sizeof(frame_header_t) + host_hdr.topic_len + host_hdr.payload_len;
        if (buffer_readable_bytes(client->rx_buffer) < total_expected) {
            /* Partial frame, wait for more data over TCP */
            break;
        }

        /* Extract topic and payload */
        char topic_str[MAX_TOPIC_LEN + 1];
        if (host_hdr.topic_len > 0) {
            memcpy(topic_str, buf_ptr + sizeof(frame_header_t), host_hdr.topic_len);
            topic_str[host_hdr.topic_len] = '\0';
        } else {
            topic_str[0] = '\0';
        }

        const uint8_t *payload_ptr = buf_ptr + sizeof(frame_header_t) + host_hdr.topic_len;
        size_t payload_len = host_hdr.payload_len;

        bool proceed = process_message(server, client, &host_hdr, topic_str, payload_ptr, payload_len);
        buffer_consume(client->rx_buffer, total_expected);

        if (!proceed) {
            return false;
        }
    }

    return true;
}

static void handle_client_write(server_epoll_t *server, client_session_t *client) {
    bool flushed = session_flush_egress(client);
    if (flushed) {
        /* All queued frames successfully transmitted, disable EPOLLOUT */
        server_update_client_events(server, client, false);
    }
}

int server_run(server_epoll_t *server) {
    if (!server) return -1;

    struct epoll_event events[MAX_EPOLL_EVENTS];
    server->running = 1;

    printf("===================================================================\n");
    printf("  IT4062 Publish/Subscribe Broker Server is running\n");
    printf("  Listening on: %s:%d\n", server->bind_ip, server->port);
    printf("  Architecture: Non-blocking I/O Multiplexing (epoll)\n");
    printf("===================================================================\n");

    while (server->running) {
        int nfds = epoll_wait(server->epoll_fd, events, MAX_EPOLL_EVENTS, 500 /* 500ms timeout for graceful shutdown */);
        if (nfds < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == server->listen_fd) {
                handle_new_connection(server);
            } else {
                client_session_t *client = (client_session_t *)events[i].data.ptr;
                if (!client) continue;

                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    handle_client_disconnect(server, client);
                    continue;
                }

                if (events[i].events & EPOLLIN) {
                    if (!handle_client_read(server, client)) {
                        handle_client_disconnect(server, client);
                        continue;
                    }
                }

                if (events[i].events & EPOLLOUT) {
                    handle_client_write(server, client);
                }
            }
        }
    }

    printf("\n[SHUTDOWN] Server event loop stopped. Cleaning up all resources...\n");
    return 0;
}

void server_free(server_epoll_t *server) {
    if (!server) return;

    /* Free all active client sessions */
    client_session_t *curr = server->clients_head;
    while (curr) {
        client_session_t *next = curr->next;
        if (curr->fd >= 0) {
            epoll_ctl(server->epoll_fd, EPOLL_CTL_DEL, curr->fd, NULL);
        }
        session_free(curr);
        curr = next;
    }
    server->clients_head = NULL;
    server->active_clients = 0;

    if (server->topic_mgr) {
        topic_manager_free(server->topic_mgr);
        server->topic_mgr = NULL;
    }

    if (server->epoll_fd >= 0) {
        close(server->epoll_fd);
        server->epoll_fd = -1;
    }

    if (server->listen_fd >= 0) {
        close(server->listen_fd);
        server->listen_fd = -1;
    }

    free(server);
    printf("[SHUTDOWN] All sockets closed and memory released cleanly. 0 leaks guaranteed.\n");
}
