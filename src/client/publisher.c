#include "protocol.h"
#include "network_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <time.h>

static bool send_frame(int fd, msg_type_t type, const char *topic, uint16_t status_code,
                      const void *payload, uint32_t payload_len) {
    uint16_t top_len = (topic && strlen(topic) > 0) ? (uint16_t)strlen(topic) : 0;
    size_t total_len = sizeof(frame_header_t) + top_len + payload_len;

    uint8_t *buf = (uint8_t *)malloc(total_len);
    if (!buf) return false;

    frame_header_t hdr;
    frame_header_init(&hdr, type, top_len, status_code, payload_len);

    frame_header_t wire_hdr;
    frame_header_to_network(&hdr, &wire_hdr);
    memcpy(buf, &wire_hdr, sizeof(frame_header_t));

    if (top_len > 0) {
        memcpy(buf + sizeof(frame_header_t), topic, top_len);
    }
    if (payload && payload_len > 0) {
        memcpy(buf + sizeof(frame_header_t) + top_len, payload, payload_len);
    }

    size_t sent = 0;
    while (sent < total_len) {
        ssize_t n = write(fd, buf + sent, total_len - sent);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR)) continue;
            free(buf);
            return false;
        }
        sent += (size_t)n;
    }

    free(buf);
    return true;
}

static bool read_exact(int fd, void *buf, size_t count) {
    size_t received = 0;
    while (received < count) {
        ssize_t n = read(fd, (uint8_t *)buf + received, count - received);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR)) continue;
            return false;
        }
        received += (size_t)n;
    }
    return true;
}

static bool receive_frame(int fd, frame_header_t *out_hdr, char *out_topic, char *out_payload) {
    frame_header_t wire_hdr;
    if (!read_exact(fd, &wire_hdr, sizeof(wire_hdr))) {
        return false;
    }

    frame_header_to_host(&wire_hdr, out_hdr);
    if (!frame_header_validate(out_hdr)) {
        fprintf(stderr, "[ERROR] Received invalid frame header from server\n");
        return false;
    }

    if (out_hdr->topic_len > 0) {
        if (!read_exact(fd, out_topic, out_hdr->topic_len)) return false;
        out_topic[out_hdr->topic_len] = '\0';
    } else {
        out_topic[0] = '\0';
    }

    if (out_hdr->payload_len > 0) {
        if (!read_exact(fd, out_payload, out_hdr->payload_len)) return false;
        out_payload[out_hdr->payload_len] = '\0';
    } else {
        out_payload[0] = '\0';
    }

    return true;
}

int main(int argc, char *argv[]) {
    const char *host = "127.0.0.1";
    int port = 9000;
    char client_id[64] = "pub_client";

    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = atoi(argv[2]);
    if (argc >= 4) strncpy(client_id, argv[3], sizeof(client_id) - 1);

    printf("Connecting to PubSub Broker at %s:%d as '%s'...\n", host, port, client_id);
    int sock_fd = connect_to_server(host, port);
    if (sock_fd < 0) {
        fprintf(stderr, "Failed to connect to broker at %s:%d\n", host, port);
        return 1;
    }

    /* Send CONNECT */
    if (!send_frame(sock_fd, MSG_CONNECT, "", STATUS_OK, client_id, (uint32_t)strlen(client_id))) {
        fprintf(stderr, "Failed to send CONNECT frame\n");
        close(sock_fd);
        return 1;
    }

    /* Expect CONNACK */
    frame_header_t res_hdr;
    char topic_buf[MAX_TOPIC_LEN + 1];
    char payload_buf[4096];
    if (!receive_frame(sock_fd, &res_hdr, topic_buf, payload_buf) || res_hdr.msg_type != MSG_CONNACK) {
        fprintf(stderr, "Failed to receive valid CONNACK\n");
        close(sock_fd);
        return 1;
    }
    printf("[CONNECTED] %s\n", payload_buf);

    /* Check if command line publish is specified */
    if (argc >= 6 && strcmp(argv[4], "--pub") == 0) {
        const char *t = argv[5];
        const char *m = (argc >= 7) ? argv[6] : "test message";
        if (send_frame(sock_fd, MSG_PUBLISH, t, STATUS_OK, m, (uint32_t)strlen(m))) {
            if (receive_frame(sock_fd, &res_hdr, topic_buf, payload_buf)) {
                printf("[PUBLISHED] Topic: %s -> %s\n", t, payload_buf);
            }
        }
        close(sock_fd);
        return 0;
    }

    /* Interactive mode */
    printf("\n--- Publisher Interactive Console ---\n");
    printf("Commands:\n");
    printf("  pub <topic> <message>         : Publish a message to a topic\n");
    printf("  list                          : Request active topics list\n");
    printf("  ping                          : Send heartbeat ping\n");
    printf("  bench <topic> <count> <ms>    : Send <count> messages spaced by <ms>\n");
    printf("  quit                          : Exit\n");
    printf("-------------------------------------\n\n");

    char line[1024];
    while (1) {
        printf("pubsub> ");
        fflush(stdout);

        if (!fgets(line, sizeof(line), stdin)) break;

        /* Strip trailing newline */
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) continue;

        if (strcmp(line, "quit") == 0 || strcmp(line, "exit") == 0) {
            send_frame(sock_fd, MSG_DISCONNECT, "", STATUS_OK, NULL, 0);
            break;
        } else if (strcmp(line, "list") == 0) {
            if (send_frame(sock_fd, MSG_LIST_TOPICS, "", STATUS_OK, NULL, 0)) {
                if (receive_frame(sock_fd, &res_hdr, topic_buf, payload_buf)) {
                    printf("Active Topics:\n%s\n", payload_buf);
                }
            }
        } else if (strcmp(line, "ping") == 0) {
            if (send_frame(sock_fd, MSG_PING, "", STATUS_OK, NULL, 0)) {
                if (receive_frame(sock_fd, &res_hdr, topic_buf, payload_buf)) {
                    printf("[PONG] Server is alive (%s)\n", payload_buf);
                }
            }
        } else if (strncmp(line, "pub ", 4) == 0) {
            char *top = line + 4;
            while (*top == ' ') top++;
            char *msg = strchr(top, ' ');
            if (!msg) {
                printf("Usage: pub <topic> <message>\n");
                continue;
            }
            *msg = '\0';
            msg++;
            while (*msg == ' ') msg++;

            if (send_frame(sock_fd, MSG_PUBLISH, top, STATUS_OK, msg, (uint32_t)strlen(msg))) {
                if (receive_frame(sock_fd, &res_hdr, topic_buf, payload_buf)) {
                    printf("[SERVER ACK] %s\n", payload_buf);
                }
            } else {
                fprintf(stderr, "Failed to send message to broker\n");
                break;
            }
        } else if (strncmp(line, "bench ", 6) == 0) {
            char b_topic[128];
            int b_count = 100;
            int b_delay_ms = 10;
            if (sscanf(line + 6, "%127s %d %d", b_topic, &b_count, &b_delay_ms) < 2) {
                printf("Usage: bench <topic> <count> [delay_ms]\n");
                continue;
            }
            printf("Starting benchmark: Sending %d messages to topic '%s' (interval %d ms)...\n",
                   b_count, b_topic, b_delay_ms);

            struct timeval t_start, t_end;
            gettimeofday(&t_start, NULL);

            int sent_ok = 0;
            for (int i = 1; i <= b_count; i++) {
                char b_msg[256];
                snprintf(b_msg, sizeof(b_msg), "Benchmark message #%d (ts=%ld)", i, (long)time(NULL));
                if (send_frame(sock_fd, MSG_PUBLISH, b_topic, STATUS_OK, b_msg, (uint32_t)strlen(b_msg))) {
                    /* Receive ACK */
                    if (receive_frame(sock_fd, &res_hdr, topic_buf, payload_buf)) {
                        sent_ok++;
                    }
                }
                if (b_delay_ms > 0) {
                    usleep((useconds_t)b_delay_ms * 1000);
                }
            }

            gettimeofday(&t_end, NULL);
            double elapsed = (t_end.tv_sec - t_start.tv_sec) + (t_end.tv_usec - t_start.tv_usec) / 1000000.0;
            printf("Finished benchmark: %d/%d sent successfully in %.2f seconds (%.1f msg/s)\n",
                   sent_ok, b_count, elapsed, elapsed > 0 ? sent_ok / elapsed : 0);
        } else {
            printf("Unknown command. Type 'pub <topic> <msg>', 'list', 'ping', 'bench' or 'quit'.\n");
        }
    }

    close(sock_fd);
    printf("Publisher closed.\n");
    return 0;
}
