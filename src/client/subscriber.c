#include "protocol.h"
#include "network_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <arpa/inet.h>
#include <time.h>

static volatile sig_atomic_t g_running = 1;

static void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

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
    while (received < count && g_running) {
        ssize_t n = read(fd, (uint8_t *)buf + received, count - received);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR)) continue;
            return false;
        }
        received += (size_t)n;
    }
    return received == count;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <host> <port> <client_id> [--delay-ms <N>] <topic1> [topic2 ...]\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1 9000 sub1 weather/hanoi news/tech\n", argv[0]);
        fprintf(stderr, "Example (Slow Subscriber): %s 127.0.0.1 9000 slow_sub --delay-ms 500 sports\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int port = atoi(argv[2]);
    const char *client_id = argv[3];
    int delay_ms = 0;
    int topic_start_idx = 4;

    if (argc > 5 && strcmp(argv[4], "--delay-ms") == 0) {
        delay_ms = atoi(argv[5]);
        topic_start_idx = 6;
    }

    if (topic_start_idx >= argc) {
        fprintf(stderr, "Error: You must specify at least one topic to subscribe to.\n");
        return 1;
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    printf("Connecting to Broker %s:%d as '%s' (Slow consumer delay: %d ms)...\n",
           host, port, client_id, delay_ms);

    int sock_fd = connect_to_server(host, port);
    if (sock_fd < 0) {
        fprintf(stderr, "Failed to connect to broker.\n");
        return 1;
    }

    /* Send CONNECT */
    if (!send_frame(sock_fd, MSG_CONNECT, "", STATUS_OK, client_id, (uint32_t)strlen(client_id))) {
        close(sock_fd);
        return 1;
    }

    /* Receive CONNACK */
    frame_header_t ack_hdr;
    frame_header_t wire_hdr;
    if (!read_exact(sock_fd, &wire_hdr, sizeof(wire_hdr))) {
        fprintf(stderr, "Failed to receive CONNACK header\n");
        close(sock_fd);
        return 1;
    }
    frame_header_to_host(&wire_hdr, &ack_hdr);
    if (ack_hdr.payload_len > 0) {
        char ack_msg[256];
        size_t to_read = ack_hdr.payload_len < sizeof(ack_msg) - 1 ? ack_hdr.payload_len : sizeof(ack_msg) - 1;
        read_exact(sock_fd, ack_msg, to_read);
        ack_msg[to_read] = '\0';
        printf("[CONNECTED] %s\n", ack_msg);
    }

    /* Subscribe to all topics */
    for (int i = topic_start_idx; i < argc; i++) {
        const char *t = argv[i];
        printf("[SUBSCRIBING] Requesting topic '%s'...\n", t);
        if (!send_frame(sock_fd, MSG_SUBSCRIBE, t, STATUS_OK, NULL, 0)) {
            fprintf(stderr, "Failed to send subscribe for '%s'\n", t);
            continue;
        }

        /* Read SUBACK */
        if (read_exact(sock_fd, &wire_hdr, sizeof(wire_hdr))) {
            frame_header_to_host(&wire_hdr, &ack_hdr);
            char resp_top[MAX_TOPIC_LEN + 1] = {0};
            char resp_pay[256] = {0};
            if (ack_hdr.topic_len > 0) read_exact(sock_fd, resp_top, ack_hdr.topic_len);
            if (ack_hdr.payload_len > 0) read_exact(sock_fd, resp_pay, ack_hdr.payload_len);
            printf("[SUBACK] Topic '%s': %s (Status: %u)\n", resp_top, resp_pay, ack_hdr.status_code);
        }
    }

    printf("\n>>> Ready to receive stream events (Press Ctrl+C to exit) <<<\n\n");

    uint64_t msg_count = 0;
    while (g_running) {
        if (delay_ms > 0) {
            /* Simulate slow consumer consumption delay */
            usleep((useconds_t)delay_ms * 1000);
        }

        if (!read_exact(sock_fd, &wire_hdr, sizeof(wire_hdr))) {
            if (!g_running) break;
            fprintf(stderr, "[DISCONNECTED] Server closed connection or error occurred.\n");
            break;
        }

        frame_header_t hdr;
        frame_header_to_host(&wire_hdr, &hdr);

        char topic_buf[MAX_TOPIC_LEN + 1] = {0};
        if (hdr.topic_len > 0) {
            if (!read_exact(sock_fd, topic_buf, hdr.topic_len)) break;
            topic_buf[hdr.topic_len] = '\0';
        }

        char *payload = NULL;
        if (hdr.payload_len > 0) {
            payload = (char *)malloc(hdr.payload_len + 1);
            if (payload) {
                if (!read_exact(sock_fd, payload, hdr.payload_len)) {
                    free(payload);
                    break;
                }
                payload[hdr.payload_len] = '\0';
            }
        }

        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        char time_str[32];
        strftime(time_str, sizeof(time_str), "%H:%M:%S", tm_info);

        msg_count++;
        if (hdr.msg_type == MSG_DELIVER) {
            printf("[%s] [#%lu] [TOPIC: %s] Payload (%u bytes): %s\n",
                   time_str, (unsigned long)msg_count, topic_buf, hdr.payload_len, payload ? payload : "(empty)");
        } else if (hdr.msg_type == MSG_ERROR) {
            printf("[%s] [ERROR from Server] %s (Status: %u)\n",
                   time_str, payload ? payload : "Unknown error", hdr.status_code);
        } else {
            printf("[%s] [FRAME: %s] Topic: %s | Payload: %s\n",
                   time_str, msg_type_to_str((msg_type_t)hdr.msg_type), topic_buf, payload ? payload : "");
        }

        if (payload) {
            free(payload);
        }
    }

    printf("\nUnsubscribing and closing connection...\n");
    for (int i = topic_start_idx; i < argc; i++) {
        send_frame(sock_fd, MSG_UNSUBSCRIBE, argv[i], STATUS_OK, NULL, 0);
    }
    send_frame(sock_fd, MSG_DISCONNECT, "", STATUS_OK, NULL, 0);
    close(sock_fd);

    printf("Subscriber closed. Total messages processed: %lu\n", (unsigned long)msg_count);
    return 0;
}
