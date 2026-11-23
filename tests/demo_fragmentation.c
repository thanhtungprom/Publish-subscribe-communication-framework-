/*
 * demo_fragmentation.c - LIVE DEMO for the common module
 *
 * Shows, on a real TCP connection, why the byte buffer + length-prefixed
 * framing are needed:
 *
 *   - the sender writes ONE 36-byte frame, but splits it into 1-byte write()s
 *     (worst-case TCP fragmentation),
 *   - the receiver reads in 5-byte chunks, so every read() gives a partial
 *     frame,
 *   - the buffer accumulates bytes until a complete frame is available, then
 *     parses it once.
 *
 * Run:  make demo
 */

#include "buffer.h"
#include "network_utils.h"
#include "protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define TOPIC     "weather/hanoi"
#define PAYLOAD   "{\"temp\":31}"
#define READ_CHUNK 5

/* Build the exact bytes that go on the wire. */
static size_t build_frame(uint8_t *out, size_t out_cap)
{
    frame_header_t hdr, wire;
    size_t topic_len = strlen(TOPIC);
    size_t body_len  = strlen(PAYLOAD);
    size_t total     = FRAME_HEADER_SIZE + topic_len + body_len;

    if (out_cap < total) {
        return 0;
    }

    frame_header_init(&hdr, MSG_PUBLISH, (uint16_t)topic_len, STATUS_OK,
                      (uint32_t)body_len);
    frame_header_to_network(&hdr, &wire);

    memcpy(out, &wire, FRAME_HEADER_SIZE);
    memcpy(out + FRAME_HEADER_SIZE, TOPIC, topic_len);
    memcpy(out + FRAME_HEADER_SIZE + topic_len, PAYLOAD, body_len);

    return total;
}

static void hexdump(const uint8_t *p, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        printf("%02X ", p[i]);
        if ((i + 1) % 16 == 0) {
            printf("\n          ");
        }
    }
    printf("\n");
}

/* Sender: connect, then push the frame one byte at a time. */
static int run_sender(int port)
{
    int fd = connect_to_server("127.0.0.1", port);
    if (fd < 0) {
        return 1;
    }

    uint8_t frame[128];
    size_t total = build_frame(frame, sizeof(frame));
    if (total == 0) {
        (void)close(fd);
        return 1;
    }

    for (size_t i = 0; i < total; i++) {
        ssize_t n = write(fd, &frame[i], 1);
        if (n != 1) {
            break;
        }
        usleep(15000); /* 15 ms between bytes: keep the pieces separate */
    }

    (void)close(fd);
    return 0;
}

/* Receiver: read small chunks, accumulate in the buffer, parse when complete. */
static int run_receiver(int listen_fd)
{
    int fd = accept(listen_fd, NULL, NULL);
    if (fd < 0) {
        return 1;
    }

    byte_buffer_t *rx = buffer_create(64);
    if (rx == NULL) {
        (void)close(fd);
        return 1;
    }

    printf("  Receiver: read() theo tung chunk %d byte\n", READ_CHUNK);
    printf("  %-4s %-9s %-11s %-9s %s\n", "lan", "read()", "readable", "can", "hanh dong");
    printf("  %s\n", "---------------------------------------------------------------");

    int round  = 0;
    int parsed = 0;

    while (!parsed && round < 200) {
        round++;

        if (!buffer_ensure_writable(rx, READ_CHUNK)) {
            break;
        }
        ssize_t n = read(fd, buffer_write_ptr(rx), READ_CHUNK);
        if (n <= 0) {
            printf("  %-4d %-9s %-11s %-9s %s\n", round, "EOF", "-", "-", "ket noi dong");
            break;
        }
        buffer_commit_write(rx, (size_t)n);

        size_t readable = buffer_readable_bytes(rx);

        /* Enough for a header yet? */
        if (readable < FRAME_HEADER_SIZE) {
            printf("  %-4d %-9zd %-11zu %-9s %s\n", round, n, readable,
                   "chua", "chua du 12 byte header -> cho");
            continue;
        }

        frame_header_t wire, hdr;
        memcpy(&wire, buffer_read_ptr(rx), FRAME_HEADER_SIZE);
        frame_header_to_host(&wire, &hdr);

        if (!frame_header_validate(&hdr)) {
            printf("  %-4d %-9zd %-11zu %-9s %s\n", round, n, readable,
                   "sai", "header khong hop le -> dong ket noi");
            break;
        }

        size_t need = FRAME_HEADER_SIZE + hdr.topic_len + hdr.payload_len;

        if (readable < need) {
            printf("  %-4d %-9zd %-11zu %-9s %s (%zu/%zu)\n", round, n, readable,
                   "chua", "da co header, cho payload", readable, need);
            continue;
        }

        /* Complete frame: parse it. */
        char topic[MAX_TOPIC_LEN + 1];
        memset(topic, 0, sizeof(topic));
        memcpy(topic, buffer_read_ptr(rx) + FRAME_HEADER_SIZE, hdr.topic_len);

        char body[MAX_PAYLOAD_LEN + 1];
        memset(body, 0, sizeof(body));
        memcpy(body, buffer_read_ptr(rx) + FRAME_HEADER_SIZE + hdr.topic_len,
               hdr.payload_len);
        body[hdr.payload_len] = '\0';

        printf("  %-4d %-9zd %-11zu %-9s %s\n", round, n, readable,
               "DU", "-> PARSE FRAME");

        printf("\n  --- Frame tach duoc sau %d lan read() ---\n", round);
        printf("      magic       = 0x%04X  ('P','S')\n", hdr.magic);
        printf("      version     = %u\n", hdr.version);
        printf("      msg_type    = %u (%s)\n", hdr.msg_type,
               msg_type_to_str((msg_type_t)hdr.msg_type));
        printf("      topic_len   = %u\n", hdr.topic_len);
        printf("      status_code = %u (%s)\n", hdr.status_code,
               status_code_to_str((status_code_t)hdr.status_code));
        printf("      payload_len = %u\n", hdr.payload_len);
        printf("      topic       = \"%s\"\n", topic);
        printf("      payload     = \"%s\"\n", body);

        buffer_consume(rx, need);
        printf("\n      buffer_consume(%zu) -> readable con lai = %zu\n",
               need, buffer_readable_bytes(rx));
        parsed = 1;
    }

    buffer_free(rx);
    (void)close(fd);
    return parsed ? 0 : 1;
}

int main(void)
{
    printf("===============================================================\n");
    printf(" DEMO: TCP fragmentation + length-prefixed framing\n");
    printf(" (toan bo phan nay la module common/)\n");
    printf("===============================================================\n\n");

    /* 1. Show the bytes that will travel on the wire. */
    uint8_t frame[128];
    size_t total = build_frame(frame, sizeof(frame));

    printf("[1] Sender tao MOT frame %zu byte va gui di:\n", total);
    printf("      ");
    hexdump(frame, total);
    printf("    Header = %d byte | topic = %zu byte | payload = %zu byte\n\n",
           FRAME_HEADER_SIZE, strlen(TOPIC), strlen(PAYLOAD));

    /* 2. Listen, then fork: child sends byte by byte, parent parses. */
    const int port = 21000 + (int)(getpid() % 500);
    int listen_fd = create_server_socket("127.0.0.1", port, 4);
    if (listen_fd < 0) {
        fprintf(stderr, "Khong tao duoc listener (port %d)\n", port);
        return 1;
    }

    printf("[2] Sender gui frame tren bang %zu lan write(1 byte) rieng biet\n", total);
    printf("    -> tren duong truyen day la %zu manh roi rac\n\n", total);

    pid_t pid = fork();
    if (pid < 0) {
        (void)close(listen_fd);
        return 1;
    }

    if (pid == 0) {                    /* child = sender */
        (void)close(listen_fd);
        _exit(run_sender(port));
    }

    /* parent = receiver */
    int rc = run_receiver(listen_fd);
    (void)close(listen_fd);
    (void)waitpid(pid, NULL, 0);

    printf("\n[3] Ket luan\n");
    printf("    - TCP khong bao toan ranh gioi message: %zu lan write = nhieu lan read.\n",
           total);
    printf("    - Nho byte_buffer + header co length, receiver van ghep duoc\n");
    printf("      chinh xac 1 frame va parse dung topic/payload.\n");
    printf("    - Neu doc thang vao struct (khong buffer) thi lan read() dau chi\n");
    printf("      co %d byte -> doc thieu header -> sai ngay.\n\n", READ_CHUNK);

    return rc;
}
