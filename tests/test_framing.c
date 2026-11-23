/*
 * test_framing.c - Unit tests for the COMMON module
 *
 * Covers: buffer grow/compact/consume, header init + byte order + validate,
 * and a loopback socket round-trip using the network helpers.
 *
 * Run:  make test
 */

#include "buffer.h"
#include "network_utils.h"
#include "protocol.h"

#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int g_checks = 0;
static int g_failed = 0;

#define CHECK(cond, msg)                                                  \
    do {                                                                  \
        g_checks++;                                                       \
        if (!(cond)) {                                                    \
            g_failed++;                                                   \
            printf("  [FAIL] %s (%s:%d)\n", (msg), __FILE__, __LINE__);   \
        } else {                                                          \
            printf("  [ OK ] %s\n", (msg));                               \
        }                                                                 \
    } while (0)

/* ------------------------------------------------------------------ */
/* 1. Byte buffer                                                     */
/* ------------------------------------------------------------------ */

static void test_buffer_basic(void)
{
    printf("\n[1] byte_buffer: basic lifecycle\n");

    byte_buffer_t *buf = buffer_create(16);
    CHECK(buf != NULL, "buffer_create(16) returns a buffer");
    if (buf == NULL) {
        return;
    }
    CHECK(buf->capacity >= 16, "capacity >= requested");
    CHECK(buffer_readable_bytes(buf) == 0, "new buffer is empty");
    CHECK(buffer_writable_bytes(buf) == buf->capacity, "all bytes writable");

    const char *hello = "HELLO";
    CHECK(buffer_append(buf, hello, 5), "append 5 bytes");
    CHECK(buffer_readable_bytes(buf) == 5, "readable == 5");
    CHECK(memcmp(buffer_read_ptr(buf), "HELLO", 5) == 0, "read_ptr points at data");

    buffer_consume(buf, 2);
    CHECK(buffer_readable_bytes(buf) == 3, "consume 2 -> readable == 3");
    CHECK(memcmp(buffer_read_ptr(buf), "LLO", 3) == 0, "read_ptr advanced");

    buffer_consume(buf, 99); /* over-consume must be clamped, not UB */
    CHECK(buffer_readable_bytes(buf) == 0, "over-consume clamps to empty");

    buffer_reset(buf);
    CHECK(buffer_readable_bytes(buf) == 0, "reset empties the buffer");

    buffer_free(buf);
    printf("  [ OK ] buffer_free() releases memory (valgrind clean)\n");
}

static void test_buffer_growth_and_compact(void)
{
    printf("\n[2] byte_buffer: growth + compaction\n");

    byte_buffer_t *buf = buffer_create(16);
    if (buf == NULL) {
        CHECK(false, "buffer_create");
        return;
    }

    /* Write 1000 bytes one at a time: forces repeated compaction + doubling. */
    bool ok = true;
    for (int i = 0; i < 1000; i++) {
        unsigned char b = (unsigned char)(i & 0xFF);
        if (!buffer_append(buf, &b, 1)) {
            ok = false;
            break;
        }
        if (buffer_readable_bytes(buf) > 100) {
            buffer_consume(buf, 100); /* keep the window small */
        }
    }
    CHECK(ok, "1000 append/consume cycles succeed");
    CHECK(buf->capacity > 16, "capacity grew beyond the initial 16");

    size_t readable = buffer_readable_bytes(buf);
    CHECK(readable == 100, "readable window is exactly 100 bytes");
    const uint8_t *p = buffer_read_ptr(buf);
    CHECK(p != NULL && p[0] == (uint8_t)((1000 - 100) & 0xFF), "data order preserved");

    /* Single huge append: must grow, not truncate. */
    char *big = (char *)malloc(8192);
    if (big != NULL) {
        memset(big, 'A', 8192);
        CHECK(buffer_append(buf, big, 8192), "append 8192 bytes at once");
        CHECK(buffer_readable_bytes(buf) == 100 + 8192, "all 8192 bytes retained");
        free(big);
    }

    buffer_free(buf);
}

/* ------------------------------------------------------------------ */
/* 2. Protocol framing                                                */
/* ------------------------------------------------------------------ */

static void test_protocol_header(void)
{
    printf("\n[3] protocol: header init / byte order / validate\n");

    frame_header_t hdr;
    frame_header_init(&hdr, MSG_PUBLISH, 12, STATUS_OK, 1000);

    CHECK(hdr.magic == PROTOCOL_MAGIC, "magic == 0x5053");
    CHECK(hdr.version == PROTOCOL_VERSION, "version == 1");
    CHECK(hdr.msg_type == (uint8_t)MSG_PUBLISH, "msg_type == MSG_PUBLISH");
    CHECK(hdr.topic_len == 12, "topic_len set");
    CHECK(hdr.payload_len == 1000, "payload_len set");

    frame_header_t wire;
    frame_header_to_network(&hdr, &wire);
    CHECK(wire.magic == htons(PROTOCOL_MAGIC), "magic converted to network order");
    CHECK(wire.payload_len == htonl(1000), "payload_len converted to network order");

    frame_header_t back;
    frame_header_to_host(&wire, &back);
    CHECK(back.magic == hdr.magic && back.topic_len == hdr.topic_len &&
              back.status_code == hdr.status_code && back.payload_len == hdr.payload_len,
          "to_network -> to_host is lossless (round trip)");

    CHECK(sizeof(frame_header_t) == FRAME_HEADER_SIZE, "header is exactly 12 bytes on the wire");

    CHECK(frame_header_validate(&hdr), "valid header passes validation");

    frame_header_t bad = hdr;
    bad.magic = 0x1234;
    CHECK(!frame_header_validate(&bad), "wrong magic rejected");

    bad = hdr;
    bad.version = 0x09;
    CHECK(!frame_header_validate(&bad), "wrong version rejected");

    bad = hdr;
    bad.topic_len = (uint16_t)(MAX_TOPIC_LEN + 1);
    CHECK(!frame_header_validate(&bad), "topic_len > MAX_TOPIC_LEN rejected");

    bad = hdr;
    bad.payload_len = (uint32_t)(MAX_PAYLOAD_LEN + 1);
    CHECK(!frame_header_validate(&bad), "payload_len > MAX_PAYLOAD_LEN rejected");

    bad = hdr;
    bad.msg_type = 0x7F;
    CHECK(!frame_header_validate(&bad), "unknown msg_type rejected");

    CHECK(!frame_header_validate(NULL), "NULL header rejected");

    printf("\n[4] protocol: string tables\n");
    CHECK(strcmp(msg_type_to_str(MSG_CONNECT), "CONNECT") == 0, "msg_type_to_str(CONNECT)");
    CHECK(strcmp(msg_type_to_str((msg_type_t)0x99), "UNKNOWN") == 0, "unknown type -> UNKNOWN");
    CHECK(strcmp(status_code_to_str(STATUS_SLOW_CONSUMER), "SLOW_CONSUMER") == 0,
          "status_code_to_str(429)");
    CHECK(strcmp(status_code_to_str((status_code_t)999), "UNKNOWN_STATUS") == 0,
          "unknown status -> UNKNOWN_STATUS");
}

/* ------------------------------------------------------------------ */
/* 3. Network helpers on loopback                                     */
/* ------------------------------------------------------------------ */

static void test_network_loopback(void)
{
    printf("\n[5] network_utils: loopback listen/connect\n");

    const int port = 19000 + (int)(getpid() % 500);
    int listen_fd  = create_server_socket("127.0.0.1", port, 8);
    CHECK(listen_fd >= 0, "create_server_socket() succeeds");
    if (listen_fd < 0) {
        return;
    }
    CHECK(set_nonblocking(listen_fd), "set_nonblocking() on listener");
    CHECK(set_socket_buffer_size(listen_fd, 65536, 65536), "set_socket_buffer_size()");

    int client_fd = connect_to_server("127.0.0.1", port);
    CHECK(client_fd >= 0, "connect_to_server() succeeds");
    if (client_fd < 0) {
        close(listen_fd);
        return;
    }
    CHECK(set_tcp_nodelay(client_fd), "set_tcp_nodelay() on client");

    int accepted = accept(listen_fd, NULL, NULL);
    CHECK(accepted >= 0, "accept() returns a peer socket");

    if (accepted >= 0) {
        /* Send one real frame through the socket and parse it back. */
        frame_header_t hdr, wire;
        const char *topic = "weather/hanoi";
        const char *body  = "{\"temp\":31}";
        frame_header_init(&hdr, MSG_PUBLISH, (uint16_t)strlen(topic),
                          STATUS_OK, (uint32_t)strlen(body));
        frame_header_to_network(&hdr, &wire);

        unsigned char out[FRAME_HEADER_SIZE + 64];
        memcpy(out, &wire, FRAME_HEADER_SIZE);
        memcpy(out + FRAME_HEADER_SIZE, topic, strlen(topic));
        memcpy(out + FRAME_HEADER_SIZE + strlen(topic), body, strlen(body));
        size_t total = FRAME_HEADER_SIZE + strlen(topic) + strlen(body);

        ssize_t w = write(client_fd, out, total);
        CHECK(w == (ssize_t)total, "full frame written to socket");

        /* Read into a byte_buffer exactly like the server ingress path. */
        byte_buffer_t *rx = buffer_create(64);
        if (rx != NULL) {
            if (buffer_ensure_writable(rx, 256)) {
                ssize_t n = read(accepted, buffer_write_ptr(rx), 256);
                CHECK(n == (ssize_t)total, "frame received in one read");
                if (n > 0) {
                    buffer_commit_write(rx, (size_t)n);
                    CHECK(buffer_readable_bytes(rx) >= FRAME_HEADER_SIZE,
                          "enough bytes for a header");

                    frame_header_t got_wire, got;
                    memcpy(&got_wire, buffer_read_ptr(rx), FRAME_HEADER_SIZE);
                    frame_header_to_host(&got_wire, &got);
                    CHECK(frame_header_validate(&got), "received header validates");
                    CHECK(got.msg_type == MSG_PUBLISH, "received msg_type == MSG_PUBLISH");

                    char got_topic[MAX_TOPIC_LEN + 1];
                    memset(got_topic, 0, sizeof(got_topic));
                    memcpy(got_topic, buffer_read_ptr(rx) + FRAME_HEADER_SIZE, got.topic_len);
                    CHECK(strcmp(got_topic, topic) == 0, "topic parsed correctly");

                    char got_body[64];
                    memset(got_body, 0, sizeof(got_body));
                    memcpy(got_body, buffer_read_ptr(rx) + FRAME_HEADER_SIZE + got.topic_len,
                           got.payload_len);
                    CHECK(strcmp(got_body, body) == 0, "payload parsed correctly");

                    buffer_consume(rx, FRAME_HEADER_SIZE + got.topic_len + got.payload_len);
                    CHECK(buffer_readable_bytes(rx) == 0, "buffer fully consumed");
                }
            }
            buffer_free(rx);
        }

        close(accepted);
    }

    close(client_fd);
    close(listen_fd);

    CHECK(create_server_socket("127.0.0.1", 0, 8) < 0, "port 0 rejected");
    CHECK(connect_to_server(NULL, 9000) < 0, "NULL host rejected");
}

int main(void)
{
    printf("===============================================\n");
    printf(" Common module unit tests (protocol/buffer/net)\n");
    printf("===============================================\n");

    test_buffer_basic();
    test_buffer_growth_and_compact();
    test_protocol_header();
    test_network_loopback();

    printf("\n===============================================\n");
    printf(" Result: %d/%d checks passed", g_checks - g_failed, g_checks);
    if (g_failed > 0) {
        printf("  ->  %d FAILED\n", g_failed);
        printf("===============================================\n");
        return 1;
    }
    printf("  ->  ALL PASSED\n");
    printf("===============================================\n");
    return 0;
}
