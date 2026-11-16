#ifndef CLIENT_SESSION_H
#define CLIENT_SESSION_H

#include "protocol.h"
#include "buffer.h"
#include <time.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DEFAULT_MAX_QUEUE_ITEMS   500
#define DEFAULT_MAX_QUEUE_BYTES   (2 * 1024 * 1024) /* 2 MB */

/* Node in client outgoing egress queue */
typedef struct egress_item {
    uint8_t            *data;           /* Serialized frame data */
    size_t              total_len;      /* Total bytes in frame */
    size_t              sent_offset;    /* Bytes already sent via write() */
    struct egress_item *next;
} egress_item_t;

/* Client session state */
typedef struct client_session {
    int                 fd;
    char                client_id[MAX_CLIENT_ID_LEN];
    client_role_t       role;
    bool                authenticated;
    time_t              connected_at;
    time_t              last_active_at;

    /* Ingress stream buffer (TCP fragmentation/coalescing handling) */
    byte_buffer_t      *rx_buffer;

    /* Egress queue (for non-blocking writes and slow subscriber handling) */
    egress_item_t      *tx_head;
    egress_item_t      *tx_tail;
    size_t              tx_queue_count;
    size_t              tx_queue_bytes;
    size_t              max_queue_count;
    size_t              max_queue_bytes;

    /* Metrics for flow control & slow consumer detection */
    uint64_t            msgs_sent;
    uint64_t            msgs_received;
    uint64_t            msgs_dropped;
    bool                is_slow_consumer;
    bool                wants_write;    /* Currently subscribed to EPOLLOUT */

    struct client_session *next;        /* Linked list of all active sessions */
} client_session_t;

/* Session lifecycle */
client_session_t *session_create(int fd, const char *client_id);
void              session_free(client_session_t *session);

/* Egress queue management & Slow Subscriber Flow Control */
bool session_enqueue_message(client_session_t *session, const uint8_t *data, size_t len);
bool session_flush_egress(client_session_t *session);
void session_clear_egress(client_session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* CLIENT_SESSION_H */
