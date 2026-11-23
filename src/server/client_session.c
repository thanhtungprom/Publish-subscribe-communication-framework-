#include "client_session.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

client_session_t *session_create(int fd, const char *client_id) {
    client_session_t *s = (client_session_t *)calloc(1, sizeof(client_session_t));
    if (!s) return NULL;

    s->fd = fd;
    if (client_id && strlen(client_id) > 0) {
        strncpy(s->client_id, client_id, sizeof(s->client_id) - 1);
    } else {
        snprintf(s->client_id, sizeof(s->client_id), "client_fd%d", fd);
    }

    s->role = ROLE_NONE;
    s->authenticated = false;
    s->connected_at = time(NULL);
    s->last_active_at = s->connected_at;

    s->rx_buffer = buffer_create(4096);
    if (!s->rx_buffer) {
        free(s);
        return NULL;
    }

    s->max_queue_count = DEFAULT_MAX_QUEUE_ITEMS;
    s->max_queue_bytes = DEFAULT_MAX_QUEUE_BYTES;
    s->tx_head = NULL;
    s->tx_tail = NULL;
    s->tx_queue_count = 0;
    s->tx_queue_bytes = 0;

    s->msgs_sent = 0;
    s->msgs_received = 0;
    s->msgs_dropped = 0;
    s->is_slow_consumer = false;
    s->wants_write = false;
    s->next = NULL;

    return s;
}

void session_clear_egress(client_session_t *session) {
    if (!session) return;
    egress_item_t *curr = session->tx_head;
    while (curr) {
        egress_item_t *next = curr->next;
        if (curr->data) {
            free(curr->data);
        }
        free(curr);
        curr = next;
    }
    session->tx_head = NULL;
    session->tx_tail = NULL;
    session->tx_queue_count = 0;
    session->tx_queue_bytes = 0;
}

void session_free(client_session_t *session) {
    if (!session) return;

    session_clear_egress(session);

    if (session->rx_buffer) {
        buffer_free(session->rx_buffer);
        session->rx_buffer = NULL;
    }

    if (session->fd >= 0) {
        close(session->fd);
        session->fd = -1;
    }

    free(session);
}

/* Drop the oldest message from egress queue to free space */
static void drop_oldest_item(client_session_t *session) {
    if (!session || !session->tx_head) return;

    egress_item_t *dropped = session->tx_head;
    session->tx_head = dropped->next;
    if (!session->tx_head) {
        session->tx_tail = NULL;
    }

    session->tx_queue_count--;
    session->tx_queue_bytes -= dropped->total_len;
    session->msgs_dropped++;

    if (dropped->data) {
        free(dropped->data);
    }
    free(dropped);
}

bool session_enqueue_message(client_session_t *session, const uint8_t *data, size_t len) {
    if (!session || !data || len == 0) return false;

    /* Check if queue is over watermark (Slow Subscriber Detection) */
    while (session->tx_queue_count >= session->max_queue_count || 
          (session->tx_queue_bytes + len) > session->max_queue_bytes) {
        
        session->is_slow_consumer = true;
        fprintf(stderr, "[FLOW CONTROL] Slow Subscriber '%s' (FD %d): queue full (%zu msgs, %zu bytes). Dropping oldest message! Total dropped: %lu\n",
                session->client_id, session->fd, session->tx_queue_count, session->tx_queue_bytes, (unsigned long)session->msgs_dropped + 1);

        drop_oldest_item(session);

        /* Safety break if queue empty but item alone exceeds max_queue_bytes */
        if (!session->tx_head && len > session->max_queue_bytes) {
            fprintf(stderr, "[ERROR] Single message (%zu bytes) exceeds max queue capacity (%zu bytes) for client '%s'\n",
                    len, session->max_queue_bytes, session->client_id);
            return false;
        }
    }

    egress_item_t *item = (egress_item_t *)malloc(sizeof(egress_item_t));
    if (!item) return false;

    item->data = (uint8_t *)malloc(len);
    if (!item->data) {
        free(item);
        return false;
    }

    memcpy(item->data, data, len);
    item->total_len = len;
    item->sent_offset = 0;
    item->next = NULL;

    if (!session->tx_tail) {
        session->tx_head = item;
        session->tx_tail = item;
    } else {
        session->tx_tail->next = item;
        session->tx_tail = item;
    }

    session->tx_queue_count++;
    session->tx_queue_bytes += len;
    return true;
}

/*
 * Flush pending egress queue to socket.
 * Returns true if all messages were flushed completely (queue is empty).
 * Returns false if socket buffer is full (EAGAIN) or error occurred.
 */
bool session_flush_egress(client_session_t *session) {
    if (!session || session->fd < 0) return true;

    while (session->tx_head) {
        egress_item_t *item = session->tx_head;
        size_t to_send = item->total_len - item->sent_offset;

        ssize_t n = write(session->fd, item->data + item->sent_offset, to_send);
        if (n > 0) {
            item->sent_offset += (size_t)n;
            if (item->sent_offset >= item->total_len) {
                /* Finished sending this message */
                session->tx_head = item->next;
                if (!session->tx_head) {
                    session->tx_tail = NULL;
                }
                session->tx_queue_count--;
                session->tx_queue_bytes -= item->total_len;
                session->msgs_sent++;

                free(item->data);
                free(item);
            } else {
                /* Partial write, socket write buffer is now saturated */
                return false;
            }
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* Kernel buffer full, wait for EPOLLOUT */
                return false;
            } else if (errno == EINTR) {
                continue;
            } else {
                /* Socket error or peer disconnected */
                return false;
            }
        } else {
            /* write returned 0 */
            return false;
        }
    }

    return true;
}
