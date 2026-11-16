#ifndef TOPIC_MANAGER_H
#define TOPIC_MANAGER_H

#include "client_session.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Node representing a subscriber in a topic */
typedef struct sub_node {
    client_session_t *session;
    struct sub_node  *next;
} sub_node_t;

/* Topic structure */
typedef struct topic {
    char          name[MAX_TOPIC_LEN];
    sub_node_t   *subscribers_head;
    size_t        sub_count;
    uint64_t      msgs_published;
    struct topic *next;
} topic_t;

/* Topic Manager structure */
typedef struct topic_manager {
    topic_t *topics_head;
    size_t   total_topics;
} topic_manager_t;

/* Manager lifecycle */
topic_manager_t *topic_manager_create(void);
void             topic_manager_free(topic_manager_t *mgr);

/* Subscription actions */
bool topic_subscribe(topic_manager_t *mgr, const char *topic_name, client_session_t *session);
bool topic_unsubscribe(topic_manager_t *mgr, const char *topic_name, client_session_t *session);
void topic_unsubscribe_all(topic_manager_t *mgr, client_session_t *session);

/* Topic queries */
topic_t *topic_find(topic_manager_t *mgr, const char *topic_name);
size_t   topic_get_sub_count(topic_manager_t *mgr, const char *topic_name);
size_t   topic_format_list(topic_manager_t *mgr, char *out_buf, size_t max_len);

#ifdef __cplusplus
}
#endif

#endif /* TOPIC_MANAGER_H */
