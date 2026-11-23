#include "topic_manager.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

topic_manager_t *topic_manager_create(void) {
    topic_manager_t *mgr = (topic_manager_t *)calloc(1, sizeof(topic_manager_t));
    return mgr;
}

static void topic_free_subscribers(topic_t *t) {
    if (!t) return;
    sub_node_t *curr = t->subscribers_head;
    while (curr) {
        sub_node_t *next = curr->next;
        free(curr);
        curr = next;
    }
    t->subscribers_head = NULL;
    t->sub_count = 0;
}

void topic_manager_free(topic_manager_t *mgr) {
    if (!mgr) return;
    topic_t *curr = mgr->topics_head;
    while (curr) {
        topic_t *next = curr->next;
        topic_free_subscribers(curr);
        free(curr);
        curr = next;
    }
    free(mgr);
}

topic_t *topic_find(topic_manager_t *mgr, const char *topic_name) {
    if (!mgr || !topic_name) return NULL;
    topic_t *curr = mgr->topics_head;
    while (curr) {
        if (strcmp(curr->name, topic_name) == 0) {
            return curr;
        }
        curr = curr->next;
    }
    return NULL;
}

static topic_t *topic_get_or_create(topic_manager_t *mgr, const char *topic_name) {
    if (!mgr || !topic_name || strlen(topic_name) == 0) return NULL;

    topic_t *found = topic_find(mgr, topic_name);
    if (found) return found;

    topic_t *t = (topic_t *)calloc(1, sizeof(topic_t));
    if (!t) return NULL;

    strncpy(t->name, topic_name, sizeof(t->name) - 1);
    t->subscribers_head = NULL;
    t->sub_count = 0;
    t->msgs_published = 0;

    /* Insert at head of topics list */
    t->next = mgr->topics_head;
    mgr->topics_head = t;
    mgr->total_topics++;

    return t;
}

bool topic_subscribe(topic_manager_t *mgr, const char *topic_name, client_session_t *session) {
    if (!mgr || !topic_name || !session) return false;

    topic_t *t = topic_get_or_create(mgr, topic_name);
    if (!t) return false;

    /* Check if already subscribed */
    sub_node_t *curr = t->subscribers_head;
    while (curr) {
        if (curr->session == session) {
            return true; /* Already subscribed */
        }
        curr = curr->next;
    }

    sub_node_t *node = (sub_node_t *)malloc(sizeof(sub_node_t));
    if (!node) return false;

    node->session = session;
    node->next = t->subscribers_head;
    t->subscribers_head = node;
    t->sub_count++;

    return true;
}

bool topic_unsubscribe(topic_manager_t *mgr, const char *topic_name, client_session_t *session) {
    if (!mgr || !topic_name || !session) return false;

    topic_t *t = topic_find(mgr, topic_name);
    if (!t) return false;

    sub_node_t *curr = t->subscribers_head;
    sub_node_t *prev = NULL;

    while (curr) {
        if (curr->session == session) {
            if (prev) {
                prev->next = curr->next;
            } else {
                t->subscribers_head = curr->next;
            }
            free(curr);
            t->sub_count--;
            return true;
        }
        prev = curr;
        curr = curr->next;
    }

    return false;
}

void topic_unsubscribe_all(topic_manager_t *mgr, client_session_t *session) {
    if (!mgr || !session) return;

    topic_t *t = mgr->topics_head;
    while (t) {
        sub_node_t *curr = t->subscribers_head;
        sub_node_t *prev = NULL;

        while (curr) {
            if (curr->session == session) {
                sub_node_t *to_del = curr;
                if (prev) {
                    prev->next = curr->next;
                } else {
                    t->subscribers_head = curr->next;
                }
                curr = curr->next;
                free(to_del);
                t->sub_count--;
            } else {
                prev = curr;
                curr = curr->next;
            }
        }
        t = t->next;
    }
}

size_t topic_get_sub_count(topic_manager_t *mgr, const char *topic_name) {
    topic_t *t = topic_find(mgr, topic_name);
    return t ? t->sub_count : 0;
}

size_t topic_format_list(topic_manager_t *mgr, char *out_buf, size_t max_len) {
    if (!mgr || !out_buf || max_len == 0) return 0;

    out_buf[0] = '\0';
    size_t written = 0;
    topic_t *curr = mgr->topics_head;

    while (curr && written < max_len) {
        int n = snprintf(out_buf + written, max_len - written,
                         "Topic: %s | Subscribers: %zu | Total Published: %lu\n",
                         curr->name, curr->sub_count, (unsigned long)curr->msgs_published);
        if (n > 0) {
            written += (size_t)n;
        }
        curr = curr->next;
    }

    return written;
}
