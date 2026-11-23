/*
 * buffer.c - Byte stream buffer (dynamic, linear with compaction)
 *
 * IT4062 - Network Programming - Project 16: Publish/Subscribe Framework
 * Owner: Common module (shared by server + clients)
 *
 * Purpose
 * -------
 * TCP is a byte stream: a single read() may return half a frame, or three
 * frames glued together. Every connection therefore needs a growable
 * "ingress" buffer where bytes are accumulated until a complete frame can be
 * parsed. This module is that buffer.
 *
 * Layout (linear, not a ring)
 * ---------------------------
 *   data[0 .......... read_pos .......... write_pos .......... capacity)
 *         consumed     readable region      free tail space
 *
 *  - Readable bytes : write_pos - read_pos
 *  - Writable bytes : capacity - write_pos  (contiguous tail)
 *  - When the tail is too small but the total free space is enough, the
 *    readable region is memmove()-d back to offset 0 (buffer_compact).
 *  - Only when even that is not enough does the buffer grow (double).
 *
 * This keeps every frame contiguous in memory, which is exactly what
 * frame_header_to_host()/memcpy() based parsing needs, with no wraparound
 * special cases.
 */

#include "buffer.h"

#include <stdlib.h>
#include <string.h>

#define BUFFER_MIN_CAPACITY 256u

/* ------------------------------------------------------------------ */
/* Lifecycle                                                          */
/* ------------------------------------------------------------------ */

byte_buffer_t *buffer_create(size_t initial_capacity)
{
    if (initial_capacity < BUFFER_MIN_CAPACITY) {
        initial_capacity = BUFFER_MIN_CAPACITY;
    }

    byte_buffer_t *buf = (byte_buffer_t *)calloc(1, sizeof(*buf));
    if (buf == NULL) {
        return NULL;
    }

    buf->data = (uint8_t *)malloc(initial_capacity);
    if (buf->data == NULL) {
        free(buf);
        return NULL;
    }

    buf->capacity  = initial_capacity;
    buf->read_pos  = 0;
    buf->write_pos = 0;
    return buf;
}

void buffer_free(byte_buffer_t *buf)
{
    if (buf == NULL) {
        return;
    }
    free(buf->data);
    free(buf);
}

void buffer_reset(byte_buffer_t *buf)
{
    if (buf == NULL) {
        return;
    }
    buf->read_pos  = 0;
    buf->write_pos = 0;
}

/* ------------------------------------------------------------------ */
/* Accessors                                                          */
/* ------------------------------------------------------------------ */

size_t buffer_readable_bytes(const byte_buffer_t *buf)
{
    if (buf == NULL || buf->write_pos < buf->read_pos) {
        return 0;
    }
    return buf->write_pos - buf->read_pos;
}

size_t buffer_writable_bytes(const byte_buffer_t *buf)
{
    if (buf == NULL || buf->write_pos > buf->capacity) {
        return 0;
    }
    return buf->capacity - buf->write_pos;
}

const uint8_t *buffer_read_ptr(const byte_buffer_t *buf)
{
    if (buf == NULL || buf->data == NULL) {
        return NULL;
    }
    return buf->data + buf->read_pos;
}

uint8_t *buffer_write_ptr(byte_buffer_t *buf)
{
    if (buf == NULL || buf->data == NULL) {
        return NULL;
    }
    return buf->data + buf->write_pos;
}

/* ------------------------------------------------------------------ */
/* Mutators                                                           */
/* ------------------------------------------------------------------ */

/* Move the readable region back to offset 0, reclaiming the consumed head. */
void buffer_compact(byte_buffer_t *buf)
{
    if (buf == NULL) {
        return;
    }

    size_t readable = buffer_readable_bytes(buf);
    if (readable == 0) {
        buf->read_pos  = 0;
        buf->write_pos = 0;
        return;
    }

    if (buf->read_pos > 0) {
        memmove(buf->data, buf->data + buf->read_pos, readable);
        buf->read_pos  = 0;
        buf->write_pos = readable;
    }
}

/*
 * Guarantee that at least @len bytes are available contiguously at
 * buffer_write_ptr(). Compacts first, then grows (doubling) if needed.
 */
bool buffer_ensure_writable(byte_buffer_t *buf, size_t len)
{
    if (buf == NULL || buf->data == NULL) {
        return false;
    }
    if (len == 0) {
        return true;
    }

    if (buffer_writable_bytes(buf) >= len) {
        return true; /* already room in the tail */
    }

    /* Reclaim the consumed head: enough total space, just not contiguous. */
    buffer_compact(buf);
    if (buffer_writable_bytes(buf) >= len) {
        return true;
    }

    /* Still not enough -> grow geometrically to amortise realloc cost. */
    size_t required = buffer_readable_bytes(buf) + len;
    size_t new_cap  = buf->capacity;

    while (new_cap < required) {
        size_t doubled = new_cap * 2u;
        if (doubled < new_cap) { /* overflow guard */
            new_cap = required;
            break;
        }
        new_cap = doubled;
    }

    uint8_t *grown = (uint8_t *)realloc(buf->data, new_cap);
    if (grown == NULL) {
        return false;
    }

    buf->data     = grown;
    buf->capacity = new_cap;
    return true;
}

/* Convenience: ensure + memcpy + commit, used for building outgoing frames. */
bool buffer_append(byte_buffer_t *buf, const void *src, size_t len)
{
    if (buf == NULL) {
        return false;
    }
    if (len == 0) {
        return true;
    }
    if (src == NULL) {
        return false;
    }
    if (!buffer_ensure_writable(buf, len)) {
        return false;
    }

    memcpy(buf->data + buf->write_pos, src, len);
    buf->write_pos += len;
    return true;
}

/* Used after a raw read() straight into buffer_write_ptr(). */
void buffer_commit_write(byte_buffer_t *buf, size_t len)
{
    if (buf == NULL) {
        return;
    }

    size_t writable = buffer_writable_bytes(buf);
    if (len > writable) {
        len = writable;
    }
    buf->write_pos += len;
}

/* Drop @len already-parsed bytes from the head of the readable region. */
void buffer_consume(byte_buffer_t *buf, size_t len)
{
    if (buf == NULL) {
        return;
    }

    size_t readable = buffer_readable_bytes(buf);
    if (len >= readable) {
        buffer_reset(buf); /* everything consumed: cheap rewind */
        return;
    }

    buf->read_pos += len;
}
