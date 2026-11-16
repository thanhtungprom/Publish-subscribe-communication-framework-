#ifndef BUFFER_H
#define BUFFER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t *data;
    size_t   capacity;
    size_t   read_pos;
    size_t   write_pos;
} byte_buffer_t;

/* Buffer lifecycle */
byte_buffer_t *buffer_create(size_t initial_capacity);
void           buffer_free(byte_buffer_t *buf);
void           buffer_reset(byte_buffer_t *buf);

/* Accessors */
size_t         buffer_readable_bytes(const byte_buffer_t *buf);
size_t         buffer_writable_bytes(const byte_buffer_t *buf);
const uint8_t *buffer_read_ptr(const byte_buffer_t *buf);
uint8_t       *buffer_write_ptr(byte_buffer_t *buf);

/* Mutators */
bool           buffer_ensure_writable(byte_buffer_t *buf, size_t len);
bool           buffer_append(byte_buffer_t *buf, const void *src, size_t len);
void           buffer_commit_write(byte_buffer_t *buf, size_t len);
void           buffer_consume(byte_buffer_t *buf, size_t len);
void           buffer_compact(byte_buffer_t *buf);

#ifdef __cplusplus
}
#endif

#endif /* BUFFER_H */
