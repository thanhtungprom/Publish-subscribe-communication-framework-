#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * ============================================================================
 * IT4062 - Network Programming - Project 16: Publish/Subscribe Framework
 * Protocol Definition and Packet Framing
 * ============================================================================
 */

#define PROTOCOL_MAGIC              0x5053    /* ASCII 'P', 'S' */
#define PROTOCOL_VERSION            0x01

#define MAX_TOPIC_LEN               256
#define MAX_PAYLOAD_LEN             (1024 * 1024)   /* 1MB maximum message payload */
#define MAX_CLIENT_ID_LEN           64
#define FRAME_HEADER_SIZE           12

/* Message Types */
typedef enum {
    MSG_CONNECT       = 0x01,   /* Client -> Server: Register client_id & role */
    MSG_CONNACK       = 0x02,   /* Server -> Client: Acknowledge connection */
    MSG_SUBSCRIBE     = 0x03,   /* Subscriber -> Server: Subscribe to topic */
    MSG_SUBACK        = 0x04,   /* Server -> Subscriber: Confirm subscribe */
    MSG_UNSUBSCRIBE   = 0x05,   /* Subscriber -> Server: Unsubscribe from topic */
    MSG_UNSUBACK      = 0x06,   /* Server -> Subscriber: Confirm unsubscribe */
    MSG_PUBLISH       = 0x07,   /* Publisher -> Server: Publish message to topic */
    MSG_DELIVER       = 0x08,   /* Server -> Subscriber: Deliver message on topic */
    MSG_LIST_TOPICS   = 0x09,   /* Client -> Server: Request active topic list */
    MSG_TOPIC_LIST    = 0x0A,   /* Server -> Client: Topic list response */
    MSG_PING          = 0x0B,   /* Heartbeat ping */
    MSG_PONG          = 0x0C,   /* Heartbeat pong */
    MSG_DISCONNECT    = 0x0D,   /* Graceful disconnect request */
    MSG_ERROR         = 0x0E    /* Protocol / Business error response */
} msg_type_t;

/* Status / Error Codes */
typedef enum {
    STATUS_OK                 = 200,
    STATUS_CREATED            = 201,
    STATUS_BAD_REQUEST        = 400,
    STATUS_INVALID_MAGIC      = 401,
    STATUS_INVALID_VERSION    = 402,
    STATUS_TOPIC_NOT_FOUND    = 404,
    STATUS_PAYLOAD_TOO_LARGE  = 413,
    STATUS_SLOW_CONSUMER      = 429,   /* Egress queue full / dropped */
    STATUS_SERVER_ERROR       = 500
} status_code_t;

/* Client Roles */
typedef enum {
    ROLE_NONE       = 0x00,
    ROLE_PUBLISHER  = 0x01,
    ROLE_SUBSCRIBER = 0x02,
    ROLE_HYBRID     = 0x03
} client_role_t;

/*
 * Fixed 12-byte Wire Header (aligned and packed explicitly)
 */
#pragma pack(push, 1)
typedef struct {
    uint16_t magic;         /* 0x5053 in network byte order */
    uint8_t  version;       /* 0x01 */
    uint8_t  msg_type;      /* msg_type_t */
    uint16_t topic_len;     /* Length of topic string (no trailing null) */
    uint16_t status_code;   /* status_code_t (200, 400, 429...) */
    uint32_t payload_len;   /* Length of payload body */
} frame_header_t;
#pragma pack(pop)

/* In-memory parsed message representation */
typedef struct {
    frame_header_t header;
    char           topic[MAX_TOPIC_LEN + 1];
    uint8_t       *payload;
    size_t         payload_size;
} pubsub_msg_t;

/* Serialization / Deserialization helpers */
void frame_header_init(frame_header_t *hdr, msg_type_t type, uint16_t topic_len, 
                       uint16_t status_code, uint32_t payload_len);
void frame_header_to_network(const frame_header_t *src, frame_header_t *dst);
void frame_header_to_host(const frame_header_t *src, frame_header_t *dst);
bool frame_header_validate(const frame_header_t *hdr);

const char *msg_type_to_str(msg_type_t type);
const char *status_code_to_str(status_code_t status);

#ifdef __cplusplus
}
#endif

#endif /* PROTOCOL_H */
