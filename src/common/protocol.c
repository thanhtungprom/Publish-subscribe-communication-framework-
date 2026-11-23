/*
 * protocol.c - Wire format: header init / byte-order conversion / validation
 *
 * IT4062 - Network Programming - Project 16: Publish/Subscribe Framework
 * Owner: Common module (shared by server + clients)
 *
 * Frame on the wire (12 bytes, #pragma pack(1)):
 *
 *   0               2       3        4          6          8                12
 *   +---------------+-------+--------+----------+----------+----------------+
 *   | magic (BE u16)| ver   | type   | topic_len| status   | payload_len    |
 *   | 0x50 0x53     | 0x01  | msg_t  | (BE u16) | (BE u16) | (BE u32)       |
 *   +---------------+-------+--------+----------+----------+----------------+
 *   | topic bytes (topic_len, no NUL) | payload bytes (payload_len)        |
 *
 * Every multi-byte field travels in NETWORK byte order (big endian). The
 * magic 0x5053 is the ASCII pair "PS" (Publish/Subscribe) and acts as a
 * cheap desynchronisation detector: a peer that sees a wrong magic knows it
 * has lost frame alignment and must drop the connection.
 *
 * Contract used by everyone else:
 *   build side : frame_header_init() -> frame_header_to_network() -> send
 *   parse side : recv -> frame_header_to_host() -> frame_header_validate()
 */

#include "protocol.h"

#include <arpa/inet.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Header construction (host order, ready to be converted)            */
/* ------------------------------------------------------------------ */

void frame_header_init(frame_header_t *hdr, msg_type_t type, uint16_t topic_len,
                       uint16_t status_code, uint32_t payload_len)
{
    if (hdr == NULL) {
        return;
    }

    hdr->magic       = PROTOCOL_MAGIC;
    hdr->version     = PROTOCOL_VERSION;
    hdr->msg_type    = (uint8_t)type;
    hdr->topic_len   = topic_len;
    hdr->status_code = status_code;
    hdr->payload_len = payload_len;
}

/* ------------------------------------------------------------------ */
/* Host <-> Network byte order                                        */
/* ------------------------------------------------------------------ */

void frame_header_to_network(const frame_header_t *src, frame_header_t *dst)
{
    if (src == NULL || dst == NULL) {
        return;
    }

    /* Copy 1-byte fields as-is, byte-swap the multi-byte ones. */
    dst->magic       = htons(src->magic);
    dst->version     = src->version;
    dst->msg_type    = src->msg_type;
    dst->topic_len   = htons(src->topic_len);
    dst->status_code = htons(src->status_code);
    dst->payload_len = htonl(src->payload_len);
}

void frame_header_to_host(const frame_header_t *src, frame_header_t *dst)
{
    if (src == NULL || dst == NULL) {
        return;
    }

    dst->magic       = ntohs(src->magic);
    dst->version     = src->version;
    dst->msg_type    = src->msg_type;
    dst->topic_len   = ntohs(src->topic_len);
    dst->status_code = ntohs(src->status_code);
    dst->payload_len = ntohl(src->payload_len);
}

/* ------------------------------------------------------------------ */
/* Validation - MUST be called on a HOST-order header                 */
/* ------------------------------------------------------------------ */

bool frame_header_validate(const frame_header_t *hdr)
{
    if (hdr == NULL) {
        return false;
    }

    if (hdr->magic != PROTOCOL_MAGIC) {
        return false; /* desynchronised / not our protocol */
    }
    if (hdr->version != PROTOCOL_VERSION) {
        return false; /* unsupported protocol revision */
    }
    if (hdr->msg_type < (uint8_t)MSG_CONNECT || hdr->msg_type > (uint8_t)MSG_ERROR) {
        return false; /* unknown message type */
    }
    if (hdr->topic_len > MAX_TOPIC_LEN) {
        return false; /* topic string would overflow the parser's buffer */
    }
    if (hdr->payload_len > MAX_PAYLOAD_LEN) {
        return false; /* oversized frame: reject before allocating */
    }

    return true;
}

/* ------------------------------------------------------------------ */
/* Logging helpers                                                    */
/* ------------------------------------------------------------------ */

const char *msg_type_to_str(msg_type_t type)
{
    switch (type) {
    case MSG_CONNECT:     return "CONNECT";
    case MSG_CONNACK:     return "CONNACK";
    case MSG_SUBSCRIBE:   return "SUBSCRIBE";
    case MSG_SUBACK:      return "SUBACK";
    case MSG_UNSUBSCRIBE: return "UNSUBSCRIBE";
    case MSG_UNSUBACK:    return "UNSUBACK";
    case MSG_PUBLISH:     return "PUBLISH";
    case MSG_DELIVER:     return "DELIVER";
    case MSG_LIST_TOPICS: return "LIST_TOPICS";
    case MSG_TOPIC_LIST:  return "TOPIC_LIST";
    case MSG_PING:        return "PING";
    case MSG_PONG:        return "PONG";
    case MSG_DISCONNECT:  return "DISCONNECT";
    case MSG_ERROR:       return "ERROR";
    default:              return "UNKNOWN";
    }
}

const char *status_code_to_str(status_code_t status)
{
    switch (status) {
    case STATUS_OK:                return "OK";
    case STATUS_CREATED:           return "CREATED";
    case STATUS_BAD_REQUEST:       return "BAD_REQUEST";
    case STATUS_INVALID_MAGIC:     return "INVALID_MAGIC";
    case STATUS_INVALID_VERSION:   return "INVALID_VERSION";
    case STATUS_TOPIC_NOT_FOUND:   return "TOPIC_NOT_FOUND";
    case STATUS_PAYLOAD_TOO_LARGE: return "PAYLOAD_TOO_LARGE";
    case STATUS_SLOW_CONSUMER:     return "SLOW_CONSUMER";
    case STATUS_SERVER_ERROR:      return "SERVER_ERROR";
    default:                       return "UNKNOWN_STATUS";
    }
}
