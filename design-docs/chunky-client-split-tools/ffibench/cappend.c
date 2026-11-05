#include <stddef.h>
#include <string.h>
typedef struct cbuf { char *buf; size_t pos, cap; } cbuf;
/* Separate TU, no LTO: a real call, like addReply -> _addReplyPayloadToBuffer today. */
size_t c_append(cbuf *b, const char *p, size_t len) {
    size_t avail = b->cap - b->pos;
    size_t n = len < avail ? len : avail;
    memcpy(b->buf + b->pos, p, n);
    b->pos += n;
    return n;
}
