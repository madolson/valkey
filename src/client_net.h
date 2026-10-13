/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * client_net.h -- Networking and client I/O interface.
 *
 * This header declares the interface between the networking / client I/O layer
 * (implemented in networking.c, io_threads.c and the connection transports) and
 * the rest of the server. It owns:
 *   - RESP input read-flag and write-flag state bits,
 *   - client lifecycle entry points (create/free/link/unlink),
 *   - the reply-encoding (addReply*) API used by command execution,
 *   - the transport read/write/flush entry points and the I/O-thread offload
 *     entry points.
 *
 * NOTE: This is NOT a standalone header. It is included from the middle of
 * server.h after the core types (client, connection, connListener, robj, sds,
 * list, user, writePreparedClient, pause_purpose, mstime_t, ClientFlags) are
 * already defined. Do not include it directly; include server.h.
 *
 * Keeping these declarations here, rather than inline in server.h, is the first
 * step of modularizing the networking layer: server.h no longer textually
 * declares the client networking/I/O API, and a future (e.g. Rust) networking
 * implementation only has to satisfy this interface.
 */

#ifndef VALKEY_CLIENT_NET_H
#define VALKEY_CLIENT_NET_H

/* Read flags for various read errors and states */
#define READ_FLAGS_QB_LIMIT_REACHED (1 << 0)
#define READ_FLAGS_ERROR_BIG_INLINE_REQUEST (1 << 1)
#define READ_FLAGS_ERROR_BIG_MULTIBULK (1 << 2)
#define READ_FLAGS_ERROR_INVALID_MULTIBULK_LEN (1 << 3)
#define READ_FLAGS_ERROR_UNAUTHENTICATED_MULTIBULK_LEN (1 << 4)
#define READ_FLAGS_ERROR_UNAUTHENTICATED_BULK_LEN (1 << 5)
#define READ_FLAGS_ERROR_BIG_BULK_COUNT (1 << 6)
#define READ_FLAGS_ERROR_MBULK_UNEXPECTED_CHARACTER (1 << 7)
#define READ_FLAGS_ERROR_MBULK_INVALID_BULK_LEN (1 << 8)
#define READ_FLAGS_ERROR_UNEXPECTED_INLINE_FROM_REPLICATED_CLIENT (1 << 9)
#define READ_FLAGS_ERROR_UNBALANCED_QUOTES (1 << 10)
#define READ_FLAGS_INLINE_ZERO_QUERY_LEN (1 << 11)
#define READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN (1 << 12)
#define READ_FLAGS_PARSING_COMPLETED (1 << 13)
#define READ_FLAGS_REPLICATED (1 << 14)
#define READ_FLAGS_DONT_PARSE (1 << 15)
#define READ_FLAGS_AUTH_REQUIRED (1 << 16)
#define READ_FLAGS_COMMAND_NOT_FOUND (1 << 17)
#define READ_FLAGS_BAD_ARITY (1 << 18)
#define READ_FLAGS_NO_KEYS (1 << 19)
#define READ_FLAGS_CROSSSLOT (1 << 20)
#define READ_FLAGS_PREFETCHED (1 << 21)
#define READ_FLAGS_ERROR_INVALID_CRLF (1 << 22)
#define READ_FLAGS_ERROR_NUL_IN_INLINE_PROTOCOL (1 << 23)

/* Write flags for various write errors and states */
#define WRITE_FLAGS_WRITE_ERROR (1 << 0)
#define WRITE_FLAGS_IS_REPLICA (1 << 1)
/* Unlike a retryable socket write error, a compression error is fatal. The IO
 * thread reports it here for the main thread to disconnect the replica. */
#define WRITE_FLAGS_COMPRESSION_ERROR (1 << 2)

client *createClient(connection *conn);
int freeClient(client *c);
void freeClientAsync(client *c);
void freeClientOrCloseLater(client *c, int async);
void logInvalidUseAndFreeClientAsync(client *c, const char *fmt, ...);
void beforeNextClient(client *c);
void clearClientConnectionState(client *c);
void resetClient(client *c);
void resetClientIOState(client *c);
void discardCommandQueue(client *c);
void freeClientOriginalArgv(client *c);
void freeClientArgv(client *c);
void sendReplyToClient(connection *conn);
int isDeferredReplyEnabled(client *c);
void initDeferredReplyBuffer(client *c);
void *addReplyDeferredLen(client *c);
void setDeferredArrayLen(client *c, void *node, long length);
void setDeferredMapLen(client *c, void *node, long length);
void setDeferredSetLen(client *c, void *node, long length);
void setDeferredAttributeLen(client *c, void *node, long length);
void setDeferredPushLen(client *c, void *node, long length);
int processInputBuffer(client *c);
void acceptCommonHandler(connection *conn, struct ClientFlags flags, char *ip);
int validatePrioritySubnets(const char *subnets_str, const char **err);
int updatePrioritySubnets(const char *subnets_str);
void readQueryFromClient(connection *conn);
int prepareClientToWrite(client *c);
writePreparedClient *prepareClientForFutureWrites(client *c);
void addReplyNull(client *c);
void addReplyNullArray(client *c);
void addReplyBool(client *c, int b);
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext);
void addReplyProto(client *c, const char *s, size_t len);
void AddReplyFromClient(client *c, client *src);
void commitDeferredReplyBuffer(client *c, int skip_if_blocked);
void addReplyBulk(client *c, robj *obj);
void addReplyBulkCString(client *c, const char *s);
void addReplyBulkCBuffer(client *c, const void *p, size_t len);
void addWritePreparedReplyBulkCBuffer(writePreparedClient *c, const void *p, size_t len);
void addReplyBulkLongLong(client *c, long long ll);
void addWritePreparedReplyBulkLongLong(writePreparedClient *c, long long ll);
void addReply(client *c, robj *obj);
void addReplyStatusLength(client *c, const char *s, size_t len);
void addReplySds(client *c, sds s);
void addReplyBulkSds(client *c, sds s);
void addWritePreparedReplyBulkSds(writePreparedClient *c, sds s);
void setDeferredReplyBulkSds(client *c, void *node, sds s);
void addReplyErrorObject(client *c, robj *err);
void addReplyOrErrorObject(client *c, robj *reply);
void afterErrorReply(client *c, const char *s, size_t len, int flags);
void addReplyErrorFormatInternal(client *c, int flags, const char *fmt, va_list ap);
void addReplyErrorSdsEx(client *c, sds err, int flags);
void addReplyErrorSdsExSafe(client *c, sds err, int flags);
void addReplyErrorSds(client *c, sds err);
void addReplyErrorSdsSafe(client *c, sds err);
void addReplyError(client *c, const char *err);
void addReplyErrorArity(client *c);
void addReplyErrorExpireTime(client *c);
void addReplyStatus(client *c, const char *status);
void addReplyDouble(client *c, double d);
void addReplyBigNum(client *c, const char *num, size_t len);
void addReplyHumanLongDouble(client *c, long double d);
void addReplyLongLong(client *c, long long ll);
void addReplyArrayLen(client *c, long length);
void addWritePreparedReplyArrayLen(writePreparedClient *c, long length);
void addReplyMapLen(client *c, long length);
void addWritePreparedReplyMapLen(writePreparedClient *c, long length);
void addReplySetLen(client *c, long length);
void addReplyAttributeLen(client *c, long length);
void addReplyPushLen(client *c, long length);
void addReplyHelp(client *c, const char **help);
void addExtendedReplyHelp(client *c, const char **help, const char **extended_help);
void addReplySubcommandSyntaxError(client *c);
void addReplyLoadedModules(client *c);
void copyReplicaOutputBuffer(client *dst, client *src);
void addListRangeReply(client *c, robj *o, long start, long end, int reverse);
void deferredAfterErrorReply(client *c, list *errors);
size_t getStringObjectSdsUsedMemory(robj *o);
size_t getStringObjectMemory(robj *o);
void freeClientReplyValue(void *o);
void *dupClientReplyValue(void *o);
char *getClientPeerId(client *c);
char *getClientSockname(client *c);
int isClientConnIpV6(client *c);
sds catClientInfoString(sds s, client *client, int hide_user_data);
sds catClientInfoShortString(sds s, client *client, int hide_user_data);
sds getAllClientsInfoString(int type, int hide_user_data);
int clientSetName(client *c, robj *name, const char **err);
bool clientCommandArgShouldBeRedacted(client *c, int arg_index);
void rewriteClientCommandVector(client *c, int argc, ...);
void rewriteClientCommandArgument(client *c, int i, robj *newval);
void replaceClientCommandVector(client *c, int argc, robj **argv);
void redactClientCommandArgument(client *c, int argc);
size_t getClientOutputBufferMemoryUsage(client *c);
size_t getClientMemoryUsage(client *c, size_t *output_buffer_mem_usage);
int freeClientsInAsyncFreeQueue(void);
int closeClientOnOutputBufferLimitReached(client *c, int async);
int getClientType(client *c);
int getClientTypeByName(char *name);
char *getClientTypeName(int client_class);
void flushReplicasOutputBuffers(void);
void disconnectReplicas(void);
void evictClients(void);
int listenToPort(connListener *fds);
void pauseActions(pause_purpose purpose, mstime_t end, uint32_t actions);
void unpauseActions(pause_purpose purpose);
uint32_t isPausedActions(uint32_t action_bitmask);
uint32_t isPausedActionsWithUpdate(uint32_t action_bitmask);
char *getPausedReason(pause_purpose purpose);
uint32_t getPausedActionsWithPurpose(pause_purpose purpose);
mstime_t getPausedActionTimeout(uint32_t action, pause_purpose *purpose);
void updatePausedActions(void);
void unblockPostponedClients(void);
void processEventsWhileBlocked(void);
void whileBlockedCron(void);
void blockingOperationStarts(void);
void blockingOperationEnds(void);
int handleClientsWithPendingWrites(void);
void adjustThreadedIOIfNeeded(void);
int clientHasPendingReplies(client *c);
int updateClientMemUsageAndBucket(client *c);
void removeClientFromMemUsageBucket(client *c, int allow_eviction);
void unlinkClient(client *c);
void removeFromServerClientList(client *c);
int writeToClient(client *c);
void linkClient(client *c);
void protectClient(client *c);
void unprotectClient(client *c);
void initSharedQueryBuf(void);
void freeSharedQueryBuf(void);
client *lookupClientByID(uint64_t id);
int authRequired(client *c);
void clientSetUser(client *c, user *u, int authenticated);
void putClientInPendingWriteQueue(client *c);
client *createCachedResponseClient(int resp);
void deleteCachedResponseClient(client *recording_client);
void waitForClientIO(client *c);
void ioThreadReadQueryFromClient(client *c);
void ioThreadWriteToClient(client *c);
int canParseCommand(client *c);
int processClientIOReadsDone(client *c);
void processClientIOWriteDone(client *c);
void releaseReplyReferences(client *c);
void resetLastWrittenBuf(client *c);
int clientConnPostponeMask(client *c);

int parseExtendedCommandArgumentsOrReply(client *c, int command_type, int start_idx, int max_args, int *flags, int *unit, int *expire_idx, robj **expire, robj **compare_val, robj **incrby_val);

/* logreqres.c - logging of requests and responses */
void reqresReset(client *c, int free_buf);
void reqresSaveClientReplyOffset(client *c);
size_t reqresAppendRequest(client *c);
size_t reqresAppendResponse(client *c);

#ifdef __GNUC__
void addReplyErrorFormatEx(client *c, int flags, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
void addReplyErrorFormat(client *c, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
void addReplyStatusFormat(client *c, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
#else
void addReplyErrorFormatEx(client *c, int flags, const char *fmt, ...);
void addReplyErrorFormat(client *c, const char *fmt, ...);
void addReplyStatusFormat(client *c, const char *fmt, ...);
#endif

#endif /* VALKEY_CLIENT_NET_H */
