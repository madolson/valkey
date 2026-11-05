#!/usr/bin/env python3
"""Classify client field refs (all.tsv) by layer and print per-field ownership."""
import sys, collections, re

IO_READ = set('''handleQbLimitReached handleReadResult handleParseError isParsingError handleParseResults
resetClientIOState initSharedQueryBuf freeSharedQueryBuf isSharedQueryBufRegistered resetSharedQueryBuf
trimClientQueryBuffer parseInlineBuffer setProtocolError parseMultibulkBuffer parseMultibulk parseInputBuffer
readToQueryBuf readAndDecodePrimaryStream readQueryFromClient processClientIOReadsDone
ioThreadReadQueryFromClient postponeClientRead'''.split())
IO_WRITE = set('''advanceReplicaReplBufferCursor postWriteToReplica getReplicaWriteRange
compressReplicaDataToOutputBuffer writeToReplicaCompressed writeToReplica initReplyIOV addPlainBufferToReplyIOV
addBulkStringToReplyIOV addEncodedBufferToReplyIOV addBufferToReplyIOV saveLastWrittenBuf proceedToUnwritten
writevToClient _writeToClient resetLastWrittenBuf trackBufReferences releaseBufReferences releaseReplyReferences
_postWriteToClient postWriteToClient writeToClient sendReplyToClient processClientIOWriteDone
handleClientsWithPendingWrites ioThreadWriteToClient'''.split())
SEAM = set('''resetClient commandProcessed processCommandAndResetClient processPendingCommandAndInputBuffer
trimCommandQueue canParseCommand consumeCommandQueue discardCommandQueue addKeysToIncrFindBatch
prefetchCommandQueueKeys processInputBuffer beforeNextClient clientConnPostponeMask'''.split())

def layer(f, fn):
    if f.startswith('io_threads.c'):
        return 'IOT'
    if f == 'networking.c':
        if fn in IO_READ: return 'RD'
        if fn in IO_WRITE: return 'WR'
        if fn in SEAM: return 'SEAM'
        if fn and (re.match(r'^(_?add|set)(Reply|Bulk|WritePrepared|Deferred|ExtendedReply)', fn) or fn in (
            'upsertPayloadHeader', 'prepareClientToWrite', 'putClientInPendingWriteQueue', 'installClientWriteHandler',
            'clientReplyAllocSize', 'aggregateClientOutputBuffer', 'clientGetReplyList', 'clientGetReplyBytesPtr',
            'afterErrorReply', 'trimReplyUnusedTailSpace', 'prepareClientForFutureWrites', 'tryAvoidBulkStrCopyToReply',
            'isDeferredReplyEnabled', 'initDeferredReplyBuffer', 'resetDeferredReplyBuffer', 'commitDeferredReplyBuffer',
            'AddReplyFromClient', 'deferredAfterErrorReply', 'copyReplicaOutputBuffer', 'clientHasPendingReplies',
            'shouldDeferPushMessage', 'isCopyAvoidPreferred', 'cmdHasPushAsReply', '_addBulkStrRefToBuffer',
            '_addBulkStrRefToToList', '_addBulkStrRefToBufferOrList', '_addReplyToBufferOrList', '_addReplyProtoToList',
            'addReplyProto')):
            return 'REPLY'
    if f == 'memory_prefetch.c':
        return 'SEAM'
    return 'CMD'

rows = [l.rstrip('\n').split('\t') for l in open(sys.argv[1]) if l.strip()]
LAY = ['RD', 'WR', 'IOT', 'SEAM', 'REPLY', 'CMD']
cnt = collections.defaultdict(lambda: collections.Counter())
files = collections.defaultdict(set)
sites = collections.defaultdict(list)
for fld, f, line, fn, acc in rows:
    L = layer(f, fn)
    cnt[fld][(L, acc)] += 1
    cnt[fld][L] += 1
    files[fld].add(f)
    sites[(fld, L)].append('%s:%s' % (f, line))

def cls(c):
    io = c['RD'] + c['WR'] + c['IOT']
    other = c['SEAM'] + c['REPLY'] + c['CMD']
    if io and not other: return 'IO-only'
    if not io: return 'C-only'
    return 'shared'

mode = sys.argv[2] if len(sys.argv) > 2 else 'table'
if mode == 'table':
    print('field\tclass\ttotal\tfiles\t' + '\t'.join(LAY) + '\tIO_writes\tC_writes')
    for fld in sorted(cnt, key=lambda k: -sum(cnt[k][L] for L in LAY)):
        c = cnt[fld]
        iow = sum(c[(L, 'W')] + c[(L, 'A')] for L in ('RD', 'WR', 'IOT'))
        cw = sum(c[(L, 'W')] + c[(L, 'A')] for L in ('SEAM', 'REPLY', 'CMD'))
        print('%s\t%s\t%d\t%d\t%s\t%d\t%d' % (fld, cls(c), sum(c[L] for L in LAY), len(files[fld]),
              '\t'.join(str(c[L]) for L in LAY), iow, cw))
elif mode == 'sites':
    fld = sys.argv[3]
    for L in LAY:
        s = sites.get((fld, L))
        if s: print(L, len(s), ' '.join(sorted(set(s), key=lambda x: (x.split(':')[0], int(x.split(':')[1])))))
