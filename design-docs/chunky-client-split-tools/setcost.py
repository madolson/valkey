import sys, collections
sys.argv=[sys.argv[0]]+sys.argv[1:]
exec(open('classify.py').read().split("rows = [")[0])
rows=[l.rstrip('\n').split('\t') for l in open('all.tsv') if l.strip()]
SETS = {
 'read_state': 'querybuf qb_pos qb_applied reqtype multibulklen bulklen querybuf_peak nread net_input_bytes read_flags flag.pending_read flag.protocol_error io_read_state cur_tid'.split(),
 'write_state': 'io_last_reply_block io_last_bufpos io_last_written nwritten write_flags io_write_state clients_pending_write_node io_tracked_reply_len net_output_bytes flag.pending_write'.split(),
 'reply_bufs': 'buf buf_usable_size bufpos reply reply_bytes last_header buf_peak buf_peak_last_reset_time flag.buf_encoded'.split(),
 'conn': ['conn'],
 'handoff': 'argv argc argv_len argv_len_sum cmd_queue parsed_cmd net_input_bytes_curr_cmd'.split(),
}
for name, fl in SETS.items():
    c=collections.Counter(); files=collections.defaultdict(set)
    for fld,f,line,fn,acc in rows:
        if fld in fl:
            L=layer(f,fn); side='IO' if L in('RD','WR','IOT') else 'C'
            c[side]+=1; c[side+acc]+=1; files[side].add(f)
    print(name, dict(c), 'C files:', len(files['C']), sorted(files['C']))
