import sys, collections
exec(open('classify.py').read().split("rows = [")[0])
rows=[l.rstrip('\n').split('\t') for l in open('all.tsv') if l.strip()]
fl=sys.argv[1].split(',')
d=collections.defaultdict(list)
for fld,f,line,fn,acc in rows:
    if fld in fl and layer(f,fn) not in ('RD','WR','IOT'):
        d[(f,fn)].append('%s%s:%s'%(fld,'' if acc=='R' else '['+acc+']',line))
for k in sorted(d, key=lambda k:-len(d[k])): print(k[0], k[1], len(d[k]), ' '.join(d[k][:8]))
