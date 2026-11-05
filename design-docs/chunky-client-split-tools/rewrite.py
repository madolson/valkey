import sys, collections, re
src = sys.argv[1]; prefix = sys.argv[3]
recs = collections.defaultdict(list)
for l in open(sys.argv[2]):
    fld, f, line, fn, acc, col = l.rstrip('\n').split('\t')
    recs[(f, int(line))].append((int(col), fld))
n = 0; manual = []
out = {}
for (f, line), rs in sorted(recs.items()):
    if f not in out: out[f] = open(src + '/' + f).read().split('\n')
    s = out[f][line - 1]
    direct = set(); macro = collections.Counter()
    for col, fld in rs:
        if s[col - 1:col - 1 + len(fld)] == fld and s[col - 3:col - 1] in ('->', '.') or s[col-2:col-1] == '.':
            direct.add((col, fld))
        else:
            macro[fld] += 1
    pos = set(direct)
    for fld, k in macro.items():
        ms = [m.start(2) + 1 for m in re.finditer(r'(->|\.)(%s)\b' % fld, s)]
        ms = [c for c in ms if (c, fld) not in direct]
        if not ms:
            manual.append((f, line, fld, s)); continue
        for c in ms: pos.add((c, fld))
    for col, fld in sorted(pos, reverse=True):
        s = s[:col - 1] + prefix + s[col - 1:]
        n += 1
    out[f][line - 1] = s
for f, L in out.items(): open(src + '/' + f, 'w').write('\n'.join(L))
print('rewrote', n, 'refs in', len(out), 'files')
for m in manual: print('MANUAL', *m)
