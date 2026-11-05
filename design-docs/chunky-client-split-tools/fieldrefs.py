#!/usr/bin/env python3
"""Exact member references to `struct client` (and ClientFlags bits) via libclang.

Output: TSV rows: field  file  line  func  access(R|W|A)
W = assignment LHS / compound assign / ++ --; A = address taken (&c->x); R otherwise.
"""
import sys, os
import clang.cindex as ci
ci.Config.set_library_file('/usr/lib64/libclang.so')
K = ci.CursorKind

SRC = sys.argv[1]
files = sys.argv[2:]
args = ['-std=gnu11', '-isystem', '/usr/lib64/clang/11.1.0/include', '-DLUA_ENABLED', '-DSTATIC_LUA=1', '-DUSE_JEMALLOC',
        '-I' + SRC, '-I' + SRC + '/../deps/libvalkey/include', '-I' + SRC + '/../deps/linenoise',
        '-I' + SRC + '/../deps/hdr_histogram', '-I' + SRC + '/../deps/fpconv',
        '-I' + SRC + '/../deps/fast_float', '-I' + SRC + '/../deps/lz4',
        '-I' + SRC + '/../deps/jemalloc/include', '-I' + SRC + '/../deps/lua/src']

ASSIGN_OPS = {'=', '+=', '-=', '*=', '/=', '|=', '&=', '^=', '<<=', '>>=', '%='}

def op_token(cur, lhs):
    # first token after the lhs extent
    end = lhs.extent.end.offset
    for t in cur.get_tokens():
        if t.extent.start.offset >= end:
            return t.spelling
    return None

def owner(cur):
    r = cur.referenced
    if r is None or r.kind != K.FIELD_DECL:
        return None
    p = r.semantic_parent
    while p is not None and p.kind in (K.UNION_DECL, K.STRUCT_DECL) and not p.spelling:
        p = p.semantic_parent  # anonymous union/struct inside client
    return p.spelling if p is not None else None

out = sys.stdout
idx = ci.Index.create()
for f in files:
    path = os.path.join(SRC, f)
    tu = idx.parse(path, args=args, options=0)
    errs = [d for d in tu.diagnostics if d.severity >= 3]
    if errs:
        print('#ERR', f, errs[0], file=sys.stderr)

    def walk(cur, func, parent, parent_role):
        kind = cur.kind
        if kind == K.FUNCTION_DECL and cur.is_definition():
            func = cur.spelling
        if kind == K.MEMBER_REF_EXPR:
            loc = cur.location
            if loc.file is not None and loc.file.name == path:
                own = owner(cur)
                if own in ('client', 'ClientFlags'):
                    name = cur.spelling if own == 'client' else 'flag.' + cur.spelling
                    out.write('%s\t%s\t%d\t%s\t%s\t%d\n' % (name, f, loc.line, func, parent_role, loc.column))
        children = list(cur.get_children())
        for i, ch in enumerate(children):
            role = 'R'
            if kind in (K.BINARY_OPERATOR, K.COMPOUND_ASSIGNMENT_OPERATOR) and i == 0 and len(children) == 2:
                op = op_token(cur, ch)
                if op in ASSIGN_OPS:
                    role = 'W'
            elif kind == K.UNARY_OPERATOR:
                toks = [t.spelling for t in cur.get_tokens()]
                if toks and (toks[0] in ('++', '--') or toks[-1] in ('++', '--')):
                    role = 'W'
                elif toks and toks[0] == '&':
                    role = 'A'
            elif kind in (K.PAREN_EXPR, K.UNEXPOSED_EXPR, K.CSTYLE_CAST_EXPR):
                role = parent_role if parent_role != 'R' else 'R'
            elif kind == K.MEMBER_REF_EXPR:
                # c->flag.x = 1: the inner c->flag inherits the write role
                role = parent_role
            walk(ch, func, cur, role)

    walk(tu.cursor, None, None, 'R')
