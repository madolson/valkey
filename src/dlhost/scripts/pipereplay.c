/*
 * pipereplay: closed-loop replay of a batch script against one server.
 *
 * The script is a list of batches separated by blank lines, one command per
 * line, arguments separated by spaces. Each connection runs the script over
 * and over ("one task" per pass): it sends a batch in one write, waits for
 * every reply, then sends the next batch. In arguments, __ID__ becomes a
 * per-task id, __W__ a per-connection id, and __Vn__ n bytes of payload.
 *
 * cc -O2 -o pipereplay pipereplay.c -lpthread
 * pipereplay -p PORT -c CONNS -t THREADS -d SECS -f SCRIPT
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define MAXB 256
#define MAXC 64
#define MAXA 64

typedef struct {
    int argc;
    char *argv[MAXA];
} rcmd;
typedef struct {
    int n;
    rcmd c[MAXC];
} rbatch;

static rbatch batches[MAXB];
static int nbatches;
static int port = 10100, conns = 50, threads = 4;
static double secs = 10;
static atomic_long tot_cmds, tot_batches, tot_errs, tot_tasks;
static volatile int stop;

typedef struct {
    int fd, w, bi;
    long task;
    char *out;
    size_t olen, ocap, opos;
    char *in;
    size_t ilen, icap;
    int want;
} conn;

static void app(conn *c, const char *p, size_t n) {
    if (c->olen + n > c->ocap) {
        c->ocap = (c->olen + n) * 2;
        c->out = realloc(c->out, c->ocap);
    }
    memcpy(c->out + c->olen, p, n);
    c->olen += n;
}

static void appArg(conn *c, const char *a) {
    char tmp[64], val[256];
    size_t vl = 0;
    const char *s = a;
    static char pay[65536];
    if (!pay[0]) memset(pay, 'x', sizeof(pay));
    /* expand placeholders */
    char *big = NULL;
    size_t bl = 0, bc = 0;
    while (*s) {
        const char *rep = NULL;
        size_t rl = 0;
        if (!strncmp(s, "__ID__", 6)) {
            vl = snprintf(val, sizeof(val), "%d:%ld", c->w, c->task);
            rep = val, rl = vl, s += 6;
        } else if (!strncmp(s, "__W__", 5)) {
            vl = snprintf(val, sizeof(val), "%d", c->w);
            rep = val, rl = vl, s += 5;
        } else if (!strncmp(s, "__V", 3)) {
            int n = atoi(s + 3);
            const char *e = strstr(s + 3, "__");
            rep = pay, rl = n, s = e + 2;
        } else {
            rep = s, rl = 1, s++;
        }
        if (bl + rl > bc) {
            bc = (bl + rl) * 2 + 64;
            big = realloc(big, bc);
        }
        memcpy(big + bl, rep, rl);
        bl += rl;
    }
    int l = snprintf(tmp, sizeof(tmp), "$%zu\r\n", bl);
    app(c, tmp, l);
    app(c, big, bl);
    app(c, "\r\n", 2);
    free(big);
}

static void buildBatch(conn *c) {
    rbatch *b = &batches[c->bi];
    c->olen = c->opos = 0;
    for (int i = 0; i < b->n; i++) {
        char tmp[32];
        int l = snprintf(tmp, sizeof(tmp), "*%d\r\n", b->c[i].argc);
        app(c, tmp, l);
        for (int j = 0; j < b->c[i].argc; j++) appArg(c, b->c[i].argv[j]);
    }
    c->want = b->n;
}

/* bytes of one complete reply, 0 if incomplete; *err set for top-level '-' */
static size_t skip(const char *p, size_t len, int *err) {
    const char *nl = memchr(p, '\n', len);
    if (!nl) return 0;
    size_t l = nl - p + 1;
    long long n;
    switch (p[0]) {
    case '-': if (err) *err = 1; return l;
    case '$':
        n = strtoll(p + 1, NULL, 10);
        if (n < 0) return l;
        return l + n + 2 <= len ? l + n + 2 : 0;
    case '*': case '%': case '~': {
        n = strtoll(p + 1, NULL, 10);
        if (n < 0) return l;
        if (p[0] == '%') n *= 2;
        size_t o = l;
        for (long long i = 0; i < n; i++) {
            size_t k = skip(p + o, len - o, NULL);
            if (!k) return 0;
            o += k;
        }
        return o;
    }
    default: return l;
    }
}

static void *worker(void *arg) {
    int tid = (int)(long)arg;
    int ep = epoll_create1(0);
    int mine = conns / threads + (tid < conns % threads);
    conn *cs = calloc(mine, sizeof(conn));
    for (int i = 0; i < mine; i++) {
        conn *c = &cs[i];
        c->w = tid * 100000 + i;
        c->fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons(port)};
        sa.sin_addr.s_addr = htonl(0x7f000001);
        if (connect(c->fd, (struct sockaddr *)&sa, sizeof(sa))) {
            perror("connect");
            exit(1);
        }
        int one = 1;
        setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        c->icap = 1 << 16;
        c->in = malloc(c->icap);
        struct epoll_event ev = {.events = EPOLLIN, .data.ptr = c};
        epoll_ctl(ep, EPOLL_CTL_ADD, c->fd, &ev);
        buildBatch(c);
        if (write(c->fd, c->out, c->olen) != (ssize_t)c->olen) exit(2);
    }
    long cmds = 0, nb = 0, errs = 0, tasks = 0;
    struct epoll_event evs[256];
    while (!stop) {
        int n = epoll_wait(ep, evs, 256, 100);
        for (int k = 0; k < n; k++) {
            conn *c = evs[k].data.ptr;
            if (c->ilen == c->icap) c->in = realloc(c->in, c->icap *= 2);
            ssize_t r = read(c->fd, c->in + c->ilen, c->icap - c->ilen);
            if (r <= 0) {
                if (r < 0 && errno == EAGAIN) continue;
                fprintf(stderr, "connection closed\n");
                exit(3);
            }
            c->ilen += r;
            size_t o = 0;
            for (;;) {
                int err = 0;
                size_t s = skip(c->in + o, c->ilen - o, &err);
                if (!s) break;
                o += s;
                errs += err;
                cmds++;
                if (--c->want == 0) {
                    nb++;
                    if (++c->bi == nbatches) c->bi = 0, c->task++, tasks++;
                    buildBatch(c);
                    if (write(c->fd, c->out, c->olen) != (ssize_t)c->olen) exit(2);
                }
            }
            memmove(c->in, c->in + o, c->ilen - o);
            c->ilen -= o;
        }
    }
    atomic_fetch_add(&tot_cmds, cmds);
    atomic_fetch_add(&tot_batches, nb);
    atomic_fetch_add(&tot_errs, errs);
    atomic_fetch_add(&tot_tasks, tasks);
    return NULL;
}

static void loadScript(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        perror(path);
        exit(1);
    }
    char line[4096];
    rbatch *b = NULL;
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\r\n")] = 0;
        if (line[0] == '#') continue;
        if (!line[0]) {
            b = NULL;
            continue;
        }
        if (!b) b = &batches[nbatches++];
        rcmd *c = &b->c[b->n++];
        for (char *t = strtok(line, " "); t; t = strtok(NULL, " ")) c->argv[c->argc++] = strdup(t);
    }
    fclose(f);
}

int main(int argc, char **argv) {
    const char *script = NULL;
    for (int i = 1; i < argc - 1; i++) {
        if (!strcmp(argv[i], "-p")) port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "-c")) conns = atoi(argv[++i]);
        else if (!strcmp(argv[i], "-t")) threads = atoi(argv[++i]);
        else if (!strcmp(argv[i], "-d")) secs = atof(argv[++i]);
        else if (!strcmp(argv[i], "-f")) script = argv[++i];
    }
    if (!script) return 1;
    loadScript(script);
    pthread_t th[256];
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < threads; i++) pthread_create(&th[i], NULL, worker, (void *)(long)i);
    struct timespec d = {(time_t)secs, (long)((secs - (time_t)secs) * 1e9)};
    nanosleep(&d, NULL);
    stop = 1;
    for (int i = 0; i < threads; i++) pthread_join(th[i], NULL);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double el = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    printf("script=%s conns=%d threads=%d secs=%.2f cmds=%ld batches=%ld tasks=%ld errors=%ld cmds_per_sec=%.0f "
           "batches_per_sec=%.0f\n",
           script, conns, threads, el, (long)tot_cmds, (long)tot_batches, (long)tot_tasks, (long)tot_errs,
           tot_cmds / el, tot_batches / el);
    return 0;
}
