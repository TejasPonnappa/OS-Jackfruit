/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Implementation: Tasks 1, 2, 3
 *   - UNIX domain socket control plane (supervisor + CLI client)
 *   - clone() with PID/UTS/mount namespaces per container
 *   - bounded-buffer logging pipeline (producer/consumer)
 *   - SIGCHLD / SIGINT / SIGTERM handling
 *   - container metadata tracking under mutex
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ------------------------------------------------------------------ */
/*  Constants                                                           */
/* ------------------------------------------------------------------ */
#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)

/* ------------------------------------------------------------------ */
/*  Types                                                               */
/* ------------------------------------------------------------------ */
typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* Global supervisor context pointer (needed by signal handlers) */
static supervisor_ctx_t *g_ctx = NULL;

/* ------------------------------------------------------------------ */
/*  Helpers                                                             */
/* ------------------------------------------------------------------ */
static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

static int parse_mib_flag(const char *flag, const char *value,
                           unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc,
                                 char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i+1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Bounded Buffer (Task 3)                                             */
/* ------------------------------------------------------------------ */
static int bounded_buffer_init(bounded_buffer_t *b)
{
    int rc;
    memset(b, 0, sizeof(*b));
    rc = pthread_mutex_init(&b->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&b->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&b->mutex); return rc; }
    rc = pthread_cond_init(&b->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&b->not_empty);
        pthread_mutex_destroy(&b->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *b)
{
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *b)
{
    pthread_mutex_lock(&b->mutex);
    b->shutting_down = 1;
    pthread_cond_broadcast(&b->not_empty);
    pthread_cond_broadcast(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
}

/*
 * Producer: push one log chunk into the buffer.
 * Blocks if full; returns -1 immediately if shutting down.
 */
int bounded_buffer_push(bounded_buffer_t *b, const log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);

    while (b->count == LOG_BUFFER_CAPACITY && !b->shutting_down)
        pthread_cond_wait(&b->not_full, &b->mutex);

    if (b->shutting_down) {
        pthread_mutex_unlock(&b->mutex);
        return -1;
    }

    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUFFER_CAPACITY;
    b->count++;

    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/*
 * Consumer: pop one log chunk from the buffer.
 * Returns  0  on success.
 * Returns  1  when shutting down AND buffer is empty (drain complete).
 */
int bounded_buffer_pop(bounded_buffer_t *b, log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);

    while (b->count == 0 && !b->shutting_down)
        pthread_cond_wait(&b->not_empty, &b->mutex);

    if (b->count == 0 && b->shutting_down) {
        pthread_mutex_unlock(&b->mutex);
        return 1; /* done */
    }

    *item = b->items[b->head];
    b->head = (b->head + 1) % LOG_BUFFER_CAPACITY;
    b->count--;

    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/*
 * Logging consumer thread.
 * Drains the bounded buffer and appends each chunk to the correct
 * per-container log file under LOG_DIR/<container_id>.log
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        int rc = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (rc != 0)
            break; /* shutdown + empty */

        /* Open (or create) the per-container log file, append mode */
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open log file");
            continue;
        }
        ssize_t written = 0;
        while (written < (ssize_t)item.length) {
            ssize_t n = write(fd, item.data + written, item.length - written);
            if (n < 0) break;
            written += n;
        }
        close(fd);
    }

    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Pipe-reader thread: reads container stdout/stderr, pushes to buffer */
/* ------------------------------------------------------------------ */
typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} pipe_reader_args_t;

static void *pipe_reader_thread(void *arg)
{
    pipe_reader_args_t *a = (pipe_reader_args_t *)arg;
    log_item_t item;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, a->container_id, CONTAINER_ID_LEN - 1);

    while (1) {
        ssize_t n = read(a->read_fd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0)
            break;
        item.length = (size_t)n;
        bounded_buffer_push(a->buffer, &item);
    }

    close(a->read_fd);
    free(a);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Container child entrypoint (Task 1)                                 */
/* ------------------------------------------------------------------ */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout and stderr to the log pipe */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    /* Apply nice value for scheduling experiments */
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    /* Set a distinct UTS hostname */
    sethostname(cfg->id, strlen(cfg->id));

    /* Mount /proc inside the container */
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        /* /proc may not exist in rootfs yet — create it */
        mkdir("/proc", 0555);
        mount("proc", "/proc", "proc", 0, NULL);
    }

    /* chroot into the container rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    /* Mount /proc again inside the chroot */
    mkdir("/proc", 0555);
    mount("proc", "/proc", "proc", 0, NULL);

    /* Execute the command */
    char *argv_exec[] = { cfg->command, NULL };
    char *envp[] = {
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME=/root",
        "TERM=xterm",
        NULL
    };
    execve(cfg->command, argv_exec, envp);

    /* execve only returns on failure */
    perror("execve");
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Monitor ioctl wrappers                                              */
/* ------------------------------------------------------------------ */
int register_with_monitor(int monitor_fd, const char *container_id,
                           pid_t host_pid, unsigned long soft_limit_bytes,
                           unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Container metadata helpers                                          */
/* ------------------------------------------------------------------ */
static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *c = ctx->containers;
    while (c) {
        if (strncmp(c->id, id, CONTAINER_ID_LEN) == 0)
            return c;
        c = c->next;
    }
    return NULL;
}

static container_record_t *alloc_container(const control_request_t *req)
{
    container_record_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    strncpy(c->id, req->container_id, CONTAINER_ID_LEN - 1);
    c->started_at = time(NULL);
    c->state = CONTAINER_STARTING;
    c->soft_limit_bytes = req->soft_limit_bytes;
    c->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(c->log_path, sizeof(c->log_path), "%s/%s.log", LOG_DIR, c->id);
    return c;
}

/* ------------------------------------------------------------------ */
/*  Launch a container (called from supervisor event loop)             */
/* ------------------------------------------------------------------ */
static int launch_container(supervisor_ctx_t *ctx, const control_request_t *req,
                             container_record_t *rec)
{
    /* Create a pipe: container writes to write_fd, supervisor reads from read_fd */
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        perror("pipe");
        return -1;
    }

    /* Allocate clone stack */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    /* Build child config */
    child_config_t *cfg = malloc(sizeof(*cfg));
    if (!cfg) {
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(cfg->command, req->command, CHILD_COMMAND_LEN - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    /* Clone with new PID, UTS, and mount namespaces */
    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t child_pid = clone(child_fn, stack + STACK_SIZE, flags, cfg);

    /* Parent closes write end of pipe */
    close(pipefd[1]);
    free(stack);
    /* cfg is owned by child after clone; child will exec and cfg memory
       stays valid until child exits since it is in child's memory space. */

    if (child_pid < 0) {
        perror("clone");
        close(pipefd[0]);
        free(cfg);
        return -1;
    }

    /* Start a pipe-reader thread to feed the bounded buffer */
    pipe_reader_args_t *pra = malloc(sizeof(*pra));
    if (pra) {
        pra->read_fd = pipefd[0];
        strncpy(pra->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        pra->buffer = &ctx->log_buffer;
        pthread_t tid;
        pthread_create(&tid, NULL, pipe_reader_thread, pra);
        pthread_detach(tid);
    } else {
        close(pipefd[0]);
    }

    /* Update metadata under lock */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->host_pid = child_pid;
    rec->state = CONTAINER_RUNNING;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, req->container_id, child_pid,
                               req->soft_limit_bytes, req->hard_limit_bytes);

    fprintf(stderr, "[supervisor] Started container '%s' pid=%d\n",
            req->container_id, child_pid);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  SIGCHLD handler — reaps children, updates metadata                 */
/* ------------------------------------------------------------------ */
static void sigchld_handler(int sig)
{
    (void)sig;
    if (!g_ctx) return;

    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(status)) {
                    c->state = CONTAINER_EXITED;
                    c->exit_code = WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    c->state = CONTAINER_KILLED;
                    c->exit_signal = WTERMSIG(status);
                }
                /* Unregister from kernel monitor (safe to call from signal
                   context since ioctl is async-signal-safe on Linux) */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd, c->id, pid);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

/* ------------------------------------------------------------------ */
/*  SIGINT / SIGTERM handler                                            */
/* ------------------------------------------------------------------ */
static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/* ------------------------------------------------------------------ */
/*  Supervisor: handle one client connection                           */
/* ------------------------------------------------------------------ */
static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    ssize_t n = recv(client_fd, &req, sizeof(req), 0);
    if (n != sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Malformed request");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {

    case CMD_START:
    case CMD_RUN: {
        pthread_mutex_lock(&ctx->metadata_lock);
        if (find_container(ctx, req.container_id)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Container '%s' already exists", req.container_id);
            break;
        }
        container_record_t *rec = alloc_container(&req);
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "Out of memory");
            break;
        }
        /* Prepend to list */
        rec->next = ctx->containers;
        ctx->containers = rec;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (launch_container(ctx, &req, rec) != 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Failed to launch container '%s'", req.container_id);
        } else {
            resp.status = 0;
            if (req.kind == CMD_START) {
                snprintf(resp.message, sizeof(resp.message),
                         "Started container '%s' pid=%d",
                         req.container_id, rec->host_pid);
                /* For CMD_START: send response immediately */
                send(client_fd, &resp, sizeof(resp), 0);
                return;
            } else {
                /* CMD_RUN: send initial ack, then wait for container to exit */
                snprintf(resp.message, sizeof(resp.message),
                         "Running container '%s' pid=%d",
                         req.container_id, rec->host_pid);
                send(client_fd, &resp, sizeof(resp), 0);

                /* Wait for the container to finish */
                while (1) {
                    pthread_mutex_lock(&ctx->metadata_lock);
                    container_state_t st = rec->state;
                    int exit_code = rec->exit_code;
                    int exit_sig  = rec->exit_signal;
                    pthread_mutex_unlock(&ctx->metadata_lock);
                    if (st == CONTAINER_EXITED || st == CONTAINER_KILLED ||
                        st == CONTAINER_STOPPED) {
                        memset(&resp, 0, sizeof(resp));
                        resp.status = (st == CONTAINER_EXITED) ? exit_code
                                                                : 128 + exit_sig;
                        snprintf(resp.message, sizeof(resp.message),
                                 "Container '%s' finished (status=%d)",
                                 req.container_id, resp.status);
                        send(client_fd, &resp, sizeof(resp), 0);
                        return;
                    }
                    usleep(100000); /* 100 ms poll */
                }
            }
        }
        break;
    }

    case CMD_PS: {
        char buf[4096] = {0};
        int off = 0;
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-12s %-10s %-12s %-12s\n",
                        "ID", "PID", "STATE", "EXIT",
                        "SOFT(MiB)", "HARD(MiB)");
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c && off < (int)sizeof(buf) - 80) {
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-8d %-12s %-10d %-12lu %-12lu\n",
                            c->id, c->host_pid,
                            state_to_string(c->state),
                            c->exit_code,
                            c->soft_limit_bytes >> 20,
                            c->hard_limit_bytes >> 20);
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        break;
    }

    case CMD_LOGS: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = find_container(ctx, req.container_id);
        char log_path[PATH_MAX] = {0};
        if (c)
            strncpy(log_path, c->log_path, PATH_MAX - 1);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!c) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "No container '%s'", req.container_id);
            break;
        }
        /* Send the log file contents back */
        FILE *f = fopen(log_path, "r");
        if (!f) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Log file not found: %s", log_path);
            break;
        }
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "=== logs for %s ===", req.container_id);
        send(client_fd, &resp, sizeof(resp), 0);

        /* Stream file content */
        char fbuf[1024];
        size_t nr;
        while ((nr = fread(fbuf, 1, sizeof(fbuf), f)) > 0)
            write(client_fd, fbuf, nr);
        fclose(f);
        return;
    }

    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = find_container(ctx, req.container_id);
        pid_t pid = c ? c->host_pid : -1;
        if (c) c->state = CONTAINER_STOPPED;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (pid < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "No container '%s'", req.container_id);
        } else {
            kill(pid, SIGTERM);
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Sent SIGTERM to container '%s' (pid=%d)",
                     req.container_id, pid);
        }
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Unknown command");
        break;
    }

    send(client_fd, &resp, sizeof(resp), 0);
}

/* ------------------------------------------------------------------ */
/*  Supervisor main (Task 1 + Task 2)                                  */
/* ------------------------------------------------------------------ */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    g_ctx = &ctx;

    /* Ensure log directory exists */
    mkdir(LOG_DIR, 0755);

    /* Init metadata lock */
    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }

    /* Init bounded buffer */
    if (bounded_buffer_init(&ctx.log_buffer) != 0) {
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* Open kernel monitor device (optional — continue if not present) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] Warning: cannot open /dev/container_monitor: %s\n",
                strerror(errno));

    /* Create UNIX domain socket */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        goto cleanup;
    }
    if (listen(ctx.server_fd, 16) != 0) {
        perror("listen");
        goto cleanup;
    }

    /* Signal handling */
    struct sigaction sa_chld, sa_term;
    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = sigchld_handler;
    sa_chld.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = sigterm_handler;
    sa_term.sa_flags   = SA_RESTART;
    sigaction(SIGINT,  &sa_term, NULL);
    sigaction(SIGTERM, &sa_term, NULL);

    /* Start logging thread */
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger");
        goto cleanup;
    }

    fprintf(stderr, "[supervisor] Ready. base-rootfs=%s socket=%s\n",
            rootfs, CONTROL_PATH);

    /* Event loop: accept CLI connections */
    while (!ctx.should_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };

        int sel = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }
        if (sel == 0) continue; /* timeout, check should_stop */

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        handle_client(&ctx, client_fd);
        close(client_fd);
    }

    fprintf(stderr, "[supervisor] Shutting down...\n");

    /* Send SIGTERM to all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING)
            kill(c->host_pid, SIGTERM);
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Brief wait for children */
    sleep(1);

cleanup:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    if (ctx.logger_thread)
        pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free container records */
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        free(c);
        c = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

    if (ctx.server_fd >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    pthread_mutex_destroy(&ctx.metadata_lock);
    fprintf(stderr, "[supervisor] Exited cleanly.\n");
    return 0;
}

/* ------------------------------------------------------------------ */
/*  CLI client: send a control request to the supervisor               */
/* ------------------------------------------------------------------ */
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s: %s\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    if (send(fd, req, sizeof(*req), 0) != sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    /* Read response(s) */
    control_response_t resp;
    ssize_t n = recv(fd, &resp, sizeof(resp), 0);
    if (n <= 0) {
        fprintf(stderr, "No response from supervisor\n");
        close(fd);
        return 1;
    }
    printf("%s\n", resp.message);

    /* For logs command, stream remaining bytes */
    if (req->kind == CMD_LOGS && resp.status == 0) {
        char buf[1024];
        ssize_t nr;
        while ((nr = recv(fd, buf, sizeof(buf), 0)) > 0)
            fwrite(buf, 1, nr, stdout);
    }

    /* For run command, wait for final status */
    if (req->kind == CMD_RUN && resp.status == 0) {
        n = recv(fd, &resp, sizeof(resp), 0);
        if (n > 0) {
            printf("%s\n", resp.message);
            close(fd);
            return resp.status;
        }
    }

    close(fd);
    return (resp.status == 0) ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/*  CLI command handlers                                                */
/* ------------------------------------------------------------------ */
static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr, "Usage: %s start <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr, "Usage: %s run <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ------------------------------------------------------------------ */
/*  main                                                                */
/* ------------------------------------------------------------------ */
int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}

