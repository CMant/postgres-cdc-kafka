#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>
#include <pthread.h>

// PostgreSQL 必需头文件（你在 PG 源码中编译）
#include "postgres.h"
#include "utils/elog.h"

#include "ipc_topic_fifo.h"
#include <librdkafka/rdkafka.h>

/* 每个进程独立使用的句柄（fork 后地址空间隔离，互不冲突） */
static rd_kafka_t *g_prod = NULL;
static rd_kafka_topic_t *g_rkt = NULL;
static char g_topic[128] = {0};
// static char req_path[1024];
// static char ack_path[1024];

struct ThreadArgs
{
    const char *topic;
};

typedef struct
{
    char req_path[256];
    char ack_path[256];
    int fd_req;          // 缓存的请求管道文件描述符
    int fd_ack;          // 缓存的应答管道文件描述符
    bool is_initialized; // 是否已初始化
} fifo_cache_t;

static fifo_cache_t g_fifo_cache = {0};

static volatile int g_child_exit_flag = 0;

static volatile int g_in_critical = 0;
static void child_sig_handler(int sig)
{
    // 🔥 如果正在发送消息，不处理退出，只标记
    if (g_in_critical)
    {
        elog(LOG, "子进程信号 %d 等待消息发送完成再退出", sig);
        g_child_exit_flag = 1;
        return;
    }

    elog(LOG, "子进程收到信号 %d，准备退出", sig);
    g_child_exit_flag = 1;
}

// static void child_sig_handler(int sig)
// {
//     elog(LOG, "子进程收到信号 %d，准备退出", sig);
//     g_child_exit_flag = 1;
// }

// 检测父进程是否还活着
static int is_parent_alive(void)
{
    // 如果 PPID 变成 1（init/systemd），说明父进程已死
    if (getppid() == 1)
        return 0;

    return 1;
}

static inline void make_topic_fifo_path2(const char *topic, char *req_path, char *ack_path)
{
    snprintf(req_path, 256, "%s%s_req.fifo", BASE_FIFO_PATH, topic);
    snprintf(ack_path, 256, "%s%s_ack.fifo", BASE_FIFO_PATH, topic);
}
// 创建当前 topic 的一对 fifo
static inline void create_topic_fifo_safe(const char *topic)
{
    // char req[256], ack[256];
    make_topic_fifo_path2(topic, g_fifo_cache.req_path, g_fifo_cache.ack_path);
    mkfifo(g_fifo_cache.req_path, 0666);
    mkfifo(g_fifo_cache.ack_path, 0666);
}

// 初始化管道缓存
static int init_fifo_cache(const char *topic)
{
    if (g_fifo_cache.is_initialized)
    {
        // 检查主题是否相同
        const char *path_topic_start = g_fifo_cache.req_path + strlen(BASE_FIFO_PATH);
        size_t topic_len = strlen(topic);
        if (strncmp(path_topic_start, topic, topic_len) == 0 &&
            strncmp(path_topic_start + topic_len, "_req.fifo", 9) == 0)
        {
            return 0; // 主题相同，无需重新初始化
        }
        else
        {
            // 主题不同，关闭旧的文件描述符
            if (g_fifo_cache.fd_req >= 0)
            {
                close(g_fifo_cache.fd_req);
                g_fifo_cache.fd_req = -1;
            }
            if (g_fifo_cache.fd_ack >= 0)
            {
                close(g_fifo_cache.fd_ack);
                g_fifo_cache.fd_ack = -1;
            }
        }
    }

    // 生成新路径
    make_topic_fifo_path2(topic, g_fifo_cache.req_path, g_fifo_cache.ack_path);

    // 创建管道（如果不存在）
    create_topic_fifo_safe(topic);
    // create_topic_fifo(topic);
    // 打开请求管道（非阻塞模式，避免等待）
    g_fifo_cache.fd_req = open(g_fifo_cache.req_path, O_WRONLY | O_NONBLOCK);
    if (g_fifo_cache.fd_req < 0)
    {
        return -1;
    }

    // 打开应答管道（非阻塞模式）
    g_fifo_cache.fd_ack = open(g_fifo_cache.ack_path, O_RDONLY | O_NONBLOCK);
    if (g_fifo_cache.fd_ack < 0)
    {
        close(g_fifo_cache.fd_req);
        g_fifo_cache.fd_req = -1;
        return -1;
    }

    g_fifo_cache.is_initialized = true;
    return 0;
}

// static inline void create_topic_fifo(const char *topic)
// {
//     char req[256], ack[256];
//     make_topic_fifo_path(topic);
//     mkfifo(req, 0666);
//     mkfifo(ack, 0666);
// }

// 根据 topic 生成两个管道路径
// static inline void make_topic_fifo_path(const char *topic)
// {
//     snprintf(req_path, 256, "%s%s_req.fifo", BASE_FIFO_PATH, topic);
//     snprintf(ack_path, 256, "%s%s_ack.fifo", BASE_FIFO_PATH, topic);
// }

/* Kafka 初始化（进程独立） */
int kafka_prod_init(const char *topic)
{
    rd_kafka_conf_t *conf;
    char errstr[512] = {0};

    conf = rd_kafka_conf_new();
    if (!conf)
    {
        elog(LOG, "[%s] rd_kafka_conf_new 失败", topic);
        return -1;
    }

    rd_kafka_conf_set(conf, "bootstrap.servers", "192.168.227.132:9092", NULL, 0);
    rd_kafka_conf_set(conf, "acks", "1", NULL, 0);

    g_prod = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!g_prod)
    {
        elog(LOG, "[%s] 生产者创建失败: %s", topic, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    g_rkt = rd_kafka_topic_new(g_prod, topic, NULL);
    if (!g_rkt)
    {
        elog(LOG, "[%s] 主题创建失败: %s", topic, rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(g_prod);
        g_prod = NULL;
        return -1;
    }

    strncpy(g_topic, topic, sizeof(g_topic) - 1);
    elog(LOG, "[%s] Kafka 初始化成功 ✅", topic);
    return 0;
}

/* 同步发送消息 */
int kafka_sync_send(const char *payload)
{
    if (!g_prod || !g_rkt || !payload)
    {
        elog(LOG, "[%s] 未初始化，发送失败", g_topic);
        return -1;
    }

    int ret = rd_kafka_produce(
        g_rkt,
        RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_COPY,
        (void *)payload,
        strlen(payload),
        NULL, 0, NULL);

    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        elog(LOG, "[%s] 消息发送失败: %s", g_topic, rd_kafka_err2str(rd_kafka_last_error()));
        return -1;
    }

    rd_kafka_flush(g_prod, 3000);
    return 0;
}

/* 销毁资源 */
void kafka_destroy(void)
{
    if (g_rkt)
    {
        rd_kafka_topic_destroy(g_rkt);
        g_rkt = NULL;
    }
    if (g_prod)
    {
        rd_kafka_destroy(g_prod);
        g_prod = NULL;
    }
}

// ===================== 子进程真正的后台循环（内部使用） =====================
void child_process_loop(const char *topic)
{
    int ret;
    ssize_t rlen;
    fd_set fds;
    struct timeval tv;
    char buf[MSG_MAX_LEN];

    // === 注册退出信号 ===
    signal(SIGTERM, child_sig_handler);
    signal(SIGINT, child_sig_handler);
    signal(SIGHUP, child_sig_handler);
    signal(SIGQUIT, child_sig_handler);

    // === 初始化 Kafka ===
    if (kafka_prod_init(topic) < 0)
    {
        _exit(1);
    }

    // === 创建并打开 FIFO ===
    create_topic_fifo_safe(topic);
    make_topic_fifo_path2(topic, g_fifo_cache.req_path, g_fifo_cache.ack_path);

    g_fifo_cache.fd_req = open(g_fifo_cache.req_path, O_RDONLY | O_NONBLOCK);
    if (g_fifo_cache.fd_req < 0)
    {
        elog(LOG, "[%s] open req fifo 失败", topic);
        kafka_destroy();
        _exit(1);
    }

    elog(LOG, "[%s] 子进程 %d 启动，父进程 %d", topic, getpid(), getppid());

    // === 主循环 ===
    // === 主循环 ===
    while (!g_child_exit_flag)
    {
        if (!is_parent_alive())
        {
            elog(LOG, "[%s] 父进程已退出，子进程自动退出", topic);
            break;
        }

        FD_ZERO(&fds);
        FD_SET(g_fifo_cache.fd_req, &fds);
        tv.tv_sec = 0;
        tv.tv_usec = 500000;

        ret = select(g_fifo_cache.fd_req + 1, &fds, NULL, NULL, &tv);
        if (ret < 0)
        {
            if (errno == EINTR)
                continue;
            // perror("select"); // Log error if needed
            break; // Or handle error appropriately
        }
        if (ret == 0)
        {
            continue; // Timeout, go back to loop and check flags again
        }

        rlen = read(g_fifo_cache.fd_req, buf, sizeof(buf) - 1);
        if (rlen < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // No data available right now, this shouldn't happen with select indicating readability
                // unless there's a race, just continue
                continue;
            }
            // Handle other read errors (e.g., EINTR if signal handling was different)
            // perror("read"); // Log error if needed
            break; // Or handle error appropriately
        }
        if (rlen == 0)
        {
            // Write end of FIFO closed by parent (likely died).
            // This is an indication that parent is gone, even if is_parent_alive() hasn't updated yet.
            // Log and exit gracefully.
            elog(LOG, "[%s] FIFO read end closed (parent likely terminated), exiting.", topic);
            break; // Exit the loop and clean up
        }

        // Only process data if rlen > 0
        buf[rlen] = '\0'; // Ensure null termination

        // ==========================================
        // 🔥 临界区保护：发送期间不响应退出信号
        // ==========================================

        g_in_critical = 1;
        int ok = kafka_sync_send(buf);
        elog(LOG, "kafka_sync_send 结束 %s\n", buf);
        g_in_critical = 0;

        // 发送完再检查退出
        if (g_child_exit_flag || !is_parent_alive())
        {
            elog(LOG, "[%s] 消息发送完成或父进程退出，安全退出", topic);
            break;
        }

        // 回执
        g_fifo_cache.fd_ack = open(g_fifo_cache.ack_path, O_WRONLY | O_NONBLOCK);
        if (g_fifo_cache.fd_ack >= 0)
        {
            char ack[8];
            snprintf(ack, sizeof(ack), "%d", ok == 0 ? 1 : 0);
            write(g_fifo_cache.fd_ack, ack, strlen(ack));
            close(g_fifo_cache.fd_ack);
            g_fifo_cache.fd_ack = -1;
        }
    }

    elog(LOG, "[%s] 子进程 %d 关闭资源并退出", topic, getpid());
    close(g_fifo_cache.fd_req);
    kafka_destroy();
    _exit(0);
}

// ===================== 🔥 核心改造：调用一次 = fork 一个子进程 =====================
// 你的原有函数名不变 → 直接调用即可自动创建子进程
void child_process_work(const char *topic)
{
    pid_t pid;

    // 忽略子进程退出 → 系统自动回收，无僵尸进程（PostgreSQL 必需）
    signal(SIGCHLD, SIG_IGN);

    pid = fork();
    if (pid < 0)
    {
        elog(LOG, "fork 子进程失败 topic=%s", topic);
        return;
    }

    if (pid == 0)
    {
        // 子进程：后台死循环处理 FIFO
        child_process_loop(topic);
    }
    else
    {
        // 父进程（PostgreSQL）：直接返回，不阻塞！
        elog(LOG, "成功启动 topic=%s 子进程，PID=%d", topic, pid);
        return;
    }
}

int topic_kafka_send(const char *topic, const char *payload)
{
    // 初始化或验证管道缓存
    // elog(LOG,"写入完毕xxx\n ");
    if (!g_fifo_cache.is_initialized)
    {
        if (init_fifo_cache(topic) != 0)
        {
            return -1;
        }
    }

    elog(LOG, "【DEBUG】即将写入 FIFO 的完整消息: [%s] 长度=%zu",
         payload, strlen(payload));

    // 写入请求管道
    ssize_t written = write(g_fifo_cache.fd_req, payload, strlen(payload));
    if (written < 0)
    {
        int saved_errno = errno;
        if (saved_errno == EPIPE || saved_errno == ENXIO)
        {
            // 管道断开，尝试重新连接
            close(g_fifo_cache.fd_req);
            close(g_fifo_cache.fd_ack);
            if (init_fifo_cache(topic) != 0)
            {
                return -1;
            }
            written = write(g_fifo_cache.fd_req, payload, strlen(payload));
            if (written < 0)
            {
                return -1;
            }
        }
        else
        {
            return -1;
        }
    }
    elog(LOG, "【DEBUG】实际写入 FIFO 字节数: %zu", written);
    elog(LOG, "写入完毕，等待应答\n");

    // 读取应答管道
    char ack[8] = {0};
    ssize_t read_bytes = read(g_fifo_cache.fd_ack, ack, sizeof(ack) - 1);
    if (read_bytes < 0)
    {
        return -1; // 读取失败
    }

    ack[read_bytes] = '\0';

    elog(LOG, "应答完毕，等待应答\n");
    return atoi(ack) == 1 ? 0 : -1;
}
