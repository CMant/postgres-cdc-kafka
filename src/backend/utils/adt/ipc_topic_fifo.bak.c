#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>

// PostgreSQL 必需头文件（你在 PG 源码中编译）
#include "postgres.h"
#include "utils/elog.h"

#include "ipc_topic_fifo.h"
#include <librdkafka/rdkafka.h>

/* 每个进程独立使用的句柄（fork 后地址空间隔离，互不冲突） */
static rd_kafka_t        *g_prod = NULL;
static rd_kafka_topic_t  *g_rkt  = NULL;
static char              g_topic[128] = {0};

// 根据 topic 生成两个管道路径
static inline void make_topic_fifo_path(const char *topic, char *req_path, char *ack_path)
{
    snprintf(req_path, 256, "%s%s_req.fifo", BASE_FIFO_PATH, topic);
    snprintf(ack_path, 256, "%s%s_ack.fifo", BASE_FIFO_PATH, topic);
}

// 创建当前 topic 的一对 fifo
static inline void create_topic_fifo(const char *topic)
{
    char req[256], ack[256];
    make_topic_fifo_path(topic, req, ack);
    mkfifo(req, 0666);
    mkfifo(ack, 0666);
}

/* Kafka 初始化（进程独立） */
int kafka_prod_init(const char *topic)
{
    rd_kafka_conf_t *conf;
    char errstr[512] = {0};

    conf = rd_kafka_conf_new();
    if (!conf) {
        elog(LOG, "[%s] rd_kafka_conf_new 失败", topic);
        return -1;
    }

    rd_kafka_conf_set(conf, "bootstrap.servers", "192.168.227.135:9092", NULL, 0);
    rd_kafka_conf_set(conf, "acks", "1", NULL, 0);

    g_prod = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!g_prod) {
        elog(LOG, "[%s] 生产者创建失败: %s", topic, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    g_rkt = rd_kafka_topic_new(g_prod, topic, NULL);
    if (!g_rkt) {
        elog(LOG, "[%s] 主题创建失败: %s", topic, rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(g_prod);
        g_prod = NULL;
        return -1;
    }

    strncpy(g_topic, topic, sizeof(g_topic)-1);
    elog(LOG, "[%s] Kafka 初始化成功 ✅", topic);
    return 0;
}

/* 同步发送消息 */
int kafka_sync_send(const char *payload)
{
    if (!g_prod || !g_rkt || !payload) {
        elog(LOG, "[%s] 未初始化，发送失败", g_topic);
        return -1;
    }

    int ret = rd_kafka_produce(
        g_rkt,
        RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_COPY,
        (void *)payload,
        strlen(payload),
        NULL, 0, NULL
    );

    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR) {
        elog(LOG, "[%s] 消息发送失败: %s", g_topic, rd_kafka_err2str(rd_kafka_last_error()));
        return -1;
    }

    rd_kafka_flush(g_prod, 3000);
    return 0;
}

/* 销毁资源 */
void kafka_destroy(void)
{
    if (g_rkt) {
        rd_kafka_topic_destroy(g_rkt);
        g_rkt = NULL;
    }
    if (g_prod) {
        rd_kafka_destroy(g_prod);
        g_prod = NULL;
    }
}

// ===================== 子进程真正的后台循环（内部使用） =====================
static void child_process_loop(const char *topic)
{
    // 初始化 Kafka
    if (kafka_prod_init(topic) < 0) {
        _exit(1);
    }

    // 创建当前topic的FIFO
    create_topic_fifo(topic);

    char req_path[256], ack_path[256];
    make_topic_fifo_path(topic, req_path, ack_path);

    // 打开读端FIFO
    int fd_req = open(req_path, O_RDONLY);
    if (fd_req < 0) {
        elog(LOG, "[%s] open req fifo 失败", topic);
        kafka_destroy();
        _exit(1);
    }

    elog(LOG, "[%s] 子进程 %d 启动，等待FIFO消息...", topic, getpid());

    char buf[MSG_MAX_LEN];
    while (1)
    {
        memset(buf, 0, sizeof(buf));
        ssize_t rlen = read(fd_req, buf, sizeof(buf)-1);

        if (rlen <= 0) {
            usleep(10000);
            continue;
        }

        // 发送到 Kafka
        int ok = kafka_sync_send(buf);

        // 发送回执
        int fd_ack = open(ack_path, O_WRONLY);
        if (fd_ack >= 0) {
            char ack[8];
            snprintf(ack, sizeof(ack), "%d", ok == 0 ? 1 : 0);
            write(fd_ack, ack, strlen(ack));
            close(fd_ack);
        }
    }

    close(fd_req);
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
    if (pid < 0) {
        elog(LOG, "fork 子进程失败 topic=%s", topic);
        return;
    }

    if (pid == 0) {
        // 子进程：后台死循环处理 FIFO
        child_process_loop(topic);
    } else {
        // 父进程（PostgreSQL）：直接返回，不阻塞！
        elog(LOG, "成功启动 topic=%s 子进程，PID=%d", topic, pid);
        return;
    }
}

===================== 你的原有同步发送函数：完全不动 =====================
指定任意动态topic，同步发送，等完成才返回
int topic_kafka_send(const char *topic, const char *payload)
{
    char req_path[256], ack_path[256];
    make_topic_fifo_path(topic, req_path, ack_path);

    // 打开对应topic的请求管道发消息
    int fd_req = open(req_path, O_WRONLY);
    if (fd_req < 0)
    {
        perror("open req fifo fail");
        return -1;
    }
    write(fd_req, payload, strlen(payload));
    close(fd_req);

    // 阻塞等当前topic专属回执
    char ack[8] = {0};
    int fd_ack = open(ack_path, O_RDONLY);
    read(fd_ack, ack, sizeof(ack));
    close(fd_ack);

    return atoi(ack) == 1 ? 0 : -1;
}


