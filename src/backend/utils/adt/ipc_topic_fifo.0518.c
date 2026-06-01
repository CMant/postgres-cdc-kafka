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
static rd_kafka_t        *g_prod = NULL;
static rd_kafka_topic_t  *g_rkt  = NULL;
static char              g_topic[128] = {0};
// static char req_path[1024];
// static char ack_path[1024];



struct ThreadArgs {
    const char *topic;
};

typedef struct {
    char req_path[256];
    char ack_path[256]; 
    int fd_req;           // 缓存的请求管道文件描述符
    int fd_ack;           // 缓存的应答管道文件描述符
    bool is_initialized;  // 是否已初始化
} fifo_cache_t;

static fifo_cache_t g_fifo_cache = {0};


static inline void make_topic_fifo_path2(const char *topic,char * req_path,char *ack_path)
{
    snprintf(req_path, 256, "%s%s_req.fifo", BASE_FIFO_PATH, topic);
    snprintf(ack_path, 256, "%s%s_ack.fifo", BASE_FIFO_PATH, topic);
}
// 创建当前 topic 的一对 fifo
static inline void create_topic_fifo_safe(const char *topic)
{
   // char req[256], ack[256];
    make_topic_fifo_path2(topic,g_fifo_cache.req_path,g_fifo_cache.ack_path);
    mkfifo(g_fifo_cache.req_path, 0666);
    mkfifo(g_fifo_cache.ack_path, 0666);
}

// 初始化管道缓存
static int init_fifo_cache(const char *topic)
{
    if (g_fifo_cache.is_initialized) {
        // 检查主题是否相同
        const char* path_topic_start = g_fifo_cache.req_path + strlen(BASE_FIFO_PATH);
        size_t topic_len = strlen(topic);
        if (strncmp(path_topic_start, topic, topic_len) == 0 && 
            strncmp(path_topic_start + topic_len, "_req.fifo", 9) == 0) {
            return 0; // 主题相同，无需重新初始化
        } else {
            // 主题不同，关闭旧的文件描述符
            if (g_fifo_cache.fd_req >= 0) {
                close(g_fifo_cache.fd_req);
                g_fifo_cache.fd_req = -1;
            }
            if (g_fifo_cache.fd_ack >= 0) {
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
    if (g_fifo_cache.fd_req < 0) {
        return -1;
    }
    
    // 打开应答管道（非阻塞模式）
    g_fifo_cache.fd_ack = open(g_fifo_cache.ack_path, O_RDONLY | O_NONBLOCK);
    if (g_fifo_cache.fd_ack < 0) {
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
    if (!conf) {
        elog(LOG, "[%s] rd_kafka_conf_new 失败", topic);
        return -1;
    }

    rd_kafka_conf_set(conf, "bootstrap.servers", "192.168.227.132:9092", NULL, 0);
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
void child_process_loop(const char *topic)
{
    // 初始化 Kafka
    if (kafka_prod_init(topic) < 0) {
        _exit(1);
    }
 
    // 创建当前topic的FIFO
    create_topic_fifo_safe(topic);

    // char req_path[256], ack_path[256];
    make_topic_fifo_path2(topic,g_fifo_cache.req_path,g_fifo_cache.ack_path);

    // 打开读端FIFO
    g_fifo_cache.fd_req = open(g_fifo_cache.req_path, O_RDONLY);
    if (g_fifo_cache.fd_req  < 0) {
        elog(LOG, "[%s] open req fifo 失败", topic);
        kafka_destroy();
        _exit(1);
    }

    elog(LOG, "[%s] 子进程 %d 启动，等待FIFO消息...", topic, getpid());

    char buf[MSG_MAX_LEN];
    while (1)
    {
        memset(buf, 0, sizeof(buf));
        ssize_t rlen = read(g_fifo_cache.fd_req , buf, sizeof(buf)-1);

        if (rlen <= 0) {
            usleep(10000);
            continue;
        }

        // 发送到 Kafka
        int ok = kafka_sync_send(buf);

        // 发送回执
        g_fifo_cache.fd_ack = open(g_fifo_cache.ack_path, O_WRONLY);
        if (g_fifo_cache.fd_ack >= 0) {
            char ack[8];
            snprintf(ack, sizeof(ack), "%d", ok == 0 ? 1 : 0);
            write(g_fifo_cache.fd_ack, ack, strlen(ack));
            close(g_fifo_cache.fd_ack);
        }
    }

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




int topic_kafka_send(const char *topic, const char *payload)
{
    // 初始化或验证管道缓存
     elog(LOG,"写入完毕xxx\n ");
    if (!g_fifo_cache.is_initialized) {
        if (init_fifo_cache(topic) != 0) {
            return -1;
        }
    }

    // 写入请求管道
    ssize_t written = write(g_fifo_cache.fd_req, payload, strlen(payload));
    if (written < 0) {
        int saved_errno = errno;
        if (saved_errno == EPIPE || saved_errno == ENXIO) {
            // 管道断开，尝试重新连接
            close(g_fifo_cache.fd_req);
            close(g_fifo_cache.fd_ack);
            if (init_fifo_cache(topic) != 0) {
                return -1;
            }
            written = write(g_fifo_cache.fd_req, payload, strlen(payload));
            if (written < 0) {
                return -1;
            }
            
        } else {
            return -1;
        }

       
    }

    // 读取应答管道
    char ack[8] = {0};
    ssize_t read_bytes = read(g_fifo_cache.fd_ack, ack, sizeof(ack)-1);
    if (read_bytes < 0) {
        return -1;  // 读取失败
    }
    
    ack[read_bytes] = '\0';
    return atoi(ack) == 1 ? 0 : -1;
}
