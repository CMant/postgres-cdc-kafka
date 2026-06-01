#ifndef IPC_TOPIC_FIFO_H
#define IPC_TOPIC_FIFO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#define BASE_FIFO_PATH "/tmp/kafka_topic_"
#define MSG_MAX_LEN 2048

// static inline void make_topic_fifo_path(const char *topic, char *req_path, char *ack_path);
static inline void make_topic_fifo_path(const char *topic);
// 创建当前 topic 的一对 fifo
// static inline void create_topic_fifo(const char *topic);

int kafka_prod_init(const char *topic);
/* 同步发送消息 */
int kafka_sync_send(const char *payload);
void child_process_loop(const char *topic);

/* 销毁资源 */
void kafka_destroy(void);

/* 子进程业务逻辑：绑定topic -> 初始化 -> 循环读取FIFO发送 */
void child_process_work(const char *topic);

int topic_kafka_send(const char *topic, const char *payload);
// int topic_kafka_send2(const char *topic, const char *payload);
#endif