#include "kafka.h"
#include <stdio.h>
#include <stdlib.h>
#include "postgres.h"
//###这部分为copy使用的，因为需要跨文件
rd_kafka_t       *g_rk  = NULL;
rd_kafka_topic_t *g_rkt = NULL;









void kafka_init_copy(const char *brokers, const char *topic)
{
    char errstr[512];
    rd_kafka_conf_t *conf;

    // 防止重复初始化
    if (g_rk != NULL || g_rkt != NULL) {
        elog(ERROR, "Kafka 已经初始化");
        return;
    }

    // 创建配置
    conf = rd_kafka_conf_new();

    // 传入配置参数
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        elog(ERROR, "Kafka 配置失败: %s", errstr);
    }

    // 创建生产者
    g_rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!g_rk) {
        elog(ERROR, "Kafka 创建生产者失败: %s", errstr);
    }

    // 创建topic（传入topic名称）
    g_rkt = rd_kafka_topic_new(g_rk, topic, NULL);
    if (!g_rkt) {
        elog(ERROR, "Kafka 创建topic失败");
    }

    elog(LOG, "COPY 消息 Kafka 初始化成功：brokers=%s, topic=%s", brokers, topic);
}

// 关闭
void kafka_close_copy(void)
{
    if (g_rkt) {
        rd_kafka_topic_destroy(g_rkt);
        g_rkt = NULL;
    }

    if (g_rk) {
        rd_kafka_flush(g_rk, 3000);
        rd_kafka_destroy(g_rk);
        g_rk = NULL;
    }

    elog(LOG, "Kafka 已关闭");
}


void kafka_send_copy(const char *msg) {
    if (!g_rk || !g_rkt) {
        elog(ERROR,"未初始化\n");
        return;
    }

    if (rd_kafka_produce(g_rkt, RD_KAFKA_PARTITION_UA,
                         RD_KAFKA_MSG_F_COPY,
                         (void *)msg, strlen(msg),
                         NULL, 0, NULL) == -1) {
        elog(ERROR, "发送失败: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
    }  
}




void kafka_init_apply(rd_kafka_t  *apply_message_rk,rd_kafka_topic_t *apply_message_rkt,rd_kafka_conf_t *apply_message_conf,const char *brokers, const char *topic)
{
    char errstr[512];
  

    // 防止重复初始化
    if (apply_message_rk != NULL || apply_message_rkt != NULL) {
        elog(ERROR, "Kafka 已经初始化");
        return;
    }

    // 创建配置
    apply_message_conf = rd_kafka_conf_new();

    // 传入配置参数
    if (rd_kafka_conf_set(apply_message_conf, "bootstrap.servers", brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        elog(ERROR, "Kafka 配置失败: %s", errstr);
    }

    // 创建生产者
    apply_message_rk = rd_kafka_new(RD_KAFKA_PRODUCER, apply_message_conf, errstr, sizeof(errstr));
    if (!apply_message_rk) {
        elog(ERROR, "Kafka 创建生产者失败: %s", errstr);
    }

    // 创建topic（传入topic名称）
    apply_message_rkt = rd_kafka_topic_new(apply_message_rk, topic, NULL);
    if (!apply_message_rkt) {
        elog(ERROR, "Kafka 创建topic失败");
    }

    elog(LOG, "COPY 消息 Kafka 初始化成功：brokers=%s, topic=%s", brokers, topic);
}

// 关闭
void kafka_close_apply(rd_kafka_t  *apply_message_rk,rd_kafka_topic_t *apply_message_rkt)
{
    if (apply_message_rkt) {
        rd_kafka_topic_destroy(apply_message_rkt);
        apply_message_rkt = NULL;
    }

    if (apply_message_rk) {
        rd_kafka_flush(apply_message_rk, 3000);
        rd_kafka_destroy(apply_message_rk);
        apply_message_rk = NULL;
    }

    elog(LOG, "Kafka 已关闭");
}


void kafka_send_apply(rd_kafka_t  *apply_message_rk,rd_kafka_topic_t *apply_message_rkt,const char *msg) {
    if (!apply_message_rk || !apply_message_rkt) {
        elog(ERROR,"未初始化\n");
        return;
    }

    if (rd_kafka_produce(apply_message_rkt, RD_KAFKA_PARTITION_UA,
                         RD_KAFKA_MSG_F_COPY,
                         (void *)msg, strlen(msg),
                         NULL, 0, NULL) == -1) {
        elog(ERROR, "发送失败: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
    }  
}




void get_dbname_from_conninfo(const char *conninfo, char *out_dbname, int max_len)
{
	//获取数据库名字，对conninfo 进行转换提取
    const char *key = "dbname=";
    const char *start;
    const char *end;
	int len;
    // 初始化清空
    memset(out_dbname, 0, max_len);

    // 查找 dbname=
    start = strstr(conninfo, key);
    if (!start)
        return;

    // 跳过 key，指向值的开始
    start += strlen(key);

    // 找到值的结束位置（空格或结尾）
    end = strpbrk(start, " \t\n\r");
    if (!end)
        end = start + strlen(start);

    // 安全复制
    len = end - start;
    if (len >= max_len)
        len = max_len - 1;
    strncpy(out_dbname, start, len);
}



// 更简洁的版本
void kafka_init(rd_kafka_t **apply_g_rk, rd_kafka_topic_t **apply_g_rkt, const char *brokers, const char *topic) {
    if (!apply_g_rk || !apply_g_rkt || !brokers || !topic) {
        // PG后台必须用elog，printf无效！
        elog(LOG, "ERROR: Invalid parameters passed to kafka_init");
        return;
    }

    char errstr[512] = {0};
    rd_kafka_conf_t *conf;

    conf = rd_kafka_conf_new();
    if (!conf) {
        elog(LOG, "ERROR: Failed to create Kafka config");
        return;
    }

    // ===================== 关键：适配PostgreSQL后台进程 =====================
    // 禁用librdkafka内部线程，避免和PG信号/进程冲突
    rd_kafka_conf_set(conf, "internal.threads", "0", errstr, sizeof(errstr));
    // 禁用信号处理
    rd_kafka_conf_set(conf, "signal", "none", errstr, sizeof(errstr));

    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        elog(LOG, "ERROR: Config set fail: %s", errstr);
        rd_kafka_conf_destroy(conf);
        return;
    }

    // 创建Kafka实例
    *apply_g_rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!*apply_g_rk) {
        elog(LOG, "ERROR: New producer fail: %s", errstr);
        rd_kafka_conf_destroy(conf);
        // 初始化失败，直接报错退出，避免后续空指针
        ereport(ERROR, (errmsg("Kafka producer initialization failed")));
        return;
    }

    // 创建主题
    *apply_g_rkt = rd_kafka_topic_new(*apply_g_rk, topic, NULL);
    if (!*apply_g_rkt) {
        elog(LOG, "ERROR: Failed to create topic: %s", topic);
        rd_kafka_destroy(*apply_g_rk);
        *apply_g_rk = NULL;
        ereport(ERROR, (errmsg("Kafka topic creation failed")));
        return;
    }

    elog(LOG, "SUCCESS: Kafka 初始化成功 [broker:%s] [topic:%s]", brokers, topic);
}


void kafka_send(const char *msg, rd_kafka_t *rk, rd_kafka_topic_t *rkt) {
    // 空指针判断
    if (!rk || !rkt) {
        elog(LOG, "ERROR: Kafka objects not initialized");
        return;
    }

    elog(LOG, "Sending message to Kafka: rk=%p, rkt=%p", rk, rkt);

    // 发送消息
    rd_kafka_resp_err_t err = rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA,
                                               RD_KAFKA_MSG_F_COPY,
                                               (char *)msg, strlen(msg),
                                               NULL, 0, NULL);
    if (err) {
        elog(LOG, "Failed to produce message: %s", rd_kafka_err2str(err));
        return;
    }

    // PG后台进程专用：非阻塞poll
    rd_kafka_poll(rk, 0);
    // 强制刷新消息（关键）
    rd_kafka_flush(rk, 100);
}