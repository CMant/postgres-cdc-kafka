#include "kafka.h"
#include <stdio.h>
#include <stdlib.h>
#include "postgres.h"
#include "utils/guc.h"

static int message_count = 0;
#define queue_buffering_max_messages "214748364"
static int max_message_count;
static long long local_memory_usage = 0;
static int max_memory_kbytes = 0; 
 
void kafka_close_copy(rd_kafka_t **apply_g_rk, rd_kafka_topic_t **apply_g_rkt,char * table_name)
{
    if (*apply_g_rkt)
    {
        rd_kafka_topic_destroy(*apply_g_rkt);
        *apply_g_rkt = NULL;
    }

    if (*apply_g_rk)
    {
        rd_kafka_flush(*apply_g_rk, 5000);
        rd_kafka_destroy(*apply_g_rk);
        *apply_g_rk = NULL;
    }

    message_count = 0;
    local_memory_usage = 0;
    ereport(LOG, (errhidecontext(true), errmsg("SUCCESS: Kafka [%s] COPY任务已结束",table_name)));
}



void get_dbname_from_conninfo(const char *conninfo, char *out_dbname, int max_len)
{
    // 获取数据库名字，对conninfo 进行转换提取
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

int kafka_init(rd_kafka_t **apply_g_rk, rd_kafka_topic_t **apply_g_rkt, const char *brokers, const char *topic)
{
    char errstr[512] = {0};
    rd_kafka_conf_t *conf;
     
     bool is_ssl_protocol;
    const char *ssl_cipher_suites;
    const char *ssl_key_location;
    const char *ssl_key_password;
   const char *ssl_cert_location;
   const char *ssl_ca_location;
   const char *ssl_endpoint_algo;
   const char *sasl_mechanism ;
   const char *sasl_username;
     const char *sasl_password;
     bool is_sasl_protocol;
     const char *security_protocol;
    bool needs_auth;
    if (!apply_g_rk || !apply_g_rkt || !brokers || !topic)
    {
        // PG后台必须用elog，printf无效！
        ereport(LOG, (errhidecontext(true), errmsg("ERROR: Invalid parameters passed to kafka_init")));
        return -1;
    }

    

    conf = rd_kafka_conf_new();
    if (!conf)
    {
        ereport(LOG, (errhidecontext(true), errmsg("ERROR: Failed to create Kafka config")));
        return -1;
    }
  
     max_message_count=atoi(GetConfigOption("kafka_queue_buffering_max_messages", true, false));
    //  if (max_message_count <= 0) max_message_count = 2147483647;
     max_memory_kbytes = atoi(GetConfigOption("kafka_queue_buffering_max_kbytes", true, false));


    // --- 性能相关参数 ---
    //单条消息最大字节数。这里写死了不限制。1000000000 就是最大值
     rd_kafka_conf_set(conf, "message.max.bytes", "1000000000", errstr, sizeof(errstr));
    // 消息队列缓冲区最大消息数
    rd_kafka_conf_set(conf, "queue.buffering.max.messages", GetConfigOption("kafka_queue_buffering_max_messages", true, false), errstr, sizeof(errstr));

    // 消息队列缓冲区最大内存字节数
    rd_kafka_conf_set(conf, "queue.buffering.max.kbytes", GetConfigOption("kafka_queue_buffering_max_kbytes", true, false), errstr, sizeof(errstr));

    // 批量发送的消息数量
    rd_kafka_conf_set(conf, "batch.num.messages", GetConfigOption("kafka_batch_num_messages", true, false), errstr, sizeof(errstr));

    // 消息发送延迟上限
    rd_kafka_conf_set(conf, "queue.enqueue.timeout.ms", GetConfigOption("kafka_queue_enqueue_timeout_ms", true, false), errstr, sizeof(errstr));

    // 消息超时时间
    rd_kafka_conf_set(conf, "message.timeout.ms", GetConfigOption("kafka_message_timeout_ms", true, false), errstr, sizeof(errstr));

    // 重试次数
    rd_kafka_conf_set(conf, "retries", GetConfigOption("kafka_retries", true, false), errstr, sizeof(errstr));

    // 重试退避时间
    rd_kafka_conf_set(conf, "retry.backoff.ms", GetConfigOption("kafka_retry_backoff_ms", true, false), errstr, sizeof(errstr));

    // Socket 发送缓冲区大小
    rd_kafka_conf_set(conf, "socket.send.buffer.bytes", GetConfigOption("kafka_socket_send_buffer_bytes", true, false), errstr, sizeof(errstr));

    // Socket 接收缓冲区大小
    rd_kafka_conf_set(conf, "socket.receive.buffer.bytes", GetConfigOption("kafka_socket_receive_buffer_bytes", true, false), errstr, sizeof(errstr));

    // Socket 连接超时
    rd_kafka_conf_set(conf, "socket.connection.setup.timeout.ms", GetConfigOption("kafka_socket_connection_setup_timeout_ms", true, false), errstr, sizeof(errstr));

    // 元数据缓存过期时间
    rd_kafka_conf_set(conf, "metadata.max.age.ms", GetConfigOption("kafka_metadata_max_age_ms", true, false), errstr, sizeof(errstr));

    // 消息压缩类型
    rd_kafka_conf_set(conf, "compression.codec", GetConfigOption("kafka_compression_codec", true, false), errstr, sizeof(errstr));

    // 消息压缩级别
    rd_kafka_conf_set(conf, "compression.level", GetConfigOption("kafka_compression_level", true, false), errstr, sizeof(errstr));

    /*
    When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order. 
    The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: 
    max.in.flight.requests.per.connection=5 (must be less than or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. 
    Producer instantation will fail if user-supplied configuration is incompatible.
     Type: boolean
     消息幂等，默认是false 这里写死，需要强制有序。
    
    */ 
    rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr));

    // --- 安全相关参数 ---

    // 设置安全相关配置时，根据协议类型有条件地设置
    security_protocol = GetConfigOption("kafka_security_protocol", true, false);

    // 总是设置安全协议
    if (security_protocol && strlen(security_protocol) > 0)
    {
        
        rd_kafka_conf_set(conf, "security.protocol", security_protocol, errstr, sizeof(errstr));
        rd_kafka_conf_set(conf, "enable.ssl.certificate.verification", GetConfigOption("kafka_enable_ssl_certificate_verification", true, false), errstr, sizeof(errstr));

    }

    // 只有当协议涉及 SSL 时才设置 SSL 相关参数
    is_ssl_protocol = (security_protocol &&
                            (strcmp(security_protocol, "SSL") == 0 ||
                             strcmp(security_protocol, "SASL_SSL") == 0));
   
    if (is_ssl_protocol)
    {
        ssl_cipher_suites = GetConfigOption("kafka_ssl_cipher_suites", true, false);
        if (ssl_cipher_suites && strlen(ssl_cipher_suites) > 0)
        {
            rd_kafka_conf_set(conf, "ssl.cipher.suites", ssl_cipher_suites, errstr, sizeof(errstr));
        }

        ssl_key_location = GetConfigOption("kafka_ssl_key_location", true, false);
        if (ssl_key_location && strlen(ssl_key_location) > 0)
        {
            rd_kafka_conf_set(conf, "ssl.key.location", ssl_key_location, errstr, sizeof(errstr));
        }

        ssl_key_password = GetConfigOption("kafka_ssl_key_password", true, false);
        if (ssl_key_password && strlen(ssl_key_password) > 0)
        {
            rd_kafka_conf_set(conf, "ssl.key.password", ssl_key_password, errstr, sizeof(errstr));
        }

        ssl_cert_location = GetConfigOption("kafka_ssl_certificate_location", true, false);
        if (ssl_cert_location && strlen(ssl_cert_location) > 0)
        {
            rd_kafka_conf_set(conf, "ssl.certificate.location", ssl_cert_location, errstr, sizeof(errstr));
        }

        ssl_ca_location = GetConfigOption("kafka_ssl_ca_location", true, false);
        if (ssl_ca_location && strlen(ssl_ca_location) > 0)
        {
            rd_kafka_conf_set(conf, "ssl.ca.location", ssl_ca_location, errstr, sizeof(errstr));
        }

        ssl_endpoint_algo = GetConfigOption("kafka_ssl_endpoint_identification_algorithm", true, false);
        if (ssl_endpoint_algo && strlen(ssl_endpoint_algo) > 0)
        {
            rd_kafka_conf_set(conf, "ssl.endpoint.identification.algorithm", ssl_endpoint_algo, errstr, sizeof(errstr));
        }
    }
     
    // 只有当协议涉及 SASL 时才设置 SASL 相关参数
    is_sasl_protocol = (security_protocol &&
                             (strcmp(security_protocol, "SASL_PLAINTEXT") == 0 ||
                              strcmp(security_protocol, "SASL_SSL") == 0));

    if (is_sasl_protocol)
    {
        sasl_mechanism = GetConfigOption("kafka_sasl_mechanism", true, false);
        if (sasl_mechanism && strlen(sasl_mechanism) > 0)
        {
            rd_kafka_conf_set(conf, "sasl.mechanism", sasl_mechanism, errstr, sizeof(errstr));

            // 根据 SASL 机制决定是否设置用户名/密码
             needs_auth = (strcmp(sasl_mechanism, "PLAIN") == 0 ||
                               strncmp(sasl_mechanism, "SCRAM", 5) == 0); // SCRAM-SHA-256, SCRAM-SHA-512

            if (needs_auth)
            {
                sasl_username = GetConfigOption("kafka_sasl_username", true, false);
                if (sasl_username && strlen(sasl_username) > 0)
                {
                    rd_kafka_conf_set(conf, "sasl.username", sasl_username, errstr, sizeof(errstr));
                }

                sasl_password = GetConfigOption("kafka_sasl_password", true, false);
                if (sasl_password && strlen(sasl_password) > 0)
                {
                    rd_kafka_conf_set(conf, "sasl.password", sasl_password, errstr, sizeof(errstr));
                }
            }
        }
    }
    // --- 基础连接参数 ---

    // 客户端ID
    rd_kafka_conf_set(conf, "client.id", GetConfigOption("kafka_client_id", true, false), errstr, sizeof(errstr));

    // 日志级别
    rd_kafka_conf_set(conf, "log_level", GetConfigOption("kafka_log_level", true, false), errstr, sizeof(errstr));

    // 调试功能
    rd_kafka_conf_set(conf, "debug", GetConfigOption("kafka_debug", true, false), errstr, sizeof(errstr));

    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        ereport(LOG, (errhidecontext(true), errmsg("ERROR: Config set fail: %s", errstr)));
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    // 创建Kafka实例
    *apply_g_rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!*apply_g_rk)
    {
        ereport(ERROR, (errhidecontext(true), errmsg("ERROR: New producer fail: %s", errstr)));
        rd_kafka_conf_destroy(conf);
        // 初始化失败，直接报错退出，避免后续空指针
        ereport(ERROR, (errhidecontext(true), errmsg("Kafka producer initialization failed")));
        return -1;
    }

    // 创建主题
    *apply_g_rkt = rd_kafka_topic_new(*apply_g_rk, topic, NULL);
    if (!*apply_g_rkt)
    {
        ereport(LOG, (errhidecontext(true), errmsg("ERROR: Failed to create topic: %s", topic)));
        rd_kafka_destroy(*apply_g_rk);
        *apply_g_rk = NULL;
        ereport(ERROR, (errhidecontext(true), errmsg("Kafka topic creation failed")));
        return -1;
    }

    // ereport(LOG, "SUCCESS: Kafka 初始化成功 [broker:%s] [topic:%s]", brokers, topic);

    // ereport(LOG,(errmsg("SUCCESS: Kafka 初始化成功 [broker:%s] [topic:%s]",brokers,topic)));
    ereport(LOG, (errhidecontext(true), errmsg("SUCCESS: Kafka 初始化成功 [broker:%s] [topic:%s]", brokers, topic)));
    return 0;
}





void kafka_send_optimized(char *msg, rd_kafka_t *rk, rd_kafka_topic_t *rkt) {
    int msg_len;
    int retries = 0;
    const int MAX_RETRY_ATTEMPTS = 10;
    long long estimated_size;
    rd_kafka_resp_err_t err;
    if (!rk || !rkt || !msg) {
        ereport(LOG, (errhidecontext(true), errmsg("ERROR: Invalid parameters")));
        return;
    }

     msg_len= strlen(msg);
    // 1. 计算消息大小（粗略估算，包含一些协议开销）
     estimated_size= msg_len + 100; 

    // 2. 检查本地计数器（双重保险：条数 OR 容量）
    // 如果已经达到条数上限 或者 预计加入后会超过内存上限
    if (message_count >= max_message_count || 
        (local_memory_usage + estimated_size) >= ((long long)max_memory_kbytes * 1024)) {
    
        // --- 触发强制 Flush ---
        // 队列满了，必须阻塞等待发送出去一部分
        int result = rd_kafka_flush(rk, 1000); // 等待 1秒
        message_count=0;
        if (result != 0) {
            // 如果 1秒 内没刷完，记录警告，但不报错，继续尝试入队
            ereport(WARNING, (errhidecontext(true), 
                             errmsg("Kafka queue is full, forced flush timed out. Proceeding with risk.")));
        }
        
        // Flush 后，计数器会被回调或内部逻辑重置
        // 这里我们简单地重新读取或依赖全局状态，继续执行 produce
    }

    // 3. 尝试发送
    // 使用 do-while 循环处理队列满的情况
   

    do {
        err = rd_kafka_produce(
            rkt, 
            RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY,
            (void *)msg, 
            msg_len,
            NULL, 0, NULL
        );

        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            // 4. 底层队列真的满了：强制 Flush 并等待
            ereport(LOG, (errhidecontext(true), 
                         errmsg("Kafka internal queue full, forcing flush...")));
     
            // 执行阻塞式 Flush
            rd_kafka_flush(rk, 2000); // 给予更长时间等待 Broker 响应
            message_count=0;
            // Flush 后，底层队列空间应该会被释放
            // 调用 poll 处理可能的响应
            rd_kafka_poll(rk, 100);
            
            retries++;
            if (retries > MAX_RETRY_ATTEMPTS) {
                ereport(ERROR, (errhidecontext(true), 
                               errmsg("Failed to send after %d forced flushes. Aborting.", MAX_RETRY_ATTEMPTS)));
                return;
            }
        } else if (err) {
            // 其他错误（如序列化失败、Broker 不可达等）
            ereport(LOG, (errhidecontext(true), 
                         errmsg("Produce failed: %s", rd_kafka_err2str(err))));
            return;
        }
        
    } while (err == RD_KAFKA_RESP_ERR__QUEUE_FULL);

    // 5. 发送成功后的处理
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        // 增加计数器
        message_count++;
        local_memory_usage += estimated_size;

        // 可选：定期 Poll 驱动 I/O
        if (message_count % 10 == 0) {
            rd_kafka_poll(rk, 0);
        }

        // --- 可选的自动 Flush ---
        // 如果积攒到一定条数（例如 1/10 的上限），自动 Flush 以降低延迟
        // if (message_count % (max_message_count / 10) == 0) {
        //     rd_kafka_flush(rk, 1000);
        // }
    }
}