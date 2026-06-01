
#include "kafka.h"

static rd_kafka_topic_t *apply_g_rkt=NULL;
static rd_kafka_t        *apply_g_rk=NULL;

int main() {
    // 1. 初始化：传 全局变量的地址（二级指针）
    kafka_init(&apply_g_rk, &apply_g_rkt, "192.168.227.135:9092", "postgres");

    // 2. 打印全局变量（验证地址）
    printf("%p  vvvvv   %p\n", apply_g_rk, apply_g_rkt);   // 变量值（Kafka实例地址）
    printf("%p  xxxxx  %p\n", &apply_g_rk, &apply_g_rkt); // 变量本身的内存地址（固定）

    // 3. 发送消息：传 全局变量本身（一级指针）
    kafka_send("Hello Kafka", apply_g_rk, apply_g_rkt);

    return 0;
}