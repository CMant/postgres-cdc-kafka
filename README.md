# postgres-cdc-kafka

`postgres-cdc-kafka` 是一个基于 PostgreSQL 源码深度定制开发的 CDC（Change Data Capture）工具。它利用 PostgreSQL 原生的 `pgoutput` 逻辑解码插件，将数据库的变更事件高效地捕获并发送到 Kafka 消息队列中。

---

## 🎯 开发背景

现有的 PostgreSQL CDC 解决方案，如 Debezium 和 Flink CDC，虽然功能强大，但在某些场景下存在一些痛点：

*   **性能瓶颈**：核心代码使用 Java 编写，相较于 C 语言，运行效率存在提升空间。
*   **解码效率**：通常依赖 `decoderbufs` 等非原生解码器。PostgreSQL 原生提供了 `pgoutput` 和 `test_decoding` 两种插件，其中 `pgoutput` 在性能和稳定性上更具优势。
*   **配置复杂**：部署和配置流程相对繁琐，学习成本较高。

---

## ✨ 核心优势

`postgres-cdc-kafka` 旨在解决上述问题，提供一个更高效、更简洁的 CDC 方案：

*   **原生血统**：深度集成于 PostgreSQL 18 源码，保持了 99.999% 的原生特性，仅在此基础上扩展了 CDC 功能。
*   **极致性能**：采用纯 Linux C 语言实现，性能远超基于 JVM 的解决方案。
*   **高效解码**：直接使用 PostgreSQL 原生 `pgoutput` 插件。日志解码（将 WAL 日志转换为真实数据）的步骤从发布端移至订阅端，资源消耗与订阅负载成正比，极为高效。
*   **极简配置**：配置方式与 PostgreSQL 原生的发布/订阅 (`PUBLISH/SUBSCRIBE`) 模型完全一致。唯一的区别在于，订阅端的表不存储实际数据（我们屏蔽了数据应用 `apply` 步骤），只用于定义复制槽和同步 DDL 结构。
*   **生态兼容**：无缝集成现有的高可用（HA）解决方案。您可以继续使用 `repmgr`、`Patroni` 构建集群，或将其部署在 K8s 环境中。由于节点间仅传输少量的 DDL 同步命令，对网络和存储的压力极小，甚至可配合远程共享存储使用。


---

## 🖼️ 架构图解

### 整体流程
下图展示了 `postgres-cdc-kafka` 如何捕获变更并发送到 Kafka：
   
   <img width="1199" height="484" alt="图片" src="https://github.com/user-attachments/assets/61f2e0cc-5bda-4073-a83c-278a446e5371" />
   
  > `postgres-cdc-kafka` 支持对单个数据库发起多个并发订阅，这与 PostgreSQL 原生订阅机制保持一致。它接收逻辑 WAL 日志，将其解码为 JSON 格式的可读数据，并投递到 Kafka。下游系统可以轻松地将这些数据路由到 Doris、StarRocks 或其他 OLAP 引擎进行分析。

### INSERT 操作处理
下图解释了在 `INSERT` 操作中，`postgres-cdc-kafka` 如何优化处理流程：

   <img width="1084" height="628" alt="图片" src="https://github.com/user-attachments/assets/81e01678-d7a2-4111-a35b-9a2e54a5e1a1" />

> 在获取到 `newtuple` 结构体中的数据后，`postgres-cdc-kafka` **跳过了** 将数据实际写入表中的昂贵 `apply` 操作。同时，对于这类不涉及真实数据写入的操作，它巧妙地将行级排他锁（Exclusive Lock）降级为行级共享锁（Share Lock），进一步提升了性能。

### Kafka Producer 映射
每个订阅实例对应一个独立的 Kafka Producer 进程：

   <img width="989" height="271" alt="图片" src="https://github.com/user-attachments/assets/16419928-e760-4673-932e-850657daa1ad" />  
   
### 数据分区策略
为了保证数据在 Kafka 中的顺序性，`postgres-cdc-kafka` 采用了以下策略： 

   <img width="1067" height="310" alt="图片" src="https://github.com/user-attachments/assets/55f25f3c-da4e-43b2-a567-b696fadaf58e" />  
   
> 由于数据库内部的数据应用（apply）必须严格有序，我们将**数据库名称**映射为 Kafka 的 Topic 名称。这样，来自同一个数据库的变更事件在投递到 Kafka 后，能够保证在单个 Topic 分区内严格有序（库内有序），而不同数据库之间的变更则无序（库间无关）。

---

## 🔄 高可用（HA）支持

`postgres-cdc-kafka` 完全继承了 PostgreSQL 的高可用架构能力。您可以使用 `repmgr`、`Patroni` 或 K8s Operator 等成熟方案来保障 CDC 服务的持续稳定运行。

   <img width="722" height="376" alt="图片" src="https://github.com/user-attachments/assets/60a1c6f7-4141-4026-847c-49863b4724b5" />

---

# 投递到kafka中的数据展示  
   
  <img width="1180" height="662" alt="图片" src="https://github.com/user-attachments/assets/066aac0c-0158-4e05-bede-40efdafe959b" />

# 安装使用
 > 仅提供二进制版本，基于Rocky9.7编译。configure参数如下，支持大部分x86平台。 debain-bookworm 容器也可以。 

  ```bash
CFLAGS="-O3 -march=x86-64 -mtune=generic -funroll-loops -fomit-frame-pointer" \
CXXFLAGS="-O3 -march=x86-64 -mtune=generic" \
LDFLAGS="-Wl,-O2 -Wl,--as-needed" \
./configure \
--prefix=/pgslave \
--disable-debug \
--disable-profiling \
--disable-coverage \
--disable-dtrace \
--disable-tap-tests \
--disable-injection-points \
--disable-cassert \
--enable-nls=no \
--disable-rpath
  ```
## 新增了一些kafka_* 配置参数 
> 两个关键配置
```bash
#kafka server broker address
kafka_server='127.0.0.1:9092'
# Maximum number of messages allowed on the producer queue.
# Default: 1000 max_value = 2147483647 
# Pay attention to this parameter: if Kafka can't keep up with the load or runs out of memory, reduce this value. 
kafka_queue_buffering_max_messages =1000 

```
## 创建订阅时注意事项  
> 以下命令默认（WITH (copy_data = true)）会把表中的数据通过copy的方式抽取出来。存量数据初始化不用担心，如果存量数据很多，请适当地调整 `kafka_queue_buffering_max_messages` 参数。
```sql
CREATE SUBSCRIPTION sbtest
CONNECTION 'host=127.0.0.1 port=5432 dbname=sbtest user=postgres'
PUBLICATION sbtest
```

# 最后
>  不想开源。 但二进制大家可以免费拿去用。 GDB很辛苦，程序崩溃过很多次，很多问题AI解决不了。 如果软件有其他问题请邮件沟通 `a645895855@163.com`
  
