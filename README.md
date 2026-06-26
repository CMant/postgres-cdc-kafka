# postgres-cdc-kafka  属于PostgreSql的地表最强CDC工具

`postgres-cdc-kafka`  在PostgreSQL18 源码上增加了一些功能。主要是利用PostgreSQL 原生的 `pgoutput` 逻辑解码插件，将数据库的变更事件高效地捕获并发送到 Kafka 消息队列中。


---

## 🎯 开发背景

现有的 `PostgreSQL CDC` 解决方案，如 `Debezium` 和 `Flink CDC`，虽然功能强大，但在某些场景下存在一些痛点：

*   **性能瓶颈**：核心代码使用 Java 编写，相较于 C 语言，运行效率存在提升空间。
*   **配置复杂**：部署和配置流程相对繁琐，学习成本较高。使用`postgres-cdc-kafka`就像在使用另一套`postgresql`数据库。

---

## ✨ 核心优势

`postgres-cdc-kafka` 旨在解决上述问题，提供一个更高效、更简洁的 CDC 方案：

*   **原生血统**：深度集成于 PostgreSQL 18 源码，保持了 99.999% 的原生特性，仅在此基础上扩展了 CDC 功能。
*   **极致性能**：采用纯 Linux C 语言实现，性能远超基于 JVM 的解决方案。
*   **高效解码**：直接使用 PostgreSQL 原生 `pgoutput` 插件。无需安装三方扩展，将对逻辑日志流的解码过程后移，解放发布端业务库的压力（虽然大多数情况下用不出什么差别，但是极限情况下强一点也是强！关键是原生插件稳定！）。
*   **极简配置**：配置方式与 PostgreSQL 原生的发布/订阅 (`PUBLISH/SUBSCRIBE`) 模型完全一致。唯一的区别在于，订阅端的表不存储实际数据（我屏蔽了数据 `apply` 日志的步骤），只需要表结构完成订阅过程，索引什么的统统都不需要建。
*   **生态兼容**：流复制功能正常，现有的高可用（HA）解决方案。比如 `repmgr`、`Patroni` 依然可以用。当然，您也可以将其部署在 K8s 环境。由于节点间仅传输少量的 DDL和订阅命令，对网络和存储的压力极小。
*   **管理完善**：虽然对`pg_recvlogical.c` 进行二次开发更方便，但是 1.无法避免使用低效解码插件；2.缺少完善的管理维护系统，3.对存量数据无法直接处理。`postgres-cdc-kafka`管理CDC任务就和管理PostgreSql的发布订阅一样。

---
## 使用`pgoutput`的缺陷  

> 参考debezium的官网
###  Additionally, the pgoutput logical decoding output plug-in does not capture values for generated columns, resulting in missing data for these columns in the connector’s output.
###  decoderbufs passes a byte array (byte[]) representation of the column data. pgoutput passes a string representation of the column data.  
> 也许是debezium的设计使用pgoutput会带来比decoderbufs更高的代价。但本人认为：投递到kafka中的数据终究是要全部转换成字符串的。`postgres-cdc-kafka` 在内核代码中获取logical日志的字面量后拼凑成json的格式，并未对pgoutput转换过程做任何修改。至少在协议紧凑性方面，在`postgresql`源码中的pgoutput比decoderbufs更有优势。
> 此外，debezium或者flink的sink端可以选择多种形式，比如直插数据库，也许是这种原因得出decoderbufs效率更好的结论。在实际的施工中，CDC直接sink入库可能会因为各种原因出现中断或报错，而使用kafka作为一层缓冲，消费端可以选择必要的字段写入是一种比较稳妥的方式。
> 对于下游消费端关于数据类型的转换代价。本人认为不需要考虑。decoderbufs内部包含数据类型，那么下游消费时，如果拼凑insert语句时需要针对不同的数据类型对value部分进行转换和调整就不需要代价吗？我认为，数据库中存放的数据是人可读的，也就是‘字符串’，所有的一切都是字符串。并且大部分数据库insert语句的value部分全部改成字符串完全不影响正常写入，转换交由数据库自己解决。消费端程序完全没有转换对应数据类型的必要，直接插入即可。
> 如果需要数据类型，也可以添加。


---

## 🖼️ 架构图解

### 整体流程
> 下图展示了 `postgres-cdc-kafka` 如何捕获变更并发送到 Kafka：
   
   <img width="1199" height="484" alt="图片" src="https://github.com/user-attachments/assets/61f2e0cc-5bda-4073-a83c-278a446e5371" />
   
  > `postgres-cdc-kafka` 支持对单个数据库发起多个并发订阅，这与 PostgreSQL 原生订阅机制保持一致。它接收逻辑 WAL 日志，将其解码为 JSON 格式的可读数据，并投递到 Kafka。下游系统可以轻松地将这些数据路由到 Doris、StarRocks 或其他 OLAP 引擎进行分析。

### INSERT 操作处理
> 下图解释了在 `INSERT` 操作中，`postgres-cdc-kafka` 如何优化处理流程(update和delete同理)：

   <img width="1084" height="628" alt="图片" src="https://github.com/user-attachments/assets/81e01678-d7a2-4111-a35b-9a2e54a5e1a1" />

> 在获取到 `newtuple` 结构体中的数据后，`postgres-cdc-kafka` **跳过了** 将数据实际写入表中的昂贵 `apply` 操作。此外，因为表中根本不存放数据，我将代码中部分行级排他锁替换成了行级访问锁，进一步提升了性能。复制槽消耗功能正常，您可以在发布端或者`postgres-cdc-kafka` 查看复制延迟。

### Kafka Producer 映射
> 每个订阅实例对应一个独立的 Kafka Producer 进程：

   <img width="989" height="271" alt="图片" src="https://github.com/user-attachments/assets/16419928-e760-4673-932e-850657daa1ad" />  
   
### 数据分区策略
为了保证数据在 Kafka 中的顺序性，`postgres-cdc-kafka` 采用了以下策略： 

   <img width="1067" height="310" alt="图片" src="https://github.com/user-attachments/assets/55f25f3c-da4e-43b2-a567-b696fadaf58e" />  
   
> 值得注意的是，由于PostgreSql原生的发布订阅也仅保证在同一个订阅内数据是与事务顺序一致的。因此，在使用`postgres-cdc-kafka`进行订阅时，如果一定要向一个数据库发起多个订阅，请务必根据业务情况进行规划。切记不要为了逻辑上划分“好看” 而不顾管理成本的复杂。况且维护多个复制槽对发布端数据库来说负担并不轻。

---

# 🔄 高可用（HA）支持

> `postgres-cdc-kafka` 完全继承了 PostgreSQL 的高可用架构能力。您可以使用 `repmgr`、`Patroni` 或 K8s Operator 等成熟方案来保障 CDC 服务的持续稳定运行。

   <img width="722" height="376" alt="图片" src="https://github.com/user-attachments/assets/60a1c6f7-4141-4026-847c-49863b4724b5" />

---

# 投递到kafka中的数据展示  
> sysbench  oltp_write_only实拍，显得真实一点。
  <img width="1180" height="662" alt="图片" src="https://github.com/user-attachments/assets/066aac0c-0158-4e05-bede-40efdafe959b" />

# 性能  

> 有状态服务的数据变更对于无状态纯cpu内存操作的`postgres-cdc-kafka`而言，根本算不上什么压力。您可以大胆尝试让一个`postgres-cdc-kafka`同时处理来自多个PostgreSql的发布。
> 6C12T虚拟机，NVME，sysbench oltp_write_only  1447407为worker线程。
<img width="791" height="228" alt="ScreenShot_2026-05-21_105507_375" src="https://github.com/user-attachments/assets/5e8554e1-6c18-49e4-b8a3-7c1c0cf673d6" />



# 安装使用
 > 基于Rocky9.7编译。configure参数如下，支持大部分x86平台。 debain-bookworm 容器也可以。 

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
> 以下命令默认（WITH (copy_data = true)）会把表中的数据通过copy的方式抽取出来。存量数据初始化不用担心，如果存量数据很多，同时要订阅的表很多，请适当地调整 `kafka_queue_buffering_max_messages` 参数，避免压垮kafka。
```sql
CREATE SUBSCRIPTION sbtest
CONNECTION 'host=127.0.0.1 port=5432 dbname=sbtest user=postgres'
PUBLICATION sbtest
```

# 最后
>  GDB很辛苦，程序崩溃过很多次，很多问题AI是解决不了的。 关于软件有其他问题或者需要安装部署维护服务，请邮件沟通 `a645895855@163.com`。

<img width="496" height="354" alt="图片" src="https://github.com/user-attachments/assets/060158ec-b6dd-4784-bb4d-e322f971e804" />

  
