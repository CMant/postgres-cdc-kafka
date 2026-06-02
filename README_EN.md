# postgres-cdc-kafka : The Ultimate Native CDC Connector for PostgreSQL

`postgres-cdc-kafka` is the ultimate CDC (Change Data Capture) tool for PostgreSQL. It is built by extending the PostgreSQL 18 source code, primarily utilizing PostgreSQL's native `pgoutput` logical decoding plugin to efficiently capture database change events and publish them to the Kafka message queue.


---

## 🎯 Development Background

Existing PostgreSQL CDC solutions, such as Debezium and Flink CDC, are powerful but have certain pain points:

*   **Performance Bottleneck**: Core code is written in Java, which has room for improvement in runtime efficiency compared to C.
*   **Decoding Efficiency**: They often rely on non-native decoders like `decoderbufs`. While PostgreSQL provides native plugins like `pgoutput` and `test_decoding`, the latter is convenient but less efficient than the native `pgoutput`.
*   **Complex Configuration**: Deployment and configuration processes can be cumbersome, resulting in a higher learning curve.

---

## ✨ Core Advantages

`postgres-cdc-kafka` aims to solve the above issues, providing a more efficient and streamlined CDC solution:

*   **Native Pedigree**: Deeply integrated into the PostgreSQL 18 source code, maintaining 99.999% native characteristics while extending CDC functionality.
*   **Ultimate Performance**: Implemented purely in Linux C language, delivering far superior performance compared to JVM-based solutions.
*   **Efficient Decoding**: Directly uses PostgreSQL's native `pgoutput` plugin. No third-party extensions are required. By moving the decoding process downstream, it relieves pressure on the source database (While differences might be negligible under normal loads, stability and edge-case performance matter!).
*   **Minimal Configuration**: Configuration is identical to PostgreSQL's native `PUBLISH/SUBSCRIBE` model. The only difference is that the subscriber tables do not store actual data (the `apply` step is bypassed). You only need table structures to complete the subscription; indexes are entirely unnecessary.
*   **Ecosystem Compatibility**: Native streaming replication remains functional. Existing High Availability (HA) solutions like `repmgr` and `Patroni` are fully supported. You can also deploy it in Kubernetes environments. Node-to-node traffic consists only of minimal DDL and subscription commands, exerting negligible pressure on networks and storage.
*   **Robust Management**: While modifying `pg_recvlogical.c` might seem easier, it avoids inefficient decoders and provides a comprehensive management system. It also handles存量 data (initial snapshots) seamlessly. Managing CDC tasks feels exactly like managing native PostgreSQL subscriptions.

---

## 🖼️ Architecture Overview

### Overall Workflow
> The diagram below illustrates how `postgres-cdc-kafka` captures changes and sends them to Kafka:
   
   <img width="1199" height="484" alt="图片" src="https://github.com/user-attachments/assets/61f2e0cc-5bda-4073-a83c-278a446e5371" />
   
 > `postgres-cdc-kafka` supports multiple concurrent subscriptions to a single database, consistent with PostgreSQL's native subscription mechanism. It receives logical WAL logs, decodes them into JSON format, and delivers them to Kafka. Downstream systems can easily route this data to Doris, StarRocks, or other OLAP engines.

### INSERT Operation Handling
> The following diagram explains how `postgres-cdc-kafka` optimizes the process for `INSERT` operations (Update and Delete follow the same principle):

   <img width="1084" height="628" alt="图片" src="https://github.com/user-attachments/assets/81e01678-d7a2-4111-a35b-9a2e54a5e1a1" />

> After obtaining the data from the `newtuple` structure, `postgres-cdc-kafka` **skips** the expensive `apply` operation (writing data to tables). Furthermore, since the tables store no data, row-level exclusive locks have been replaced with row-level access share locks, further boosting performance. Replication slot tracking works normally, allowing you to monitor replication lag.

### Kafka Producer Mapping
> Each subscription instance corresponds to an independent Kafka Producer process:
   <img width="989" height="271" alt="图片" src="https://github.com/user-attachments/assets/16419928-e760-4673-932e-850657daa1ad" />  
   
### Data Partitioning Strategy
> To guarantee data ordering within Kafka, `postgres-cdc-kafka` adopts the following strategy:
   <img width="1067" height="310" alt="图片" src="https://github.com/user-attachments/assets/55f25f3c-da4e-43b2-a567-b696fadaf58e" />  
   
> **Note**: Native PostgreSQL logical replication only guarantees transaction order within a single subscription. Therefore, if you create multiple subscriptions to one database, plan them according to business needs. Avoid creating subscriptions just for "logical tidiness" as managing multiple replication slots adds overhead to the publisher.

---

# 🔄 High Availability (HA) Support

> `postgres-cdc-kafka` fully inherits PostgreSQL's HA architecture capabilities. You can use `repmgr`, `Patroni`, or K8s Operators to ensure continuous and stable operation of your CDC services.

   <img width="722" height="376" alt="图片" src="https://github.com/user-attachments/assets/60a1c6f7-4141-4026-847c-49863b4724b5" />

---

# Sample Data in Kafka

> Real-world capture from `sysbench oltp_write_only`.
  <img width="1180" height="662" alt="图片" src="https://github.com/user-attachments/assets/066aac0c-0158-4e05-bede-40efdafe959b" />

# Performance

> Stateful data changes are negligible for the stateless, CPU/memory-bound operations of `postgres-cdc-kafka`. Feel free to let one instance handle publications from multiple PostgreSQL sources.
> Test Environment: 6C12T VM, NVMe storage. `1447407` is the worker thread.
<img width="791" height="228" alt="ScreenShot_2026-05-21_105507_375" src="https://github.com/user-attachments/assets/5e8554e1-6c18-49e4-b8a3-7c1c0cf673d6" />



# Installation and Usage

> Compiled based on Rocky 9.7. The configure parameters are as follows, supporting most x86 platforms. Debian Bookworm containers are also supported.
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
## Custom Kafka Runtime Parameters (postgresql.conf)
> Two core configurable Kafka options added:
```bash
#kafka server broker address
kafka_server='127.0.0.1:9092'
# Maximum number of messages allowed on the producer queue.
# Default: 1000 max_value = 2147483647 
# Pay attention to this parameter: if Kafka can't keep up with the load or runs out of memory, reduce this value. 
kafka_queue_buffering_max_messages =1000 

```
## Create CDC Subscription SQL
> `copy_data = true` is enabled by default, which automatically performs full historical snapshot dump via COPY for existing table data.
If massive initial bulk data exists across numerous subscribed tables, tune `kafka_queue_buffering_max_messages` properly to avoid overwhelming Kafka cluster during snapshot initialization.
```sql
CREATE SUBSCRIPTION sbtest
CONNECTION 'host=127.0.0.1 port=5432 dbname=sbtest user=postgres'
PUBLICATION sbtest
```

# Final Notes
>  Countless late-night GDB debugging & crash fixes were invested into this project; many low-level kernel/PG source bugs cannot be resolved with AI assistance alone.
For commercial deployment consulting, customization or maintenance service: contact via email: `a645895855@163.com`

<img width="496" height="354" alt="图片" src="https://github.com/user-attachments/assets/060158ec-b6dd-4784-bb4d-e322f971e804" />

  
