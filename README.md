PostgreSQL Database Management System
=====================================

This directory contains the source code distribution of the PostgreSQL
database management system.

PostgreSQL is an advanced object-relational database management system
that supports an extended subset of the SQL standard, including
transactions, foreign keys, subqueries, triggers, user-defined types
and functions.  This distribution also contains C language bindings.

Copyright and license information can be found in the file COPYRIGHT.

General documentation about this version of PostgreSQL can be found at
<https://www.postgresql.org/docs/18/>.  In particular, information
about building PostgreSQL from the source code can be found at
<https://www.postgresql.org/docs/18/installation.html>.

The latest version of this software, and related software, may be
obtained at <https://www.postgresql.org/download/>.  For more information
look at our web site located at <https://www.postgresql.org/>.

=====================================
2026-05-27 modify by TianJin-DBA CMant-yanyx   
Append：

I modified the software to decode PostgreSQL logical log streams into JSON format and deliver them to Kafka based on the publish-subscribe pattern.

Plugins like text_coding and wal2json can also decode logical log streams into JSON directly. Nevertheless, their decoding processes run on the primary node, resulting in higher overhead than the native pgoutput implementation.

To shift the decoding workload away from the primary node, I adjusted the source code to implement the PostgreSQL-CDC-to-Kafka pipeline. I disabled the logic that applies WAL data to physical tables. All subscription tables on this database remain empty and serve only as configuration carriers rather than regular data tables. In short, I converted stateful PostgreSQL into a stateless service.

This solution is fully compatible with mainstream PostgreSQL high-availability architectures, including repmgr, Patroni and Kubernetes. You can use shared storage or remote NAS to keep one single copy of data. Disk performance and network latency pose no concerns, as we only store metadata for tables and subscriptions. Alternatively, you can store three copies of data on separate nodes, a common practice for repmgr-based 3-node clusters.

In terms of load capacity, a one-to-one deployment works perfectly. As a stateless service, it delivers superior performance compared to traditional stateful solutions. It runs with high efficiency and outperforms Java-based CDC tools such as Flink and Debezium.

Notice: Please configure Kafka parameters properly to avoid excessive memory consumption and overwhelming the Kafka cluster.
