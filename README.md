# postgres-cdc-kafka
# 开发原因
  postgresql 目前基于逻辑日志流的CDC工具比较常用的是Debezium，Flink，但是这两个存在3个比较严重的问题  
   1 代码本身使用JAVA编写，运行效率还有提升的空间。  
   2 使用decoderbufs作为逻辑解码工具，并非postgresql原生，postgresql原生一共提供2个，pgoutput和textcoding，无论是textcoding还是decodebufs 运行效率都不如pgoutput。
   3 配置复杂  
# postgres-cdc-kafka的优势  
   1 基于postgresql18 源码开发，99.999%以上保持原汁原味（实际上我只是增加了一些功能）  
   2 纯Linux C实现，代码运行效率把Java按在地上摩擦
   3 使用原生pgoutput解码插件，将逻辑日志流解码成人类可读的真实数据这一步从发布端后移到订阅端。发布订阅功能占用多少资源，它就占用多少。  
   4 postgres-cdc-kafka本身配置仍然和发布订阅操作流程一样，最大的区别是创建的表不会存放任何数据，我把apply数据这一步屏蔽了。  
   5 现有技术储备复用，仍然可以通过repmgr，patroni构建集群来保证cdc过程的HA，亦或者k8s。如4中所言，正因为在节点间传输的只是一些表的创建和订阅语句。哪怕远程共享磁盘也不影响运行效率。
# 图解
  <img width="1032" height="408" alt="ScreenShot_2026-05-22_092632_891" src="https://github.com/user-attachments/assets/07bb2919-4e11-4fbf-9af1-3fbf871d6966" />
