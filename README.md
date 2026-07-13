# Flink图书馆数据清洗课程设计
## 开发环境
JDK 1.8
Scala 2.12.12
Maven 3.6+
Apache Flink 1.17.0
IDE：IntelliJ IDEA 2021
运行集群：Ubuntu虚拟机 Standalone Flink（192.168.102.128:8081  注意：打开虚拟机的flink集群才可以查看）

## 项目功能介绍
1. 自定义无限模拟数据源，持续生成图书馆原始文本记录
2. 通过正则表达式清洗原始数据，提取分馆名称、周开放时长字段
3. 使用5秒滑动窗口分组聚合，统计各分馆总访客、平均开放时长
4. 支持两种运行模式：IDEA本地调试、打包上传Flink集群分布式运行
5. 清洗结果导出本地CSV文件

## 项目运行步骤
### 1. 本地IDEA调试
修改pom.xml中flink依赖scope，删除`<scope>provided</scope>`，直接运行Scala主类 `LibraryDataCleanScala`。

### 2. 虚拟机集群部署
1. Maven打包：`mvn package`
2. XFTP/Xshell将target下jar包上传至Ubuntu
3. 虚拟机启动Flink集群：`start-cluster.sh`
4. 提交任务命令：
```bash
flink run -c library.LibraryDataCleanScala xxx.jar
