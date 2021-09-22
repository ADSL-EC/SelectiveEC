# About SelectiveEC

SelectiveEC is a new recovery task scheduling module that provides provable network traffic and recovery load balancing for large-scale EC-based storage systems. It relies on bipartite graphs to model the recovery traffic among live nodes. Then, it intelligently selects tasks to form batches and carefully determines where to read source blocks or to store recovered ones, using theories such as a perfect or maximum matching and k-regular spanning subgraph. SelectiveEC supports single-node failure and multi-node failure  recovery, and can be deployed in both  homogeneous and heterogeneous network environments. We implement SelectiveEC in HDFS 3. SelectiveEC increases the recovery throughput by up to 30.68% compared with state-of-the-art baselines in homogeneous network environments.  It further achieves  1.32x  recovery throughput and 1.23x benchmark throughput  of HDFS on average in heterogeneous network environments , due to the straggler avoidance by the balanced scheduling.

If you use SelectiveEC in your work or research, please kindly let us know.  Regular version is under review in IEEE Transactions on Parallel and Distributed Systems. We also encourage you to reference our paper:

Here is the workshop bibtex:

       @inproceedings{xu2020selectiveec,
        author = {Xu, Liangliang and Lyu, Min and Li, Qiliang and Xie, Lingjiang and Xu, Yinlong},
        title = {SelectiveEC: Selective Reconstruction in Erasure-coded Storage Systems},
        booktitle = {Proceedings of the 12th USENIX Workshop on Hot Topics in Storage and File Systems},
        series = {HotStorage '20},
        year = {2020},
        publisher = {USENIX}
      } 


## Dependencies

* JDK 1.8.0
* Apache Maven 3.6.3
* Protobuf 2.5.0
* CMake 3.14.5

## Build

To configure and build SelectiveEC, you need to install the required dependencies first. Then execute the following commands.
```bash
$ git clone git@github.com:QiliangLi/SelectiveEC.git
$ cd SelectiveEC
$ mvn package -Pdist,native -DskipTests -Dtar
```

## Configuration

We configure our 18 node cluster with the following hostname and ip address.
| Hostname | IP Address |
| :----: | :----: |
| node1 | 100.0.0.1 |
| node2 | 100.0.0.2 |
| node3 | 100.0.0.3 |
| node4 | 100.0.0.4 |
| node5 | 100.0.0.5 |
| node6 | 100.0.0.6 |
| node7 | 100.0.0.7 |
| node8 | 100.0.0.8 |
| node10 | 100.0.0.9 |
| node11 | 100.0.0.10 |
| node12 | 100.0.0.11 |
| node13 | 100.0.0.12 |
| node14 | 100.0.0.13 |
| node15 | 100.0.0.14 |
| node16 | 100.0.0.15 |
| node17 | 100.0.0.16 |
| node18 | 100.0.0.17 |
| node19 | 100.0.0.18 |

We create the recovery-schedule.xml to configure the parameters used in SelectiveEC. The meaning of parameters and their default values are listed as follows.
| Parameter | Value | Notes |
| :----: | :----: | :----: |
| schedule.k | 6 | K in erasure coding policy |
| schedule.m | 3 | M in erasure coding policy |
| recovery.blocksize | 16 | Block size in HDFS |
| recovery.bandwidth | 30 | Bandwidth used for reconstruction |

We configure parameters of the hadoop daemons with the following values.

### hdfs-site.xml

```xml
<configuration>
	<property>
        <name>dfs.replication</name>
        <value>3</value>
	</property>
	<property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>100.0.0.1:50090</value>
	</property>
	<property>
        <name>heartbeat.recheck.interval</name>
        <value>15000</value>
	</property>
	<property>
        <name>dfs.heartbeat.interval</name>
        <value>3s</value>
	</property>
	<property>
  		<name>dfs.permissions</name>
  		<value>false</value>
	</property>
	<property>
		<name>dfs.block.size</name>
		<value>16M</value>
	</property>
	<property>
		<name>dfs.datanode.data.dir</name>
		<value>${HADOOP_HOME}/data</value>
	</property>
</configuration>
```

### core-site.xml

```xml
<configuration>
	<property>
        <name>fs.defaultFS</name>
        <value>hdfs://node1</value>
	</property>
	<property>
        <name>hadoop.tmp.dir</name>
        <value>${HADOOP_HOME}/tmp</value>
	</property>
</configuration>
```

### mapred-site.xml

```xml
<configuration>
	<property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
	</property>
	<property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
	</property>
	<property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
	</property>
	<property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
	</property>
</configuration>
```

### yarn-site.xml

```xml
<configuration>
	<property>
        <name>yarn.resourcemanager.hostname</name>
        <value>node1</value>
	</property>
	<property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
	</property>
	<property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>1024</value>
	</property>
	<property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>8192</value>
	</property>
	<property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
	</property>
	<property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
	</property>
	<property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>8192</value>
	</property>
	<property>
        <name>yarn.nodemanager.vmem-pmem-ratio</name>
        <value>2.1</value>
	</property>
</configuration>
```

We configure workers are listed as follows.
```
node2
node3
node4
node5
node6
node7
node8
node10
node11
node12
node13
node14
node15
node16
node17
node18
node19
```