# Spark概述



## Spark的特点    

Spark看到MRv2的问题，对MapReduce做了大量优化，总结如下：

- 减少磁盘I/O

  > 随着实时大数据应用越来越多，Hadoop作为离线的高吞吐、低响应框架已不能满足这类需求。HadoopMapReduce的map端将中间输出和结果存储在磁盘中，reduce端又需要从磁盘读写中间结果，势必造成磁盘IO成为瓶颈。Spark允许将map端的中间输出和结果存储在内存中，reduce端在拉取中间结果时避免了大量的磁盘I/O。Hadoop Yarn中的ApplicationMaster申请到Container后，具体的任务需要利用NodeManager从HDFS的不同节点下载任务所需的资源（如Jar包），这也增加了磁盘I/O。Spark将应用程序上传的资源文件缓冲到Driver本地文件服务的内存中，当Executor执行任务时直接从Driver的内存中读取，也节省了大量的磁盘I/O。

- 增加并行度

  > 由于将中间结果写到磁盘与从磁盘读取中间结果属于不同的环节，Hadoop将它们简单的通过串行执行衔接起来。Spark把不同的环节抽象为Stage，允许多个Stage既可以串行执行，又可以并行执行。

- 避免重新计算

  > 当Stage中某个分区的Task执行失败后，会重新对此Stage调度，但在重新调度的时候会过滤已经执行成功的分区任务，所以不会造成重复计算和资源浪费。

- 可选的Shuffle排序

  > HadoopMapReduce在Shuffle之前有着固定的排序操作，而Spark则可以根据不同场景选择在map端排序或者reduce端排序。

- 灵活的内存管理策略

  > Spark将内存分为堆上的存储内存、堆外的存储内存、堆上的执行内存、堆外的执行内存4个部分。Spark既提供了执行内存和存储内存之间是固定边界的实现，又提供了执行内存和存储内存之间是“软”边界的实现。Spark默认使用“软”边界的实现，执行内存或存储内存中的任意一方在资源不足时都可以借用另一方的内存，最大限度的提高资源的利用率，减少对资源的浪费。Spark由于对内存使用的偏好，内存资源的多寡和使用率就显得尤为重要，为此Spark的内存管理器提供的Tungsten实现了一种与操作系统的内存Page非常相似的数据结构，用于直接操作操作系统内存，节省了创建的Java对象在堆中占用的内存，使得Spark对内存的使用效率更加接近硬件。Spark会给每个Task分配一个配套的任务内存管理器，对Task粒度的内存进行管理。Task的内存可以被多个内部的消费者消费，任务内存管理器对每个消费者进行Task内存的分配与管理，因此Spark对内存有着更细粒度的管理。



## 基本概念
 要想对Spark有整体性的了解，推荐读者阅读Matei Zaharia的Spark论文。此处笔者先介绍Spark中的一些概念：

- RDD（resillient distributed dataset）

  > 弹性分布式数据集。Spark应用程序通过使用Spark的转换API可以将RDD封装为一系列具有血缘关系的RDD，也就是DAG。只有通过Spark的动作API才会将RDD及其DAG提交到DAGScheduler。RDD的祖先一定是一个跟数据源相关的RDD，负责从数据源迭代读取数据。

- DAG（Directed Acycle graph）

  > 有向无环图。在图论中，如果一个有向图无法从某个顶点出发经过若干条边回到该点，则这个图是一个有向无环图（DAG图）。Spark使用DAG来反映各RDD之间的依赖或血缘关系。

- Partition

  > 数据分区。即一个RDD的数据可以划分为多少个分区。Spark根据Partition的数量来确定Task的数量。

- NarrowDependency

  > 窄依赖。即子RDD依赖于父RDD中固定的Partition。NarrowDependency分为OneToOneDependency和RangeDependency两种。

- ShuffleDependency

  > Shuffle依赖，也称为宽依赖。即子RDD对父RDD中的所有Partition都可能产生依赖。子RDD对父RDD各个Partition的依赖将取决于分区计算器（Partitioner）的算法。

- Job

  > 用户提交的作业。当RDD及其DAG被提交给DAGScheduler调度后，DAGScheduler会将所有RDD中的转换及动作视为一个Job。一个Job由一到多个Task组成。

- Stage

  > Job的执行阶段。DAGScheduler按照ShuffleDependency作为Stage的划分节点对RDD的DAG进行Stage划分（上游的Stage将为ShuffleMapStage）。因此一个Job可能被划分为一到多个Stage。Stage分为ShuffleMapStage和ResultStage两种。

- Task

  > 具体执行任务。一个Job在每个Stage内都会按照RDD的Partition 数量，创建多个Task。Task分为ShuffleMapTask和ResultTask两种。ShuffleMapStage中的Task为ShuffleMapTask，而ResultStage中的Task为ResultTask。ShuffleMapTask和ResultTask类似于Hadoop中的 Map任务和Reduce任务。



## Spark核心功能
Spark Core中提供了Spark最基础与最核心的功能，主要包括：

#### 基础设施

在Spark中有很多基础设施，被Spark中的各种组件广泛使用。这些基础设施包括Spark配置（SparkConf）、Spark内置的Rpc框架、事件总线（ListenerBus）、度量系统。

SparkConf用于管理Spark应用程序的各种配置信息。Spark内置的Rpc框架使用Netty实现，有同步和异步的多种实现，Spark各个组件间的通信都依赖于此Rpc框架。如果说Rpc框架是跨机器节点不同组件间的通信设施，那么事件总线就是SparkContext内部各个组件间使用事件——监听器模式异步调用的实现。度量系统由Spark中的多种度量源（Source）和多种度量输出（Sink）构成，完成对整个Spark集群中各个组件运行期状态的监控。

#### SparkContext

通常而言，用户开发的Spark应用程序（Application）的提交与执行都离不开SparkContext的支持。在正式提交Application之前，首先需要初始化SparkContext。SparkContext隐藏了网络通信、分布式部署、消息通信、存储体系、计算引擎、度量系统、文件服务、Web UI等内容，应用程序开发者只需要使用SparkContext提供的API完成功能开发。

#### SparkEnv

Spark执行环境（SparkEnv）是Spark中的Task运行所必须的组件。SparkEnv内部封装了Rpc环境（RpcEnv）、序列化管理器、广播管理器（BroadcastManager）、map任务输出跟踪器（MapOutputTracker）、存储体系、度量系统（MetricsSystem）、输出提交协调器（OutputCommitCoordinator）等Task运行所需的各种组件。

#### 存储体系

Spark优先考虑使用各节点的内存作为存储，当内存不足时才会考虑使用磁盘，这极大地减少了磁盘I/O，提升了任务执行的效率，使得Spark适用于实时计算、迭代计算、流式计算等场景。在实际场景中，有些Task是存储密集型的，有些则是计算密集型的，所以有时候会造成存储空间很空闲，而计算空间的资源又很紧张。Spark的内存存储空间与执行存储空间之间的边界可以是“软”边界，因此资源紧张的一方可以借用另一方的空间，这既可以有效利用资源，又可以提高Task的执行效率。此外，Spark的内存空间还提供了Tungsten的实现，直接操作操作系统的内存。由于Tungsten省去了在堆内分配Java对象，因此能更加有效的利用系统的内存资源，并且因为直接操作系统内存，空间的分配和释放也更迅速。

#### 调度系统

调度系统主要由DAGScheduler和TaskScheduler组成，它们都内置在SparkContext中。DAGScheduler负责创建Job、将DAG中的RDD划分到不同的Stage、给Stage创建对应的Task、批量提交Task等功能。TaskScheduler负责按照FIFO或者FAIR等调度算法对批量Task进行调度；为Task分配资源；将Task发送到集群管理器分配给当前应用的Executor上由Executor负责执行等工作。现如今，Spark增加了SparkSession和DataFrame等新的API，SparkSession底层实际依然依赖于SparkContext。

#### 计算引擎

计算引擎由内存管理器（MemoryManager）、Tungsten、任务内存管理器（TaskMemoryManager）、Task、外部排序器（ExternalSorter）、Shuffle管理器（ShuffleManager）等组成。MemoryManager除了对存储体系中的存储内存提供支持和管理，还外计算引擎中的执行内存提供支持和管理。Tungsten除用于存储外，也可以用于计算或执行。TaskMemoryManager对分配给单个Task的内存资源进行更细粒度的管理和控制。ExternalSorter用于在map端或reduce端对ShuffleMapTask计算得到的中间结果进行排序、聚合等操作。ShuffleManager用于将各个分区对应的ShuffleMapTask产生的中间结果持久化到磁盘，并在reduce端按照分区远程拉取ShuffleMapTask产生的中间结果。



## 编程模型

 Spark 应用程序从编写到提交、执行、输出的整个过程如图2-5所示。

![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/2018052711413040.png)

图2-5中描述了Spark编程模型的关键环节的步骤如下：

（1）用户使用SparkContext提供的API（常用的有textFile、sequenceFile、runJob、stop等）编写Driverapplication程序。此外，SparkSession、DataFrame、SQLContext、HiveContext及StreamingContext都对SparkContext进行了封装，并提供了DataFrame、SQL、Hive及流式计算相关的API。

（2）使用SparkContext提交的用户应用程序，首先会通过RpcEnv向集群管理器（ClusterManager）注册应用（Application）并且告知集群管理器需要的资源数量。

（3）集群管理器根据Application的需求，给Application分配Executor资源，并在Worker上启动CoarseGrainedExecutorBackend进程（CoarseGrainedExecutorBackend进程内部将创建Executor）。

（4）Executor所在的CoarseGrainedExecutorBackend进程在启动的过程中将通过RpcEnv直接向Driver注册Executor的资源信息，TaskScheduler将保存已经分配给应用的Executor资源的地址、大小等相关信息。

（5）SparkContext根据各种转换API，构建RDD之间的血缘关系（lineage）和DAG，RDD构成的DAG将最终提交给DAGScheduler。

（6）DAGScheduler给提交的DAG创建Job并根据RDD的依赖性质将DAG划分为不同的Stage。DAGScheduler根据Stage内RDD的Partition数量创建多个Task并批量提交给TaskScheduler。

（7）TaskScheduler对批量的Task按照FIFO或FAIR调度算法进行调度，然后给Task分配Executor资源，最后将Task发送给Executor由Executor执行。此外，SparkContext还会在RDD转换开始之前使用BlockManager和BroadcastManager将任务的Hadoop配置进行广播。

（8）集群管理器（ClusterManager）会根据应用的需求，给应用分配资源，即将具体任务分配到不同Worker节点上的多个Executor来处理任务的运行。Standalone、YARN、Mesos、EC2等都可以作为Spark的集群管理器。

（9）Task在运行的过程中需要对一些数据（例如中间结果、检查点等）进行持久化，Spark支持选择HDFS 、Amazon S3、Alluxio（原名叫Tachyon）等作为存储。



## Spark基本架构

从集群部署的角度来看，Spark集群由以下部分组成：

#### Cluster Manager

Spark的集群管理器，主要负责资源的分配与管理。Cluster Manager在Yarn部署模式下为ResourceManager；在Mesos部署模式下为Mesos master；在Standalone部署模式下为Master。

#### ResourceManager

分配的资源属于一级分配，它将各个Worker上的内存、CPU等资源分配给应用程序，但是并不负责对Executor的资源分配。Standalone部署模式下为Master会直接给应用程序分配内存、CPU以及Executor等资源。目前，Standalone、YARN、Mesos、EC2等都可以作为Spark的集群管理器。

#### Worker

Spark的工作节点。在Yarn部署模式下实际由NodeManager替代。Worker节点主要负责以下工作：创建Executor，将资源和任务进一步分配给Executor，同步资源信息给Cluster Manager。在Standalone部署模式下，Master将Worker上的内存、CPU以及Executor等资源分配给应用程序后，将命令Worker启动CoarseGrainedExecutorBackend进程（此进程会创建Executor实例）。

#### Executor

执行计算任务的一线组件。主要负责任务的执行以及与Worker、Driver的信息同步。

#### Driver App

客户端驱动程序，也可以理解为客户端应用程序，用于将任务程序转换为RDD和DAG，并与Cluster Manager进行通信与调度。Driver可以运行在客户端，也可以由客户端提交给Cluster Manager并由ClusterManager安排Worker运行。

这些组成部分之间的整体关系如图2-8所示。

![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/20180527114746705.png)