# SparkContext 初始化

## 流程

### 创建 Spark 执行环境 SparkEnv；
### 创建 RDD 清理器 metadataCleaner；

SparkContext 为 了 保 持 对 所 有 持 久 化 的 RDD 的 跟 踪， 使 用 类 型 是 TimeStampedWeakValueHashMap 的 persistentRdds 缓存。 metadataCleaner 的功能是清除过期的持久化 RDD。

从MetadataCleaner 的实现可以看出其实质是一个用 TimerTask 实现的定时器，不断调用
cleanupFunc: (Long) => Unit 这样的函数参数。构造 metadataCleaner 时的函数参数是 cleanup，
用于清理 persistentRdds 中的过期内容，

### 创建并初始化 Spark UI；

 DAGScheduler 是主要的产生各类 SparkListenerEvent 的源头，它将各种 SparkListenerEvent 发送到 listenerBus 的事件队列中， listenerBus 通过定时器将 SparkListenerEvent 事件匹配到具体的 SparkListener，改变 SparkListener 中的统计监控数据，最终由 SparkUI 的界面展示。从图 3-1 中还可以看到 Spark 里定义了很多监听器 SparkListener 的 实 现， 包 括 JobProgressListener、 EnvironmentListener、 StorageListener、ExecutorsListener，它们的类继承体系如图 3-2 所示

listenerBus 的类型是 LiveListenerBus。 LiveListenerBus 实现了监听器模型，通过监听事件
触发对各种监听器监听状态信息的修改，达到 UI 界面的数据刷新效果。 LiveListenerBus 由以
下部分组成：

- 事件阻塞队列

  > 类型为 LinkedBlockingQueue[SparkListenerEvent]，固定大小是 10 000；

- 监听器数组

  > 类型为 ArrayBuffer[SparkListener]，存放各类监听器 SparkListener。

- 事件匹配监听器的线程

  > 此 Thread 不断拉取 LinkedBlockingQueue 中的事件，遍历监
  > 听器，调用监听器的方法。任何事件都会在 LinkedBlockingQueue 中存在一段时间，
  > 然后 Thread 处理了此事件后，会将其清除。

#### 构造 JobProgressListener

我们以 JobProgressListener 为例来讲解 SparkListener。 JobProgressListener 是 SparkContext
中一个重要的组成部分，通过监听 listenerBus 中的事件更新任务进度。 SparkStatusTracker 和
SparkUI 实际上也是通过 JobProgressListener 来实现任务状态跟踪的。

```scala
private[spark] val jobProgressListener = new JobProgressListener(conf)
listenerBus.addListener(jobProgressListener)
val statusTracker = new SparkStatusTracker(this)
```



JobProgressListener 的作用是通过 HashMap、 ListBuffer 等数据结构存储 JobId 及对应的
JobUIData 信息，并按照激活、完成、失败等 job 状态统计。对于 StageId、 StageInfo 等信息按
照激活、完成、忽略、失败等 Stage 状态统计，并且存储 StageId 与 JobId 的一对多关系。这
些统计信息最终会被 JobPage 和 StagePage 等页面访问和渲染。 

 JobProgressListener 实现了 onJobStart、 onJobEnd、 onStageCompleted、onStageSubmitted、onTaskStart、 onTaskEnd 等方法，这些方法正是在 listenerBus 的驱动下，改变JobProgressListener 中的各种 Job、 Stage 相关的数据。

#### SparkUI 的创建与初始化



如果不需要提供 SparkUI 服务，可以将属性 spark.ui.enabled 修改为 false。其中
createLiveUI 实际是调用了 create 方法



在 create 方法里除了 JobProgressListener 是外部传入的之外，又增加了一些 SparkListener。例如，用于对 JVM 参数、 Spark 属性、 Java 系统属性、 classpath等进行监控的EnvironmentListener ；用于维护 Executor 的存储状态的 StorageStatusListener ；用于准备将 Executor 的信息展示在 ExecutorsTab 的 ExecutorsListener ；用于准备将 Executor 相关存储信息展示在 BlockManagerUI 的 StorageListener 等。最后创建 SparkUI， Spark UI 服务默认是可以被杀掉的，通过修改属性 spark.ui.killEnabled 为 false 可以保证不被杀死。 initialize 方法会组织前端页面各个 Tab 和 Page 的展示及布局

#### Spark UI 的页面布局与展示



###  Hadoop 相关配置及 Executor 环境变量的设置；
### 创建任务调度 TaskScheduler；

> 负责任务的提交，并且请求集群管理器对任务调度。TaskScheduler 也可以看做任务调度的客户端。createTaskScheduler 方法会根据 master 的配置匹配部署模式，创建 TaskSchedulerImpl，并生成不同的 SchedulerBackend。

```scala
master match {
    case "local" =>
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalBackend(scheduler, 1)
        scheduler.initialize(backend)
        (backend, scheduler)
```

创建任务调度器 TaskScheduler

TaskSchedulerImpl 的构造过程如下：
1）从 SparkConf 中读取配置信息，包括每个任务分配的 CPU 数、调度模式（调度模式有
FAIR 和 FIFO 两种，默认为 FIFO，可以修改属性 spark.scheduler.mode 来改变）等。

2）创建 TaskResultGetter，它的作用是通过线程池（Executors.newFixedThreadPool 创建
的，默认 4 个线程，线程名字以 task-result-getter 开头，线程工厂默认是 Executors.defaultThreadFactory）对 Worker 上的 Executor 发送的 Task 的执行结果进行处理。

TaskSchedulerImpl 的 调 度 模 式 有 FAIR 和 FIFO 两 种。 任 务 的 最 终 调 度 实 际 都 是 落
实 到 接 口 SchedulerBackend 的 具 体 实 现 上 的。 



创 建 完 TaskSchedulerImpl 和 LocalBackend 后， 对 TaskSchedulerImpl 调 用 方 法 initialize
进行初始化。以默认的 FIFO 调度为例， TaskSchedulerImpl 的初始化过程如下：
1）使 TaskSchedulerImpl 持有 LocalBackend 的引用。
2）创建 Pool， Pool 中缓存了调度队列、调度算法及 TaskSetManager 集合等信息。
3）创建 FIFOSchedulableBuilder， FIFOSchedulableBuilder 用来操作 Pool 中的调度队列。





6）创建和启动 DAGScheduler；

>DAGScheduler 主要用于在任务正式交给 TaskSchedulerImpl 提交之前做一些准备工作，
>包括：创建 Job，将 DAG 中的 RDD 划分到不同的 Stage，提交 Stage，等等。DAGScheduler 的数据结构主要维护 jobId 和 stageId 的关系、 Stage、 ActiveJob，以及缓存
>的 RDD 的 partitions 的位置信息

7） TaskScheduler 的启动；
8）初始化块管理器 BlockManager（BlockManager 是存储体系的主要组件之一，将在第 4
章介绍）；
9）启动测量系统 MetricsSystem；
10）创建和启动 Executor 分配管理器 ExecutorAllocationManager；
11） ContextCleaner 的创建与启动；
12） Spark 环境更新；
13）创建 DAGSchedulerSource 和 BlockManagerSource；
14）将 SparkContext 标记为激活。

```scala
/**
Main entry point for Spark functionality. A SparkContext represents the connection to a Spark cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before creating a new one.  This limitation may eventually be removed; 
 **/


```



创建 SparkEnv 主要使用 SparkEnv 的createDriverEnv， SparkEnv.createDriverEnv 方法有三个参数： conf、 isLocal 和 listenerBus。

SparkEnv 的方法 createDriverEnv 最终调用 create 创建 SparkEnv。 SparkEnv 的构造步骤如下：
1）创建安全管理器 SecurityManager；
2）创建基于 Akka 的分布式消息系统 ActorSystem；
3）创建 Map 任务输出跟踪器 mapOutputTracker；

>  跟踪 map 阶段任务的输出状态

4）实例化 ShuffleManager；

> ShuffleManager 负 责 管 理 本 地 及 远 程 的 block 数 据 的 shuffle 操 作。 



5）创建 ShuffleMemoryManager；
6）创建块传输服务 BlockTransferService；
7）创建 BlockManagerMaster；
8）创建块管理器 BlockManager；
9）创建广播管理器 BroadcastManager；
10）创建缓存管理器 CacheManager；
11）创建 HTTP 文件服务器 HttpFileServer；
12）创建测量系统 MetricsSystem；
13）创建 SparkEnv。