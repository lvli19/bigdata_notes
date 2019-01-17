# ;RDD计算模型



## 1.RDD设计背景

在实际应用中，存在许多迭代式算法（比如机器学习、图算法等）和交互式数据挖掘工具，这些应用场景的共同之处是，不同计算阶段之间会重用中间结果，即一个阶段的输出结果会作为下一个阶段的输入。但是，目前的MapReduce框架都是把中间结果写入到HDFS中，带来了大量的数据复制、磁盘IO和序列化开销。虽然，类似Pregel等图计算框架也是将结果保存在内存当中，但是，这些框架只能支持一些特定的计算模式，并没有提供一种通用的数据抽象。RDD就是为了满足这种需求而出现的，它提供了一个抽象的数据架构，我们不必担心底层数据的分布式特性，只需将具体的应用逻辑表达为一系列转换处理，不同RDD之间的转换操作形成依赖关系，可以实现管道化，从而避免了中间结果的存储，大大降低了数据复制、磁盘IO和序列化开销。

- 高效进行数据共享
- 提供粗粒度变换（map，filter，join…)的接口，将相同操作应用到多个数据集



## 2.RDD概念

一个RDD就是一个分布式对象集合，本质上是一个只读的分区记录集合，每个RDD可以分成多个分区，每个分区就是一个数据集片段，并且一个RDD的不同分区可以被保存到集群中不同的节点上，从而可以在集群中的不同节点上进行并行计算。RDD提供了一种高度受限的共享内存模型，即RDD是只读的记录分区的集合，不能直接修改，只能基于稳定的物理存储中的数据集来创建RDD，或者通过在其他RDD上执行确定的转换操作（如map、join和groupBy）而创建得到新的RDD。RDD提供了一组丰富的操作以支持常见的数据运算，分为“行动”（Action）和“转换”（Transformation）两种类型，前者用于执行计算并指定输出的形式，后者指定RDD之间的相互依赖关系。两类操作的主要区别是，转换操作（比如map、filter、groupBy、join等）接受RDD并返回RDD，而行动操作（比如count、collect等）接受RDD但是返回非RDD（即输出一个值或结果）。RDD提供的转换接口都非常简单，都是类似map、filter、groupBy、join等粗粒度的数据转换操作，而不是针对某个数据项的细粒度修改。因此，RDD比较适合对于数据集中元素执行相同操作的批处理式应用，而不适合用于需要异步、细粒度状态的应用，比如Web应用系统、增量式的网页爬虫等。正因为这样，这种粗粒度转换接口设计，会使人直觉上认为RDD的功能很受限、不够强大。但是，实际上RDD已经被实践证明可以很好地应用于许多并行计算应用中，可以具备很多现有计算框架（比如MapReduce、SQL、Pregel等）的表达能力，并且可以应用于这些框架处理不了的交互式数据挖掘应用。
Spark用Scala语言实现了RDD的API，程序员可以通过调用API实现对RDD的各种操作。RDD典型的执行过程如下：
\1. RDD读入外部数据源（或者内存中的集合）进行创建；
\2. RDD经过一系列的“转换”操作，每一次都会产生不同的RDD，供给下一个“转换”使用；
\3. 最后一个RDD经“行动”操作进行处理，并输出到外部数据源（或者变成Scala集合或标量）。
需要说明的是，RDD采用了惰性调用，即在RDD的执行过程中（如图9-8所示），真正的计算发生在RDD的“行动”操作，对于“行动”之前的所有“转换”操作，Spark只是记录下“转换”操作应用的一些基础数据集以及RDD生成的轨迹，即相互之间的依赖关系，而不会触发真正的计算。

![图9-8 Spark的转换和行动操作](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-8-Spark%E7%9A%84%E8%BD%AC%E6%8D%A2%E5%92%8C%E8%A1%8C%E5%8A%A8%E6%93%8D%E4%BD%9C.jpg)
图9-8 Spark的转换和行动操作

例如，在图9-9中，从输入中逻辑上生成A和C两个RDD，经过一系列“转换”操作，逻辑上生成了F（也是一个RDD），之所以说是逻辑上，是因为这时候计算并没有发生，Spark只是记录了RDD之间的生成和依赖关系。当F要进行输出时，也就是当F进行“行动”操作的时候，Spark才会根据RDD的依赖关系生成DAG，并从起点开始真正的计算。

![图9-9  RDD执行过程的一个实例](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-9-RDD%E6%89%A7%E8%A1%8C%E8%BF%87%E7%A8%8B%E7%9A%84%E4%B8%80%E4%B8%AA%E5%AE%9E%E4%BE%8B.jpg)
图9-9 RDD执行过程的一个实例

上述这一系列处理称为一个“血缘关系（Lineage）”，即DAG拓扑排序的结果。采用惰性调用，通过血缘关系连接起来的一系列RDD操作就可以实现管道化（pipeline），避免了多次转换操作之间数据同步的等待，而且不用担心有过多的中间数据，因为这些具有血缘关系的操作都管道化了，一个操作得到的结果不需要保存为中间数据，而是直接管道式地流入到下一个操作进行处理。同时，这种通过血缘关系把一系列操作进行管道化连接的设计方式，也使得管道中每次操作的计算变得相对简单，保证了每个操作在处理逻辑上的单一性；相反，在MapReduce的设计中，为了尽可能地减少MapReduce过程，在单个MapReduce中会写入过多复杂的逻辑。
例1：一个Spark的“Hello World”程序
这里以一个“Hello World”入门级Spark程序来解释RDD执行过程，这个程序的功能是读取一个HDFS文件，计算出包含字符串“Hello World”的行数。

```
val sc= new SparkContext(“spark://localhost:7077”,”Hello World”, “YOUR_SPARK_HOME”,”YOUR_APP_JAR”)
val fileRDD = sc.textFile(“hdfs://192.168.0.103:9000/examplefile”)
val filterRDD = fileRDD.filter(_.contains(“Hello World”))
filterRDD.cache()
filterRDD.count()
```

可以看出，一个Spark应用程序，基本是基于RDD的一系列计算操作。第1行代码用于创建SparkContext对象；第2行代码从HDFS文件中读取数据创建一个RDD；第3行代码对fileRDD进行转换操作得到一个新的RDD，即filterRDD；第4行代码表示对filterRDD进行持久化，把它保存在内存或磁盘中（这里采用cache接口把数据集保存在内存中），方便后续重复使用，当数据被反复访问时（比如查询一些热点数据，或者运行迭代算法），这是非常有用的，而且通过cache()可以缓存非常大的数据集，支持跨越几十甚至上百个节点；第5行代码中的count()是一个行动操作，用于计算一个RDD集合中包含的元素个数。这个程序的执行过程如下：
*  创建这个Spark程序的执行上下文，即创建SparkContext对象；
*  从外部数据源（即HDFS文件）中读取数据创建fileRDD对象；
*  构建起fileRDD和filterRDD之间的依赖关系，形成DAG图，这时候并没有发生真正的计算，只是记录转换的轨迹；
*  执行到第5行代码时，count()是一个行动类型的操作，触发真正的计算，开始实际执行从fileRDD到filterRDD的转换操作，并把结果持久化到内存中，最后计算出filterRDD中包含的元素个数。

## 3.RDD特性

总体而言，Spark采用RDD以后能够实现高效计算的主要原因如下：
（1）高效的容错性。现有的分布式共享内存、键值存储、内存数据库等，为了实现容错，必须在集群节点之间进行数据复制或者记录日志，也就是在节点之间会发生大量的数据传输，这对于数据密集型应用而言会带来很大的开销。在RDD的设计中，数据只读，不可修改，如果需要修改数据，必须从父RDD转换到子RDD，由此在不同RDD之间建立了血缘关系。所以，RDD是一种天生具有容错机制的特殊集合，不需要通过数据冗余的方式（比如检查点）实现容错，而只需通过RDD父子依赖（血缘）关系重新计算得到丢失的分区来实现容错，无需回滚整个系统，这样就避免了数据复制的高开销，而且重算过程可以在不同节点之间并行进行，实现了高效的容错。此外，RDD提供的转换操作都是一些粗粒度的操作（比如map、filter和join），RDD依赖关系只需要记录这种粗粒度的转换操作，而不需要记录具体的数据和各种细粒度操作的日志（比如对哪个数据项进行了修改），这就大大降低了数据密集型应用中的容错开销；
（2）中间结果持久化到内存。数据在内存中的多个RDD操作之间进行传递，不需要“落地”到磁盘上，避免了不必要的读写磁盘开销；
（3）存放的数据可以是Java对象，避免了不必要的对象序列化和反序列化开销。

## 4. RDD之间的依赖关系

RDD中不同的操作会使得不同RDD中的分区会产生不同的依赖。RDD中的依赖关系分为窄依赖（Narrow Dependency）与宽依赖（Wide Dependency），图9-10展示了两种依赖之间的区别。
窄依赖表现为一个父RDD的分区对应于一个子RDD的分区，或多个父RDD的分区对应于一个子RDD的分区；比如图9-10(a)中，RDD1是RDD2的父RDD，RDD2是子RDD，RDD1的分区1，对应于RDD2的一个分区（即分区4）；再比如，RDD6和RDD7都是RDD8的父RDD，RDD6中的分区（分区15）和RDD7中的分区（分区18），两者都对应于RDD8中的一个分区（分区21）。
宽依赖则表现为存在一个父RDD的一个分区对应一个子RDD的多个分区。比如图9-10(b)中，RDD9是RDD12的父RDD，RDD9中的分区24对应了RDD12中的两个分区（即分区27和分区28）。
总体而言，如果父RDD的一个分区只被一个子RDD的一个分区所使用就是窄依赖，否则就是宽依赖。窄依赖典型的操作包括map、filter、union等，宽依赖典型的操作包括groupByKey、sortByKey等。对于连接（join）操作，可以分为两种情况。
（1）对输入进行协同划分，属于窄依赖（如图9-10(a)所示）。所谓协同划分（co-partitioned）是指多个父RDD的某一分区的所有“键（key）”，落在子RDD的同一个分区内，不会产生同一个父RDD的某一分区，落在子RDD的两个分区的情况。
（2）对输入做非协同划分，属于宽依赖，如图9-10(b)所示。
对于窄依赖的RDD，可以以流水线的方式计算所有父分区，不会造成网络之间的数据混合。对于宽依赖的RDD，则通常伴随着Shuffle操作，即首先需要计算好所有父分区数据，然后在节点之间进行Shuffle。

![图9-10 窄依赖与宽依赖的区别](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-10-%E7%AA%84%E4%BE%9D%E8%B5%96%E4%B8%8E%E5%AE%BD%E4%BE%9D%E8%B5%96%E7%9A%84%E5%8C%BA%E5%88%AB.jpg)
图9-10 窄依赖与宽依赖的区别

Spark的这种依赖关系设计，使其具有了天生的容错性，大大加快了Spark的执行速度。因为，RDD数据集通过“血缘关系”记住了它是如何从其它RDD中演变过来的，血缘关系记录的是粗颗粒度的转换操作行为，当这个RDD的部分分区数据丢失时，它可以通过血缘关系获取足够的信息来重新运算和恢复丢失的数据分区，由此带来了性能的提升。相对而言，在两种依赖关系中，窄依赖的失败恢复更为高效，它只需要根据父RDD分区重新计算丢失的分区即可（不需要重新计算所有分区），而且可以并行地在不同节点进行重新计算。而对于宽依赖而言，单个节点失效通常意味着重新计算过程会涉及多个父RDD分区，开销较大。此外，Spark还提供了数据检查点和记录日志，用于持久化中间RDD，从而使得在进行失败恢复时不需要追溯到最开始的阶段。在进行故障恢复时，Spark会对数据检查点开销和重新计算RDD分区的开销进行比较，从而自动选择最优的恢复策略。

## 5.阶段的划分

Spark通过分析各个RDD的依赖关系生成了DAG，再通过分析各个RDD中的分区之间的依赖关系来决定如何划分阶段，具体划分方法是：在DAG中进行反向解析，遇到宽依赖就断开，遇到窄依赖就把当前的RDD加入到当前的阶段中；将窄依赖尽量划分在同一个阶段中，可以实现流水线计算（具体的阶段划分算法请参见AMP实验室发表的论文《Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing》）。例如，如图9-11所示，假设从HDFS中读入数据生成3个不同的RDD（即A、C和E），通过一系列转换操作后再将计算结果保存回HDFS。对DAG进行解析时，在依赖图中进行反向解析，由于从RDD A到RDD B的转换以及从RDD B和F到RDD G的转换，都属于宽依赖，因此，在宽依赖处断开后可以得到三个阶段，即阶段1、阶段2和阶段3。可以看出，在阶段2中，从map到union都是窄依赖，这两步操作可以形成一个流水线操作，比如，分区7通过map操作生成的分区9，可以不用等待分区8到分区9这个转换操作的计算结束，而是继续进行union操作，转换得到分区13，这样流水线执行大大提高了计算的效率。

![图9-11根据RDD分区的依赖关系划分阶段](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-11-%E6%A0%B9%E6%8D%AERDD%E5%88%86%E5%8C%BA%E7%9A%84%E4%BE%9D%E8%B5%96%E5%85%B3%E7%B3%BB%E5%88%92%E5%88%86%E9%98%B6%E6%AE%B5.jpg)
图9-11根据RDD分区的依赖关系划分阶段

由上述论述可知，把一个DAG图划分成多个“阶段”以后，每个阶段都代表了一组关联的、相互之间没有Shuffle依赖关系的任务组成的任务集合。每个任务集合会被提交给任务调度器（TaskScheduler）进行处理，由任务调度器将任务分发给Executor运行。

## 5.RDD运行过程

通过上述对RDD概念、依赖关系和阶段划分的介绍，结合之前介绍的Spark运行基本流程，这里再总结一下RDD在Spark架构中的运行过程（如图9-12所示）：
（1）创建RDD对象；
（2）SparkContext负责计算RDD之间的依赖关系，构建DAG；
（3）DAGScheduler负责把DAG图分解成多个阶段，每个阶段中包含了多个任务，每个任务会被任务调度器分发给各个工作节点（Worker Node）上的Executor去执行。

![图9-12 RDD在Spark中的运行过程](http://dblab.xmu.edu.cn/blog/wp-content/uploads/2016/11/%E5%9B%BE9-12-RDD%E5%9C%A8Spark%E4%B8%AD%E7%9A%84%E8%BF%90%E8%A1%8C%E8%BF%87%E7%A8%8B.jpg)
图9-12 RDD在Spark中的运行过程



# RDD操作

### 1. 创建

第一种：读取一个外部数据集, 从文件系统中加载数据创建RDD

#### 从HDFS读取

采用textFile()方法来从文件系统中加载数据创建RDD，该方法把文件的URI作为参数，这个URI可以是本地文件系统的地址，或者是分布式文件系统HDFS的地址，或者是Amazon S3的地址等等。

从HDFS中加载word.txt文件，并显示第一行文本内容：

```scala
scala> val textFile = sc.textFile("hdfs://localhost:9000/user/hadoop/word.txt")
scala> textFile.first()
```

#### 从本地文本

把本地文件系统中的文本文件加载到RDD中的语句如下：

```scala
scala> val lines = sc.textFile("hdfs://localhost:9000/user/hadoop/word.txt")
scala> val lines = sc.textFile("/user/hadoop/word.txt")
scala> val lines = sc.textFile("word.txt")
```

#### 读写HBase数据

[读写HBase数据](http://dblab.xmu.edu.cn/blog/1316-2/)

- 第二种：调用SparkContext的parallelize方法，在Driver中一个已经存在的集合（数组）上创建。

[文件数据读写](http://dblab.xmu.edu.cn/blog/1314-2/)

## 

### 2. 转换操作

对于RDD而言，每一次转换操作都会产生不同的RDD，供给下一个“转换”使用。转换得到的RDD是惰性求值的，也就是说，整个转换过程只是记录了转换的轨迹，并不会发生真正的计算，只有遇到行动操作时，才会发生真正的计算，开始从血缘关系源头开始，进行物理的转换操作。
下面列出一些常见的转换操作（Transformation API）：

* filter(func)：筛选出满足函数func的元素，并返回一个新的数据集
* map(func)：将每个元素传递到函数func中，并将结果返回为一个新的数据集
* flatMap(func)：与map()相似，但每个输入元素都可以映射到0或多个输出结果
* groupByKey()：应用于(K,V)键值对的数据集时，返回一个新的(K, Iterable)形式的数据集
* reduceByKey(func)：应用于(K,V)键值对的数据集时，返回一个新的(K, V)形式的数据集，其中的每个值是将每个key传递到函数func中进行聚合

### 3. 控制操作

进行RDD持久化，让rdd按照不同存储策略保存在内存或磁盘中

### 4. 行动操作

行动操作是真正触发计算的地方。Spark程序执行到行动操作时，才会执行真正的计算，从文件中加载数据，完成一次又一次转换操作，最终，完成行动操作得到结果。

下面列出一些常见的行动操作（Action API）：

* count() 返回数据集中的元素个数
* collect() 以数组的形式返回数据集中的所有元素
* first() 返回数据集中的第一个元素
* take(n) 以数组的形式返回数据集中的前n个元素
* reduce(func) 通过函数func（输入两个参数并返回一个值）聚合数据集中的元素
* foreach(func) 将数据集中的每个元素传递到函数func中运行*

分为两类

- 操作结果变为scala集合或变量
- 将rdd保存在文件系统/数据库







## RDD实现

### 编程接口

通用接口抽象rdd

- 分区信息
- 依赖关系
- 函数
- 划分策略/数据的元信息

|      | 操作                    | 含义                                       |
| ---- | ----------------------- | ------------------------------------------ |
|      | Partitions()            | 返回分片对象列表                           |
|      | PreferredLocations(p)   | 根据数据本地特性，列出分片p的首选位置      |
|      | Dependencies()          | 返回依赖列表                               |
|      | Iterator(p,parentIters) | 给定p的父分片的迭代器，计算分片p的元素     |
|      | Parititioner()          | 返回说明rdd是否是hash或者range分片的元数据 |

### 作业调度



### Dependency

NarrowDependency, OneToOneDependency, RangeDependency, ShuffleDependency





Spark中的两个重要抽象是RDD和共享变量。上一章我们已经介绍了RDD，这里介绍共享变量。

在默认情况下，当Spark在集群的多个不同节点的多个任务上并行运行一个函数时，它会把函数中涉及到的每个变量，在每个任务上都生成一个副本。但是，有时候，需要在多个任务之间共享变量，或者在任务（Task）和任务控制节点（Driver Program）之间共享变量。为了满足这种需求，Spark提供了两种类型的变量：广播变量（broadcast variables）和累加器（accumulators）。广播变量用来把变量在所有节点的内存之间进行共享。累加器则支持在所有不同节点之间进行累加计算（比如计数或者求和）。



## RDD计算模型

RDD可以看做是对各种数据计算模型的统一抽象，Spark的计算过程主要是RDD的迭代计算过程，如图2-6所示。RDD的迭代计算过程非常类似于管道。分区数量取决于Partition数量的设定，每个分区的数据只会在一个Task中计算。所有分区可以在多个机器节点的Executor上并行执行。

![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/2018052711441980.jpg)

图2-6   RDD计算模型

图2-6只是简单的从分区的角度将RDD的计算看作是管道，如果从RDD的血缘关系、Stage划分的角度来看，由RDD构成的DAG经过DAGScheduler调度后，将变成图2-7所示的样子。

![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/20180527114517534.png)



图2-7   DAGScheduler对由RDD构成的DAG进行调度

图2-7中共展示了A、B、C、D、E、F、G一共七个RDD。每个RDD中的小方块代表一个分区，将会有一个Task处理此分区的数据。

RDD A经过groupByKey转换后得到RDD B。

RDD C经过map转换后得到RDD D。

RDD D和RDD E经过union转换后得到RDD F。

RDD B和RDD F经过join转换后得到RDD G。

从图中可以看到map和union生成的RDD与其上游RDD之间的依赖是NarrowDependency，而groupByKey和join生成的RDD与其上游的RDD之间的依赖是ShuffleDependency。由于DAGScheduler按照ShuffleDependency作为Stage的划分的依据，因此A被划入了ShuffleMapStage 1；C、D、E、F被划入了ShuffleMapStage 2；B和G被划入了ResultStage 3。