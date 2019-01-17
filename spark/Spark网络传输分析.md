# Spark网络传输分析

[Spark 底层网络模块](http://sharkdtu.com/posts/spark-network.html)

[Spark2.1.0——內置RPC框架詳解](https://hk.saowen.com/a/9ed91a1877ddb80b2846a612af1ff5645af71aa4ef7d73c757263798659c1906)

[Spark內置框架rpc通訊機制及RpcEnv基礎設施-Spark商業環境實戰](https://hk.saowen.com/a/f29bcadbcefc909c27daabd66018f6953cf9c7c8fab028e7b8ff9f043563b93b)



## Spark内置RPC框架的基本架构

![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/816981-20181016145505263-870721231.jpg)

TransportContext内部包含传输上下文的配置信息TransportConf和对客户端请求消息进行处理的RpcHandler。TransportConf在创建TransportClientFactory和TransportServer时都是必须的，而RpcHandler只用于创建TransportServer。TransportClientFactory是RPC客户端的工厂类。TransportServer是RPC服务端的实现。

### RPC框架的各个组件

- **TransportContext** 传输上下文，包含了用于创建传输服务端（TransportServer）和传输客户端工厂（TransportClientFactory）的上下文信息，并支持使用TransportChannelHandler设置Netty提供的SocketChannel的Pipeline的实现。

- **TransportConf**  传输上下文的配置信息。

- **RpcHandler** 对调用传输客户端（TransportClient）的sendRPC方法发送的消息进行处理的程序。

- **MessageEncoder** 在将消息放入管道前，先对消息内容进行编码，防止管道另一端读取时丢包和解析错误。
- **MessageDecoder** 对从管道中读取的ByteBuf进行解析，防止丢包和解析错误；

- **TransportFrameDecoder** 对从管道中读取的ByteBuf按照数据帧进行解析；
- **RpcResponseCallback** RpcHandler对请求的消息处理完毕后，进行回调的接口。
- **TransportClientFactory** 创建传输客户端（TransportClient）的传输客户端工厂类。
- **ClientPool** 在两个对等节点间维护的关于传输客户端（TransportClient）的池子。ClientPool是TransportClientFactory的内部组件。

**TransportClient** RPC框架的客户端，用于获取预先协商好的流中的连续块。TransportClient旨在允许有效传输大量数据，这些数据将被拆分成几百KB到几MB的块。当TransportClient处理从流中获取的获取的块时，实际的设置是在传输层之外完成的。sendRPC方法能够在客户端和服务端的同一水平线的通信进行这些设置。

**TransportClientBootstrap** 当服务端响应客户端连接时在客户端执行一次的引导程序。
**TransportRequestHandler** 用于处理客户端的请求并在写完块数据后返回的处理程序。
**TransportResponseHandler** 用于处理服务端的响应，并且对发出请求的客户端进行响应的处理程序。
**TransportChannelHandler** ：代理由TransportRequestHandler处理的请求和由TransportResponseHandler处理的响应，并加入传输层的处理。
**TransportServerBootstrap**：当客户端连接到服务端时在服务端执行一次的引导程序。

**TransportServer**：RPC框架的服务端，提供高效的、低级别的流服务。



#### RPC配置TransportConf

TransportContext中的TransportConf给Spark的RPC框架提供配置信息，它有两个成员属性——配置提供者conf和配置的模块名称module。这两个属性的定义如下：

private final ConfigProvider conf;

private final String module;

Spark通常使用SparkTransportConf创建TransportConf



代码清单2  SparkTransportConf的实现

```scala
object SparkTransportConf {
  private val MAX_DEFAULT_NETTY_THREADS = 8
  def fromSparkConf(_conf: SparkConf, module: String, numUsableCores: Int = 0): TransportConf = {
    val conf = _conf.clone
    val numThreads = defaultNumThreads(numUsableCores)
    conf.setIfMissing(s"spark.$module.io.serverThreads", numThreads.toString)
    conf.setIfMissing(s"spark.$module.io.clientThreads", numThreads.toString)
 
    new TransportConf(module, new ConfigProvider {
      override def get(name: String): String = conf.get(name)
    })
  }
  private def defaultNumThreads(numUsableCores: Int): Int = {
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    math.min(availableCores, MAX_DEFAULT_NETTY_THREADS)
  }
}
```

从代码清单2看到，可以使用SparkTransportConf的fromSparkConf方法来构造TransportConf。传递的三个参数分别为SparkConf、模块名module及可用的内核数numUsableCores。如果numUsableCores小于等于0，那么线程数是系统可用处理器的数量，不过系统的内核数不可能全部用于网络传输使用，所以这里还将分配给网络传输的内核数量最多限制在8个。最终确定的线程数将被用于设置客户端传输线程数（spark.module.io.clientThreads属性）和服务端传输线程数（spark.module.io.serverThreads属性）。fromSparkConf最终构造TransportConf对象时传递的ConfigProvider为实现了get方法的匿名的内部类，get的实现实际是代理了SparkConf的get方法.



## RPC客户端工廠TransportClientFactory

TransportContext的createClientFactory方法可以創建TransportClientFactory的實例.

TransportClientFactory構造器中的各個變量分別為：

- context：即參數傳遞的TransportContext的引用；
- conf：即TransportConf，這裏通過調用TransportContext的getConf獲取；
- clientBootstraps：即參數傳遞的TransportClientBootstrap列表；
- connectionPool：即針對每個Socket地址的連接池ClientPool的緩存；connectionPool的數據結構較為複雜，為便於讀者理解，這裏以圖2來表示connectionPool的數據結構。
- numConnectionsPerPeer：即從TransportConf獲取的key為”spark.+模塊名+.io.numConnectionsPerPeer”的屬性值。此屬性值用於指定對等節點間的連接數。這裏的模塊名實際為TransportConf的module字段，Spark的很多組件都利用RPC框架構建，它們之間按照模塊名區分，例如RPC模塊的key為“spark.rpc.io.numConnectionsPerPeer”；
- rand：對Socket地址對應的連接池ClientPool中緩存的TransportClient進行隨機選擇，對每個連接做負載均衡；
- ioMode：IO模式，即從TransportConf獲取key為”spark.+模塊名+.io.mode”的屬性值。默認值為NIO，Spark還支持EPOLL；
- socketChannelClass：客户端Channel被創建時使用的類，通過ioMode來匹配，默認為NioSocketChannel，Spark還支持EpollEventLoopGroup；
- workerGroup：根據Netty的規範，客户端只有worker組，所以此處創建workerGroup。workerGroup的實際類型是NioEventLoopGroup；
- pooledAllocator ：彙集ByteBuf但對本地線程緩存禁用的分配器。

![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/20180710100300998.jpg)

![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/816981-20181103200349095-1502690183.jpg)

管道處理請求和響應的流程圖



![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/816981-20181103205227870-118958154.jpg)

![](https://raw.githubusercontent.com/seaside2mm/github-photos/master/816981-20181103210752867-1302315006.jpg)





消息抽象
--------------------- 

总结起来，Spark中定义三种类型的消息：RPC消息、ChunkFetch消息以及Stream消息。Message是这些消息的抽象接口，它定义了三个关键接口，分别得到消息类型、消息体以及判断消息体是否编码在header中，消息体统一由ManagedBuffer表示，ManagedBuffer抽象了JAVA NIO ByteBuffer、Netty ByteBuf以及File Segment，所以无论是ByteBuffer、ByteBuf还是File Segment，都可以表示为ManagedBuffer。如下图列出所有spark中涉及到的具体消息，下面分别详细阐述各种消息。

[![spark-network-protocol](http://sharkdtu.com/images/spark-network-protocol.png)](http://sharkdtu.com/images/spark-network-protocol.png)

- RPC消息

  > 用于抽象所有spark中涉及到RPC操作时需要传输的消息，通常这类消息很小，一般都是些控制类消息。包括RpcRequest、OneWayMessage、RpcResponse以及RpcFailure四种消息。

- ChunkFetch消息

  > 用于抽象所有spark中涉及到数据拉取操作时需要传输的消息，它用于shuffle数据以及RDD Block数据传输。ChunkFetch消息包括ChunkFetchRequest、ChunkFetchSuccess以及ChunkFetchFailure三种消息。

- Stream消息

  > 主要用于driver到executor传输jar、file文件等。Stream消息包括StreamRequest、StreamResponse以及StreamFailure三种消息



一个message从被发送到被接收需要经过”MessageEncoder->网络->TransportFrameDecoder->MessageDecoder”，下面按照这一过程详细阐述各handler的作用。

[![spark-network-handler](http://sharkdtu.com/images/spark-network-handler.png)](http://sharkdtu.com/images/spark-network-handler.png)