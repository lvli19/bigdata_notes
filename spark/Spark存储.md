# Spark存储



## shuffle服务端与客户端



### 传输上下文TransportContext

```scala
/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */

public TransportContext(
    TransportConf conf,
    RpcHandler rpcHandler,
    boolean closeIdleConnections) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;
}
```

TransportConf: 主要控制netty框架提供的shuffle的I/O交互的客户端和服务器端线程数量；

RpcHandler: 负责shuffleI/O服务器在接收到客户端RPC请求后，提供打开Block或者上传Block的RPC处理，此处即NettyBlockRpcServer

Decoder:

Encoder