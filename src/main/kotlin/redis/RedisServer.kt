package redis

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.slf4j.LoggerFactory
import kotlin.jvm.java

class RedisServer(
    private val port: Int = 6379,
) {
    private val logger = LoggerFactory.getLogger(RedisServer::class.java)

    private val useEpoll = Epoll.isAvailable()
    private val bossGroup: EventLoopGroup = if (useEpoll) EpollEventLoopGroup(1) else NioEventLoopGroup(1)
    private val workerGroup: EventLoopGroup = if (useEpoll) EpollEventLoopGroup() else NioEventLoopGroup()
    private val serverChannelClass: Class<out ServerSocketChannel> =
        if (useEpoll) EpollServerSocketChannel::class.java else NioServerSocketChannel::class.java
    private var channel: Channel? = null

    fun start() {
        logger.info("Using ${if (useEpoll) "Epoll" else "NIO"} transport")
        try {
            val bootstrap = ServerBootstrap()
            bootstrap
                .group(bossGroup, workerGroup)
                .channel(serverChannelClass)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(RedisServerInitializer())

            channel = bootstrap.bind(port).sync().channel()
            logger.info("Redis server started on port $port")

            channel?.closeFuture()?.sync()
        } finally {
            shutdown()
        }
    }

    fun shutdown() {
        logger.info("Shutting down redis server...")
        channel?.close()
        workerGroup.shutdownGracefully()
        bossGroup.shutdownGracefully()
        logger.info("Redis server stopped")
    }
}
