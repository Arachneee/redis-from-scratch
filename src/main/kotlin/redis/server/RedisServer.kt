package redis.server

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
import redis.config.RedisConfig
import redis.storage.KeyExpirationScheduler
import redis.storage.OperationsBundle
import java.util.concurrent.TimeUnit

class RedisServer(
    private val config: RedisConfig = RedisConfig(),
    private val ops: OperationsBundle = OperationsBundle.create(),
    private val keyExpirationScheduler: KeyExpirationScheduler = KeyExpirationScheduler(ops.key, config),
) {
    private val logger = LoggerFactory.getLogger(RedisServer::class.java)

    private val useEpoll = Epoll.isAvailable()
    private val bossGroup: EventLoopGroup = createEventLoopGroup()
    private val workerGroup: EventLoopGroup = createEventLoopGroup()
    private val serverChannelClass: Class<out ServerSocketChannel> =
        if (useEpoll) EpollServerSocketChannel::class.java else NioServerSocketChannel::class.java
    private var channel: Channel? = null

    fun start() {
        logger.info("Using ${if (useEpoll) "Epoll" else "NIO"} transport (single-threaded)")
        try {
            val bootstrap = ServerBootstrap()
            bootstrap
                .group(bossGroup, workerGroup)
                .channel(serverChannelClass)
                .option(ChannelOption.SO_BACKLOG, config.soBacklog)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(RedisServerInitializer(ops, config))

            channel = bootstrap.bind(config.port).sync().channel()
            logger.info("Redis server started on port ${config.port}")

            keyExpirationScheduler.start(workerGroup)

            channel?.closeFuture()?.sync()
        } finally {
            shutdown()
        }
    }

    fun shutdown() {
        logger.info("Shutting down redis server...")
        channel?.close()?.sync()
        workerGroup
            .shutdownGracefully(
                config.shutdownQuietPeriodMs,
                config.shutdownTimeoutMs,
                TimeUnit.MILLISECONDS,
            ).sync()
        bossGroup
            .shutdownGracefully(
                config.shutdownQuietPeriodMs,
                config.shutdownTimeoutMs,
                TimeUnit.MILLISECONDS,
            ).sync()
        logger.info("Redis server stopped")
    }

    private fun createEventLoopGroup(): EventLoopGroup = if (useEpoll) EpollEventLoopGroup(1) else NioEventLoopGroup(1)
}
