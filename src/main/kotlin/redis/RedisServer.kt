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
import java.util.concurrent.TimeUnit

class RedisServer(
    private val port: Int = DEFAULT_PORT,
    private val repository: RedisRepository = RedisRepository(),
    private val keyExpirationScheduler: KeyExpirationScheduler = KeyExpirationScheduler(repository),
    private val shutdownQuietPeriodMs: Long = DEFAULT_SHUTDOWN_QUIET_PERIOD_MS,
    private val shutdownTimeoutMs: Long = DEFAULT_SHUTDOWN_TIMEOUT_MS,
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
                .option(ChannelOption.SO_BACKLOG, SO_BACKLOG)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(RedisServerInitializer(repository))

            channel = bootstrap.bind(port).sync().channel()
            logger.info("Redis server started on port $port")

            keyExpirationScheduler.start(workerGroup)

            channel?.closeFuture()?.sync()
        } finally {
            shutdown()
        }
    }

    fun shutdown() {
        logger.info("Shutting down redis server...")
        channel?.close()?.sync()
        workerGroup.shutdownGracefully(shutdownQuietPeriodMs, shutdownTimeoutMs, TimeUnit.MILLISECONDS).sync()
        bossGroup.shutdownGracefully(shutdownQuietPeriodMs, shutdownTimeoutMs, TimeUnit.MILLISECONDS).sync()
        logger.info("Redis server stopped")
    }

    private fun createEventLoopGroup(): EventLoopGroup =
        if (useEpoll) EpollEventLoopGroup(1) else NioEventLoopGroup(1)

    companion object {
        private const val DEFAULT_PORT = 6379
        private const val DEFAULT_SHUTDOWN_QUIET_PERIOD_MS = 100L
        private const val DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000L
        private const val SO_BACKLOG = 1024
    }
}
