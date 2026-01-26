package redis.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.Unpooled
import io.netty.buffer.Unpooled.buffer
import io.netty.channel.Channel
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.resolver.HostsFileEntriesProvider.parser
import org.slf4j.LoggerFactory
import redis.aof.AofManager
import redis.command.CommandRegistry
import redis.config.RedisConfig
import redis.error.RedisErrors
import redis.error.WrongTypeException
import redis.protocol.RESPParser
import redis.protocol.RESPValue
import redis.storage.KeyExpirationScheduler
import redis.storage.OperationsBundle
import java.lang.Compiler.command
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

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
    private val aofManager = AofManager(config.aofFilename)
    private val commandRegistry = CommandRegistry(ops)
    private val isShuttingDown = AtomicBoolean(false)

    fun start() {
        logger.info("Using ${if (useEpoll) "Epoll" else "NIO"} transport (single-threaded)")
        loadAof()

        try {
            val bootstrap = ServerBootstrap()
            bootstrap
                .group(bossGroup, workerGroup)
                .channel(serverChannelClass)
                .option(ChannelOption.SO_BACKLOG, config.soBacklog)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(RedisServerInitializer(commandRegistry, config, aofManager))

            channel = bootstrap.bind(config.port).sync().channel()
            logger.info("Redis server started on port ${config.port}")

            keyExpirationScheduler.start(workerGroup)

            channel?.closeFuture()?.sync()
        } finally {
            shutdown()
        }
    }

    fun shutdown() {
        if (!isShuttingDown.compareAndSet(false, true)) return

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
        aofManager.close()
        logger.info("Redis server stopped")
    }

    private fun loadAof() {
        val cumulativeBuffer = Unpooled.buffer()
        val tempBuffer = ByteArray(65536)
        try {
            aofManager.readAof()?.use { input ->
                var bytesRead: Int
                while (input.read(tempBuffer).also { bytesRead = it } != -1) {
                    cumulativeBuffer.writeBytes(tempBuffer, 0, bytesRead)

                    while (cumulativeBuffer.isReadable) {
                        cumulativeBuffer.markReaderIndex()
                        val value = RESPParser.parse(cumulativeBuffer)

                        if (value == null) {
                            cumulativeBuffer.resetReaderIndex()
                            break
                        }

                        (value as? RESPValue.Array)?.let { executeReplayCommand(it) }
                        cumulativeBuffer.discardReadBytes()
                    }
                }
            }
        } finally {
            cumulativeBuffer.release()
        }
    }

    private fun executeReplayCommand(array: RESPValue.Array) {
        val commandName = array.getCommand() ?: return
        try {
            commandRegistry.find(commandName)?.execute(array.elements ?: emptyList())
        } catch (e: Exception) {
            logger.error("Error replaying AOF command: $commandName", e)
        }
    }

    private fun createEventLoopGroup(): EventLoopGroup = if (useEpoll) EpollEventLoopGroup(1) else NioEventLoopGroup(1)
}
