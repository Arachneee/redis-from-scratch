package redis.server

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import redis.aof.AofHandler
import redis.aof.AofManager
import redis.command.CommandRegistry
import redis.command.RedisCommandHandler
import redis.config.RedisConfig
import redis.protocol.RedisProtocolDecoder
import redis.protocol.RedisProtocolEncoder

class RedisServerInitializer(
    private val commandRegistry: CommandRegistry,
    private val config: RedisConfig,
    private val aofManager: AofManager,
) : ChannelInitializer<SocketChannel>() {
    private val encoder = RedisProtocolEncoder()
    private val commandHandler = RedisCommandHandler(commandRegistry)

    override fun initChannel(ch: SocketChannel) {
        ch.pipeline().apply {
            addLast(RedisProtocolDecoder())
            addLast(encoder)
            addLast(AofHandler(commandRegistry, aofManager))
            addLast(commandHandler)
        }
    }
}
