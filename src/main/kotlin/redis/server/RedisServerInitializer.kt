package redis.server

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import redis.command.CommandRegistry
import redis.protocol.RedisProtocolDecoder
import redis.protocol.RedisProtocolEncoder
import redis.storage.OperationsBundle

class RedisServerInitializer(
    ops: OperationsBundle,
) : ChannelInitializer<SocketChannel>() {
    private val encoder = RedisProtocolEncoder()
    private val commandRegistry = CommandRegistry(ops)
    private val commandHandler = RedisCommandHandler(commandRegistry)

    override fun initChannel(ch: SocketChannel) {
        ch.pipeline().apply {
            addLast(RedisProtocolDecoder())
            addLast(encoder)
            addLast(AofHandler(commandRegistry))
            addLast(commandHandler)
        }
    }
}
