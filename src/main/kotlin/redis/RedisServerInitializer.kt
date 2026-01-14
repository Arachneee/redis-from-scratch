package redis

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import redis.command.CommandRegistry

class RedisServerInitializer : ChannelInitializer<SocketChannel>() {
    private val repository = RedisRepository()
    private val commandRegistry = CommandRegistry(repository)

    override fun initChannel(ch: SocketChannel) {
        ch.pipeline().apply {
            addLast(RedisProtocolDecoder())
            addLast(RedisProtocolEncoder())
            addLast(RedisCommandHandler(commandRegistry))
        }
    }
}
