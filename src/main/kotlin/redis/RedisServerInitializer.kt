package redis

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel

class RedisServerInitializer : ChannelInitializer<SocketChannel>() {
    override fun initChannel(ch: SocketChannel) {
        ch.pipeline().apply {
            addLast(RedisProtocolDecoder())
            addLast(RedisProtocolEncoder())
            addLast(RedisCommandHandler())
        }
    }
}
