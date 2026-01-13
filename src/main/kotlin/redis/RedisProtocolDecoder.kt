package redis

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import redis.protocol.RESPParser

class RedisProtocolDecoder : ByteToMessageDecoder() {
    private val parser = RESPParser()

    override fun decode(
        ctx: ChannelHandlerContext,
        buffer: ByteBuf,
        out: MutableList<Any>,
    ) {
        while (buffer.isReadable) {
            buffer.markReaderIndex()

            val result = parser.parse(buffer)
            if (result == null) {
                buffer.resetReaderIndex()
                return
            }

            out.add(result)
        }
    }
}
