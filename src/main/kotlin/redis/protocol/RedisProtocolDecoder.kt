package redis.protocol

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder

class RedisProtocolDecoder : ByteToMessageDecoder() {
    override fun decode(
        ctx: ChannelHandlerContext,
        buffer: ByteBuf,
        out: MutableList<Any>,
    ) {
        while (buffer.isReadable) {
            buffer.markReaderIndex()

            val result = RESPParser.parse(buffer)
            if (result == null) {
                buffer.resetReaderIndex()
                return
            }

            out.add(result)
        }
    }
}
