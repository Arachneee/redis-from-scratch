package redis

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import org.slf4j.LoggerFactory
import sun.security.krb5.Confounder.bytes

class RedisProtocolDecoder : ByteToMessageDecoder() {
    private val logger = LoggerFactory.getLogger(RedisProtocolDecoder::class.java)

    override fun decode(
        ctx: ChannelHandlerContext?,
        `in`: ByteBuf?,
        out: MutableList<Any>?,
    ) {
        val readableBytes = `in`!!.readableBytes()
        if (readableBytes <= 0) return

        logger.info("[New Packet Received]")
        logger.info("Length: $readableBytes")
        logger.info("Hex Dump:\n" + ByteBufUtil.hexDump(`in`))

        val bytes = ByteArray(readableBytes)
        `in`.readBytes(bytes)

        val response = handleRequest(String(bytes))

        val responseBuf = ctx!!.alloc().buffer()
        responseBuf.writeBytes(response.toByteArray())

        ctx.writeAndFlush(responseBuf)
    }

    private fun handleRequest(request: String): String {
        logger.info("Content: $request")
        if (request.contains("COMMAND")) return "*0\r\n"
        if (request.contains("PING")) return "+PONG\r\n"
        return String.format(
            "-ERR '%s' is not a valid Redis command.\r\n",
            request.substringBefore(" "),
        )
    }
}
