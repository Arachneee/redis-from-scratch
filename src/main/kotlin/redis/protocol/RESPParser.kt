package redis.protocol

import io.netty.buffer.ByteBuf

class RESPParser {
    fun parse(buffer: ByteBuf): RESPValue? {
        if (!buffer.isReadable) return null

        val prefix = buffer.readByte().toInt().toChar()
        return when (prefix) {
            '+' -> parseSimpleString(buffer)
            '-' -> parseError(buffer)
            ':' -> parseInteger(buffer)
            '$' -> parseBulkString(buffer)
            '*' -> parseArray(buffer)
            else -> null
        }
    }

    private fun parseSimpleString(buffer: ByteBuf): RESPValue.SimpleString? {
        val message = readLine(buffer) ?: return null
        return RESPValue.SimpleString(message)
    }

    private fun parseError(buffer: ByteBuf): RESPValue.Error? {
        val message = readLine(buffer) ?: return null
        return RESPValue.Error(message)
    }

    private fun parseInteger(buffer: ByteBuf): RESPValue.Integer? {
        val line = readLine(buffer) ?: return null
        return RESPValue.Integer(line.toLong())
    }

    private fun parseBulkString(buffer: ByteBuf): RESPValue.BulkString? {
        val lengthLine = readLine(buffer) ?: return null
        val length = lengthLine.toInt()

        if (length < 0) return RESPValue.BulkString(null)

        if (!hasBulkStringData(buffer, length)) return null

        val data = ByteArray(length)
        buffer.readBytes(data)
        buffer.skipBytes(2)
        return RESPValue.BulkString(data)
    }

    private fun parseArray(buffer: ByteBuf): RESPValue.Array? {
        val countLine = readLine(buffer) ?: return null
        val count = countLine.toInt()

        if (count < 0) return RESPValue.Array(null)

        val elements = mutableListOf<RESPValue>()
        repeat(count) {
            val element = parse(buffer) ?: return null
            elements.add(element)
        }
        return RESPValue.Array(elements)
    }

    private fun readLine(buffer: ByteBuf): String? {
        val crlfIndex = findCrlfIndex(buffer)
        if (crlfIndex == -1) return null

        val length = crlfIndex - 1 - buffer.readerIndex()
        val line = buffer.readCharSequence(length, Charsets.UTF_8).toString()
        buffer.skipBytes(2)
        return line
    }

    private fun findCrlfIndex(buffer: ByteBuf): Int {
        val lfIndex = buffer.indexOf(buffer.readerIndex(), buffer.writerIndex(), '\n'.code.toByte())
        if (lfIndex == -1 || lfIndex == buffer.readerIndex() ||
            buffer.getByte(lfIndex - 1) != '\r'.code.toByte()
        ) {
            return -1
        }
        return lfIndex
    }

    private fun hasBulkStringData(
        buffer: ByteBuf,
        length: Int,
    ): Boolean =
        buffer.readableBytes() >= length + 2 &&
            buffer.getByte(buffer.readerIndex() + length) == '\r'.code.toByte() &&
            buffer.getByte(buffer.readerIndex() + length + 1) == '\n'.code.toByte()
}
