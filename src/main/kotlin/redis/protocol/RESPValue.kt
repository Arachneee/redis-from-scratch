package redis.protocol

fun List<RESPValue>.getStringAt(index: Int): String? =
    (getOrNull(index) as? RESPValue.BulkString)?.asString

fun List<RESPValue>.getBytesAt(index: Int): ByteArray? =
    (getOrNull(index) as? RESPValue.BulkString)?.data

fun List<RESPValue>.getStringsFrom(startIndex: Int): List<String> =
    drop(startIndex).mapNotNull { (it as? RESPValue.BulkString)?.asString }

sealed class RESPValue {
    data class SimpleString(
        val value: String,
    ) : RESPValue() {
        override fun toRESP(): ByteArray = "+$value\r\n".toByteArray(Charsets.UTF_8)
    }

    data class Error(
        val message: String,
    ) : RESPValue() {
        override fun toRESP(): ByteArray = "-$message\r\n".toByteArray(Charsets.UTF_8)
    }

    data class Integer(
        val value: Long,
    ) : RESPValue() {
        override fun toRESP(): ByteArray = ":$value\r\n".toByteArray(Charsets.UTF_8)
    }

    data class BulkString(
        val data: ByteArray?,
    ) : RESPValue() {
        val asString: String?
            get() = data?.toString(Charsets.UTF_8)

        override fun toRESP(): ByteArray =
            if (data == null) {
                "\$-1\r\n".toByteArray(Charsets.UTF_8)
            } else {
                "\$${data.size}\r\n".toByteArray(Charsets.UTF_8) + data + "\r\n".toByteArray(Charsets.UTF_8)
            }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is BulkString) return false
            return data contentEquals other.data
        }

        override fun hashCode(): Int = data?.contentHashCode() ?: 0

        override fun toString(): String = "BulkString(data=${asString ?: "null"})"
    }

    data class Array(
        val elements: List<RESPValue>?,
    ) : RESPValue() {
        fun getCommand(): String? = (elements?.firstOrNull() as? BulkString)?.asString?.uppercase()

        override fun toRESP(): ByteArray =
            if (elements == null) {
                "*-1\r\n".toByteArray(Charsets.UTF_8)
            } else {
                val header = "*${elements.size}\r\n".toByteArray(Charsets.UTF_8)
                elements.fold(header) { acc, element -> acc + element.toRESP() }
            }
    }

    abstract fun toRESP(): ByteArray
}
