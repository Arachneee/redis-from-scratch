package redis.protocol

sealed class RESPValue {
    data class SimpleString(val value: String) : RESPValue() {
        override fun toRESP(): String = "+$value\r\n"
    }

    data class Error(val message: String) : RESPValue() {
        override fun toRESP(): String = "-$message\r\n"
    }

    data class Integer(val value: Long) : RESPValue() {
        override fun toRESP(): String = ":$value\r\n"
    }

    data class BulkString(val value: String?) : RESPValue() {
        override fun toRESP(): String =
            if (value == null) {
                "\$-1\r\n"
            } else {
                "\$${value.length}\r\n$value\r\n"
            }
    }

    data class Array(val elements: List<RESPValue>?) : RESPValue() {
        override fun toRESP(): String =
            if (elements == null) {
                "*-1\r\n"
            } else {
                "*${elements.size}\r\n${elements.joinToString("") { it.toRESP() }}"
            }
    }

    abstract fun toRESP(): String
}
