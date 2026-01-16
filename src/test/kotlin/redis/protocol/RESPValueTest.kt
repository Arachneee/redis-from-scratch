package redis.protocol

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class RESPValueTest {

    @Nested
    inner class SimpleStringTest {
        @Test
        fun `SimpleString을 RESP 형식으로 변환한다`() {
            val value = RESPValue.SimpleString("OK")

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("+OK\r\n")
        }

        @Test
        fun `빈 SimpleString을 RESP 형식으로 변환한다`() {
            val value = RESPValue.SimpleString("")

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("+\r\n")
        }
    }

    @Nested
    inner class ErrorTest {
        @Test
        fun `Error를 RESP 형식으로 변환한다`() {
            val value = RESPValue.Error("ERR unknown command")

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("-ERR unknown command\r\n")
        }
    }

    @Nested
    inner class IntegerTest {
        @Test
        fun `양수 Integer를 RESP 형식으로 변환한다`() {
            val value = RESPValue.Integer(42)

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo(":42\r\n")
        }

        @Test
        fun `0을 RESP 형식으로 변환한다`() {
            val value = RESPValue.Integer(0)

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo(":0\r\n")
        }

        @Test
        fun `음수 Integer를 RESP 형식으로 변환한다`() {
            val value = RESPValue.Integer(-1)

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo(":-1\r\n")
        }
    }

    @Nested
    inner class BulkStringTest {
        @Test
        fun `BulkString을 RESP 형식으로 변환한다`() {
            val value = RESPValue.BulkString("hello".toByteArray())

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("\$5\r\nhello\r\n")
        }

        @Test
        fun `빈 BulkString을 RESP 형식으로 변환한다`() {
            val value = RESPValue.BulkString("".toByteArray())

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("\$0\r\n\r\n")
        }

        @Test
        fun `null BulkString을 RESP 형식으로 변환한다`() {
            val value = RESPValue.BulkString(null)

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("\$-1\r\n")
        }

        @Test
        fun `asString은 문자열을 반환한다`() {
            val value = RESPValue.BulkString("hello".toByteArray())

            assertThat(value.asString).isEqualTo("hello")
        }

        @Test
        fun `null data의 asString은 null을 반환한다`() {
            val value = RESPValue.BulkString(null)

            assertThat(value.asString).isNull()
        }

        @Test
        fun `동일한 데이터를 가진 BulkString은 equals가 true다`() {
            val value1 = RESPValue.BulkString("hello".toByteArray())
            val value2 = RESPValue.BulkString("hello".toByteArray())

            assertThat(value1).isEqualTo(value2)
        }

        @Test
        fun `다른 데이터를 가진 BulkString은 equals가 false다`() {
            val value1 = RESPValue.BulkString("hello".toByteArray())
            val value2 = RESPValue.BulkString("world".toByteArray())

            assertThat(value1).isNotEqualTo(value2)
        }
    }

    @Nested
    inner class ArrayTest {
        @Test
        fun `Array를 RESP 형식으로 변환한다`() {
            val value = RESPValue.Array(
                listOf(
                    RESPValue.BulkString("hello".toByteArray()),
                    RESPValue.BulkString("world".toByteArray())
                )
            )

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("*2\r\n\$5\r\nhello\r\n\$5\r\nworld\r\n")
        }

        @Test
        fun `빈 Array를 RESP 형식으로 변환한다`() {
            val value = RESPValue.Array(emptyList())

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("*0\r\n")
        }

        @Test
        fun `null Array를 RESP 형식으로 변환한다`() {
            val value = RESPValue.Array(null)

            val result = value.toRESP()

            assertThat(String(result)).isEqualTo("*-1\r\n")
        }

        @Test
        fun `getCommand는 첫 번째 요소를 대문자로 반환한다`() {
            val value = RESPValue.Array(
                listOf(
                    RESPValue.BulkString("get".toByteArray()),
                    RESPValue.BulkString("key".toByteArray())
                )
            )

            assertThat(value.getCommand()).isEqualTo("GET")
        }

        @Test
        fun `빈 Array의 getCommand는 null을 반환한다`() {
            val value = RESPValue.Array(emptyList())

            assertThat(value.getCommand()).isNull()
        }

        @Test
        fun `null Array의 getCommand는 null을 반환한다`() {
            val value = RESPValue.Array(null)

            assertThat(value.getCommand()).isNull()
        }
    }

    @Nested
    inner class ExtensionFunctionsTest {
        @Test
        fun `getStringAt은 지정된 인덱스의 문자열을 반환한다`() {
            val list = listOf(
                RESPValue.BulkString("GET".toByteArray()),
                RESPValue.BulkString("key".toByteArray())
            )

            assertThat(list.getStringAt(1)).isEqualTo("key")
        }

        @Test
        fun `getStringAt은 범위를 벗어나면 null을 반환한다`() {
            val list = listOf(RESPValue.BulkString("GET".toByteArray()))

            assertThat(list.getStringAt(5)).isNull()
        }

        @Test
        fun `getBytesAt은 지정된 인덱스의 바이트 배열을 반환한다`() {
            val list = listOf(
                RESPValue.BulkString("GET".toByteArray()),
                RESPValue.BulkString("value".toByteArray())
            )

            assertThat(list.getBytesAt(1)).isEqualTo("value".toByteArray())
        }

        @Test
        fun `getStringsFrom은 시작 인덱스부터의 문자열 목록을 반환한다`() {
            val list = listOf(
                RESPValue.BulkString("DEL".toByteArray()),
                RESPValue.BulkString("key1".toByteArray()),
                RESPValue.BulkString("key2".toByteArray()),
                RESPValue.BulkString("key3".toByteArray())
            )

            assertThat(list.getStringsFrom(1)).containsExactly("key1", "key2", "key3")
        }
    }
}
