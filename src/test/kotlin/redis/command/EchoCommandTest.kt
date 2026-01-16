package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue

class EchoCommandTest {
    private val command = EchoCommand()

    @Test
    fun `메시지를 그대로 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ECHO".toByteArray()),
            RESPValue.BulkString("hello".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).asString).isEqualTo("hello")
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("ECHO".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `BulkString이 아닌 인자는 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ECHO".toByteArray()),
            RESPValue.SimpleString("hello")
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
