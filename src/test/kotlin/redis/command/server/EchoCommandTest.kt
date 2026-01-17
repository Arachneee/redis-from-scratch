package redis.command.server

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue

class EchoCommandTest {
    private lateinit var command: EchoCommand

    @BeforeEach
    fun setUp() {
        command = EchoCommand()
    }

    @Test
    fun `메시지를 그대로 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("ECHO".toByteArray()),
            RESPValue.BulkString("Hello".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).asString).isEqualTo("Hello")
    }

    @Test
    fun `인자가 없으면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("ECHO".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("ECHO")
        assertThat(command.arity).isEqualTo(2)
        assertThat(command.flags).contains("fast")
    }
}
