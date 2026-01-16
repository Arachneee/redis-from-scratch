package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue

class PingCommandTest {
    private val command = PingCommand()

    @Test
    fun `PONG을 반환한다`() {
        val args = listOf(RESPValue.BulkString("PING".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("PONG")
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("PING")
        assertThat(command.arity).isEqualTo(1)
        assertThat(command.flags).contains("fast")
    }
}
