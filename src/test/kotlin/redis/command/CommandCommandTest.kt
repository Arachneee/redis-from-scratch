package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue

class CommandCommandTest {
    private lateinit var commands: List<RedisCommand>
    private lateinit var command: CommandCommand

    @BeforeEach
    fun setUp() {
        commands = listOf(PingCommand(), EchoCommand())
        command = CommandCommand { commands }
    }

    @Test
    fun `인자 없이 호출하면 모든 명령어 정보를 반환한다`() {
        val args = listOf(RESPValue.BulkString("COMMAND".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).hasSize(2)
    }

    @Test
    fun `COUNT 서브커맨드는 명령어 개수를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("COMMAND".toByteArray()),
            RESPValue.BulkString("COUNT".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(2)
    }

    @Test
    fun `DOCS 서브커맨드는 빈 배열을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("COMMAND".toByteArray()),
            RESPValue.BulkString("DOCS".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).isEmpty()
    }

    @Test
    fun `INFO 서브커맨드는 요청한 명령어 정보를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("COMMAND".toByteArray()),
            RESPValue.BulkString("INFO".toByteArray()),
            RESPValue.BulkString("PING".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).hasSize(1)
    }

    @Test
    fun `INFO 서브커맨드에 명령어가 없으면 모든 명령어를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("COMMAND".toByteArray()),
            RESPValue.BulkString("INFO".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).hasSize(2)
    }

    @Test
    fun `알 수 없는 서브커맨드는 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("COMMAND".toByteArray()),
            RESPValue.BulkString("UNKNOWN".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
