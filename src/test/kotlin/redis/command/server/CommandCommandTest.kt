package redis.command.server

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.command.CommandRegistry
import redis.protocol.RESPValue
import redis.storage.OperationsBundle

class CommandCommandTest {
    private lateinit var registry: CommandRegistry
    private lateinit var command: CommandCommand

    @BeforeEach
    fun setUp() {
        val ops = OperationsBundle.create()
        registry = CommandRegistry(ops)
        command = registry.find("COMMAND") as CommandCommand
    }

    @Test
    fun `모든 명령어 정보를 반환한다`() {
        val args = listOf(RESPValue.BulkString("COMMAND".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val commands = (result as RESPValue.Array).elements!!
        assertThat(commands.size).isGreaterThan(0)
    }

    @Test
    fun `각 명령어 정보는 올바른 형식을 가진다`() {
        val args = listOf(RESPValue.BulkString("COMMAND".toByteArray()))

        val result = command.execute(args)

        val commands = (result as RESPValue.Array).elements!!
        val firstCommand = (commands.first() as RESPValue.Array).elements!!

        assertThat(firstCommand.size).isEqualTo(6)
        assertThat(firstCommand[0]).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat(firstCommand[1]).isInstanceOf(RESPValue.Integer::class.java)
        assertThat(firstCommand[2]).isInstanceOf(RESPValue.Array::class.java)
        assertThat(firstCommand[3]).isInstanceOf(RESPValue.Integer::class.java)
        assertThat(firstCommand[4]).isInstanceOf(RESPValue.Integer::class.java)
        assertThat(firstCommand[5]).isInstanceOf(RESPValue.Integer::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("COMMAND")
        assertThat(command.arity).isEqualTo(-1)
        assertThat(command.flags).contains("readonly", "loading", "stale")
    }
}
