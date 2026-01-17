package redis.command.string

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.StringOperations

class SetNxCommandTest {
    private lateinit var stringOps: StringOperations
    private lateinit var command: SetNxCommand

    @BeforeEach
    fun setUp() {
        val store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        command = SetNxCommand(stringOps)
    }

    @Test
    fun `키가 없으면 값을 설정하고 1을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SETNX".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("value".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(1)
        assertThat(stringOps.get("key")?.toString(Charsets.UTF_8)).isEqualTo("value")
    }

    @Test
    fun `키가 이미 존재하면 0을 반환하고 값을 변경하지 않는다`() {
        stringOps.set("key", "original".toByteArray())
        val args = listOf(
            RESPValue.BulkString("SETNX".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("new-value".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(0)
        assertThat(stringOps.get("key")?.toString(Charsets.UTF_8)).isEqualTo("original")
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SETNX".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("SETNX")
        assertThat(command.arity).isEqualTo(3)
        assertThat(command.flags).contains("write", "fast")
    }
}
