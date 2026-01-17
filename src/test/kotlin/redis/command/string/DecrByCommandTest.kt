package redis.command.string

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.StringOperations

class DecrByCommandTest {
    private lateinit var stringOps: StringOperations
    private lateinit var command: DecrByCommand

    @BeforeEach
    fun setUp() {
        val store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        command = DecrByCommand(stringOps)
    }

    @Test
    fun `존재하지 않는 키를 -delta로 설정하고 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("DECRBY".toByteArray()),
            RESPValue.BulkString("counter".toByteArray()),
            RESPValue.BulkString("5".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(-5)
    }

    @Test
    fun `기존 숫자 값을 delta만큼 감소시킨다`() {
        stringOps.set("counter", "10".toByteArray())
        val args = listOf(
            RESPValue.BulkString("DECRBY".toByteArray()),
            RESPValue.BulkString("counter".toByteArray()),
            RESPValue.BulkString("3".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(7)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("DECRBY")
        assertThat(command.arity).isEqualTo(3)
    }
}
