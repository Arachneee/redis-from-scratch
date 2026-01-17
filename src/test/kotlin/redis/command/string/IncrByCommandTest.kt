package redis.command.string

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.StringOperations

class IncrByCommandTest {
    private lateinit var stringOps: StringOperations
    private lateinit var command: IncrByCommand

    @BeforeEach
    fun setUp() {
        val store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        command = IncrByCommand(stringOps)
    }

    @Test
    fun `존재하지 않는 키를 delta로 설정하고 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("INCRBY".toByteArray()),
            RESPValue.BulkString("counter".toByteArray()),
            RESPValue.BulkString("5".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(5)
    }

    @Test
    fun `기존 숫자 값을 delta만큼 증가시킨다`() {
        stringOps.set("counter", "10".toByteArray())
        val args = listOf(
            RESPValue.BulkString("INCRBY".toByteArray()),
            RESPValue.BulkString("counter".toByteArray()),
            RESPValue.BulkString("5".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(15)
    }

    @Test
    fun `음수 delta로 감소시킬 수 있다`() {
        stringOps.set("counter", "10".toByteArray())
        val args = listOf(
            RESPValue.BulkString("INCRBY".toByteArray()),
            RESPValue.BulkString("counter".toByteArray()),
            RESPValue.BulkString("-3".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(7)
    }

    @Test
    fun `delta가 숫자가 아니면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("INCRBY".toByteArray()),
            RESPValue.BulkString("counter".toByteArray()),
            RESPValue.BulkString("not-a-number".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("INCRBY".toByteArray()),
            RESPValue.BulkString("counter".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("INCRBY")
        assertThat(command.arity).isEqualTo(3)
    }
}
