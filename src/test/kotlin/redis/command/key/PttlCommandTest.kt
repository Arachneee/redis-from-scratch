package redis.command.key

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.KeyOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class PttlCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var keyOps: KeyOperations
    private lateinit var command: PttlCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        stringOps = StringOperations(store)
        keyOps = KeyOperations(store)
        command = PttlCommand(keyOps)
    }

    @Test
    fun `남은 TTL을 밀리초 단위로 반환한다`() {
        stringOps.set("key", "value".toByteArray())
        keyOps.expire("key", 10)
        clock.advanceBy(3000)
        val args = listOf(
            RESPValue.BulkString("PTTL".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        val pttl = (result as RESPValue.Integer).value
        assertThat(pttl).isBetween(6000L, 7000L)
    }

    @Test
    fun `TTL이 설정되지 않은 키는 -1을 반환한다`() {
        stringOps.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("PTTL".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(-1)
    }

    @Test
    fun `존재하지 않는 키는 -2를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("PTTL".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Integer).value).isEqualTo(-2)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("PTTL")
        assertThat(command.arity).isEqualTo(2)
        assertThat(command.flags).contains("readonly", "fast")
    }
}
