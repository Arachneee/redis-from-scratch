package redis.command.key

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.KeyOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class ExpireCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var keyOps: KeyOperations
    private lateinit var command: ExpireCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        stringOps = StringOperations(store)
        keyOps = KeyOperations(store)
        command = ExpireCommand(keyOps)
    }

    @Test
    fun `키에 TTL을 설정하고 1을 반환한다`() {
        stringOps.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("EXPIRE".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("10".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1)
    }

    @Test
    fun `존재하지 않는 키에는 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("EXPIRE".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray()),
            RESPValue.BulkString("10".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0)
    }

    @Test
    fun `TTL이 만료되면 키가 삭제된다`() {
        stringOps.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("EXPIRE".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("1".toByteArray())
        )

        command.execute(args)

        assertThat(stringOps.get("key")).isNotNull

        clock.advanceBy(1001)

        assertThat(stringOps.get("key")).isNull()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("EXPIRE".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `초 값이 숫자가 아니면 에러를 반환한다`() {
        stringOps.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("EXPIRE".toByteArray()),
            RESPValue.BulkString("key".toByteArray()),
            RESPValue.BulkString("not-a-number".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
