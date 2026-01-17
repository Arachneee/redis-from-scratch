package redis.command.string

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.StringOperations

class SetExCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var command: SetExCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        stringOps = StringOperations(store)
        command = SetExCommand(stringOps)
    }

    @Test
    fun `값과 TTL을 함께 설정한다`() {
        val args = listOf(
            RESPValue.BulkString("SETEX".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray()),
            RESPValue.BulkString("10".toByteArray()),
            RESPValue.BulkString("hello".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("OK")
        assertThat(stringOps.get("mykey")).isEqualTo("hello".toByteArray())
        assertThat(store.expirationTimes.containsKey("mykey")).isTrue()
    }

    @Test
    fun `TTL이 지나면 값이 만료된다`() {
        val args = listOf(
            RESPValue.BulkString("SETEX".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray()),
            RESPValue.BulkString("1".toByteArray()),
            RESPValue.BulkString("hello".toByteArray())
        )

        command.execute(args)
        clock.advanceBy(1001)

        assertThat(stringOps.get("mykey")).isNull()
    }

    @Test
    fun `유효하지 않은 TTL은 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SETEX".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray()),
            RESPValue.BulkString("notanumber".toByteArray()),
            RESPValue.BulkString("hello".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `TTL이 0이하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SETEX".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("hello".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SETEX".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray()),
            RESPValue.BulkString("10".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
