package redis.command.string

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.error.WrongTypeException
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.ListOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class StrlenCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var command: StrlenCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        stringOps = StringOperations(store)
        command = StrlenCommand(stringOps)
    }

    @Test
    fun `문자열의 길이를 반환한다`() {
        stringOps.set("mykey", "hello world".toByteArray())

        val args = listOf(
            RESPValue.BulkString("STRLEN".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(11L)
    }

    @Test
    fun `존재하지 않는 키는 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("STRLEN".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `List 타입에 strlen하면 WrongTypeException을 던진다`() {
        val listOps = ListOperations(store)
        listOps.lpush("mylist", listOf("value".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("STRLEN".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키는 0을 반환한다`() {
        stringOps.set("mykey", "hello".toByteArray())
        store.setExpiration("mykey", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("STRLEN".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("STRLEN".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
