package redis.command.list

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

class LLenCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var listOps: ListOperations
    private lateinit var command: LLenCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        listOps = ListOperations(store)
        command = LLenCommand(listOps)
    }

    @Test
    fun `리스트 길이를 반환한다`() {
        listOps.rpush("mylist", listOf("a".toByteArray(), "b".toByteArray(), "c".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LLEN".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `존재하지 않는 키는 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("LLEN".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `빈 리스트는 0을 반환한다`() {
        listOps.rpush("mylist", listOf("a".toByteArray()))
        listOps.lpop("mylist", 1)

        val args = listOf(
            RESPValue.BulkString("LLEN".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("LLEN".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 llen하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("LLEN".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키는 0을 반환한다`() {
        listOps.rpush("mylist", listOf("value".toByteArray()))
        store.setExpiration("mylist", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("LLEN".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }
}
