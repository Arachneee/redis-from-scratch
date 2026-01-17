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

class AppendCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var command: AppendCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        stringOps = StringOperations(store)
        command = AppendCommand(stringOps)
    }

    @Test
    fun `존재하지 않는 키에 append하면 새 키를 생성한다`() {
        val args = listOf(
            RESPValue.BulkString("APPEND".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray()),
            RESPValue.BulkString("hello".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(5L)
        assertThat(stringOps.get("mykey")).isEqualTo("hello".toByteArray())
    }

    @Test
    fun `기존 값에 append하면 길이를 반환한다`() {
        stringOps.set("mykey", "hello".toByteArray())

        val args = listOf(
            RESPValue.BulkString("APPEND".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray()),
            RESPValue.BulkString(" world".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(11L)
        assertThat(stringOps.get("mykey")).isEqualTo("hello world".toByteArray())
    }

    @Test
    fun `List 타입에 append하면 WrongTypeException을 던진다`() {
        val listOps = ListOperations(store)
        listOps.lpush("mylist", listOf("value".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("APPEND".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("value".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에 append하면 새 키를 생성한다`() {
        stringOps.set("mykey", "old".toByteArray())
        store.setExpiration("mykey", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("APPEND".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray()),
            RESPValue.BulkString("new".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("APPEND".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
