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

class LPushCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var listOps: ListOperations
    private lateinit var command: LPushCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        listOps = ListOperations(store)
        command = LPushCommand(listOps)
    }

    @Test
    fun `새 키에 값을 추가하면 리스트 길이를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("LPUSH".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("value1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }

    @Test
    fun `기존 리스트에 값을 추가하면 전체 길이를 반환한다`() {
        listOps.lpush("mylist", listOf("first".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LPUSH".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("second".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(2L)
    }

    @Test
    fun `여러 값을 한번에 추가할 수 있다`() {
        val args = listOf(
            RESPValue.BulkString("LPUSH".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("a".toByteArray()),
            RESPValue.BulkString("b".toByteArray()),
            RESPValue.BulkString("c".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("LPUSH".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 lpush하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("LPUSH".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("newvalue".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에 lpush하면 새 리스트를 생성한다`() {
        listOps.lpush("mylist", listOf("old".toByteArray()))
        store.setExpiration("mylist", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("LPUSH".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("new".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }
}
