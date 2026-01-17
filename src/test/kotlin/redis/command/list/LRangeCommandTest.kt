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

class LRangeCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var listOps: ListOperations
    private lateinit var command: LRangeCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        listOps = ListOperations(store)
        command = LRangeCommand(listOps)
    }

    @Test
    fun `전체 범위를 조회한다`() {
        listOps.rpush("mylist", listOf("a".toByteArray(), "b".toByteArray(), "c".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LRANGE".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val elements = (result as RESPValue.Array).elements!!
        assertThat(elements).hasSize(3)
        assertThat((elements[0] as RESPValue.BulkString).asString).isEqualTo("a")
        assertThat((elements[1] as RESPValue.BulkString).asString).isEqualTo("b")
        assertThat((elements[2] as RESPValue.BulkString).asString).isEqualTo("c")
    }

    @Test
    fun `부분 범위를 조회한다`() {
        listOps.rpush("mylist", listOf("a".toByteArray(), "b".toByteArray(), "c".toByteArray(), "d".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LRANGE".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("1".toByteArray()),
            RESPValue.BulkString("2".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val elements = (result as RESPValue.Array).elements!!
        assertThat(elements).hasSize(2)
        assertThat((elements[0] as RESPValue.BulkString).asString).isEqualTo("b")
        assertThat((elements[1] as RESPValue.BulkString).asString).isEqualTo("c")
    }

    @Test
    fun `음수 인덱스로 끝에서부터 조회한다`() {
        listOps.rpush("mylist", listOf("a".toByteArray(), "b".toByteArray(), "c".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LRANGE".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("-2".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val elements = (result as RESPValue.Array).elements!!
        assertThat(elements).hasSize(2)
        assertThat((elements[0] as RESPValue.BulkString).asString).isEqualTo("b")
        assertThat((elements[1] as RESPValue.BulkString).asString).isEqualTo("c")
    }

    @Test
    fun `존재하지 않는 키는 빈 배열을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("LRANGE".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val elements = (result as RESPValue.Array).elements!!
        assertThat(elements).isEmpty()
    }

    @Test
    fun `범위를 벗어나면 빈 배열을 반환한다`() {
        listOps.rpush("mylist", listOf("a".toByteArray(), "b".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LRANGE".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("10".toByteArray()),
            RESPValue.BulkString("20".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val elements = (result as RESPValue.Array).elements!!
        assertThat(elements).isEmpty()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("LRANGE".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("0".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 lrange하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("LRANGE".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키는 빈 배열을 반환한다`() {
        listOps.rpush("mylist", listOf("value".toByteArray()))
        store.setExpiration("mylist", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("LRANGE".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("-1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val elements = (result as RESPValue.Array).elements!!
        assertThat(elements).isEmpty()
    }
}
