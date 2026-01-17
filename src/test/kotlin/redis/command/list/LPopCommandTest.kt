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

class LPopCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var listOps: ListOperations
    private lateinit var command: LPopCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        listOps = ListOperations(store)
        command = LPopCommand(listOps)
    }

    @Test
    fun `리스트에서 첫 번째 요소를 꺼낸다`() {
        listOps.lpush("mylist", listOf("a".toByteArray(), "b".toByteArray(), "c".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).asString).isEqualTo("c")
    }

    @Test
    fun `count를 지정하면 여러 요소를 배열로 반환한다`() {
        listOps.lpush("mylist", listOf("a".toByteArray(), "b".toByteArray(), "c".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("2".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val elements = (result as RESPValue.Array).elements!!
        assertThat(elements).hasSize(2)
        assertThat((elements[0] as RESPValue.BulkString).asString).isEqualTo("c")
        assertThat((elements[1] as RESPValue.BulkString).asString).isEqualTo("b")
    }

    @Test
    fun `존재하지 않는 키는 nil을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).data).isNull()
    }

    @Test
    fun `리스트가 비어있으면 nil을 반환한다`() {
        listOps.lpush("mylist", listOf("a".toByteArray()))
        listOps.lpop("mylist", 1)

        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).data).isNull()
    }

    @Test
    fun `count가 리스트 크기보다 크면 가능한 만큼만 반환한다`() {
        listOps.lpush("mylist", listOf("a".toByteArray(), "b".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray()),
            RESPValue.BulkString("10".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val elements = (result as RESPValue.Array).elements!!
        assertThat(elements).hasSize(2)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 lpop하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키는 nil을 반환한다`() {
        listOps.lpush("mylist", listOf("value".toByteArray()))
        store.setExpiration("mylist", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.BulkString::class.java)
        assertThat((result as RESPValue.BulkString).data).isNull()
    }

    @Test
    fun `모든 요소를 꺼내면 키도 삭제된다`() {
        listOps.lpush("mylist", listOf("a".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("LPOP".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        command.execute(args)

        assertThat(store.containsKey("mylist")).isFalse()
    }
}
