package redis.command.hash

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.error.WrongTypeException
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.HashOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class HGetAllCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var hashOps: HashOperations
    private lateinit var command: HGetAllCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        hashOps = HashOperations(store)
        command = HGetAllCommand(hashOps)
    }

    @Test
    fun `해시의 모든 필드와 값을 반환한다`() {
        hashOps.hset(
            "myhash",
            listOf(
                "field1" to "value1".toByteArray(),
                "field2" to "value2".toByteArray()
            )
        )

        val args = listOf(
            RESPValue.BulkString("HGETALL".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val array = (result as RESPValue.Array).elements!!
        assertThat(array).hasSize(4)

        val resultMap = mutableMapOf<String, String>()
        for (i in array.indices step 2) {
            val field = String((array[i] as RESPValue.BulkString).data!!)
            val value = String((array[i + 1] as RESPValue.BulkString).data!!)
            resultMap[field] = value
        }
        assertThat(resultMap).containsEntry("field1", "value1")
        assertThat(resultMap).containsEntry("field2", "value2")
    }

    @Test
    fun `존재하지 않는 키에 대해 빈 배열을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HGETALL".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).isEmpty()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HGETALL".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 hgetall하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("HGETALL".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 hgetall하면 빈 배열을 반환한다`() {
        hashOps.hset("myhash", listOf("field1" to "value1".toByteArray()))
        store.setExpiration("myhash", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("HGETALL".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        assertThat((result as RESPValue.Array).elements).isEmpty()
    }
}
