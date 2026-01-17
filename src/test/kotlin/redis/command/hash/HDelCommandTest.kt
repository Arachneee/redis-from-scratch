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

class HDelCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var hashOps: HashOperations
    private lateinit var command: HDelCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        hashOps = HashOperations(store)
        command = HDelCommand(hashOps)
    }

    @Test
    fun `존재하는 필드를 삭제하면 1을 반환한다`() {
        hashOps.hset("myhash", listOf("field1" to "value1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("HDEL".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }

    @Test
    fun `여러 필드를 한번에 삭제할 수 있다`() {
        hashOps.hset(
            "myhash",
            listOf(
                "field1" to "value1".toByteArray(),
                "field2" to "value2".toByteArray(),
                "field3" to "value3".toByteArray()
            )
        )

        val args = listOf(
            RESPValue.BulkString("HDEL".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray()),
            RESPValue.BulkString("field2".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(2L)
    }

    @Test
    fun `존재하지 않는 필드를 삭제하면 0을 반환한다`() {
        hashOps.hset("myhash", listOf("field1" to "value1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("HDEL".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `존재하지 않는 키에서 삭제하면 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HDEL".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray()),
            RESPValue.BulkString("field".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `모든 필드를 삭제하면 키도 삭제된다`() {
        hashOps.hset("myhash", listOf("field1" to "value1".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("HDEL".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray())
        )

        command.execute(args)

        assertThat(store.containsKey("myhash")).isFalse()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HDEL".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 hdel하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("HDEL".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("field".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에서 hdel하면 0을 반환한다`() {
        hashOps.hset("myhash", listOf("field1" to "value1".toByteArray()))
        store.setExpiration("myhash", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("HDEL".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }
}
