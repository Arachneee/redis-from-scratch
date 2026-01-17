package redis.command.key

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.HashOperations
import redis.storage.KeyOperations
import redis.storage.ListOperations
import redis.storage.RedisStore
import redis.storage.SetOperations
import redis.storage.StringOperations
import redis.storage.ZSetOperations

class TypeCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var keyOps: KeyOperations
    private lateinit var command: TypeCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        keyOps = KeyOperations(store)
        command = TypeCommand(keyOps)
    }

    @Test
    fun `존재하지 않는 키는 none을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("TYPE".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("none")
    }

    @Test
    fun `String 타입을 반환한다`() {
        val stringOps = StringOperations(store)
        stringOps.set("mykey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("TYPE".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("string")
    }

    @Test
    fun `List 타입을 반환한다`() {
        val listOps = ListOperations(store)
        listOps.lpush("mylist", listOf("value".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("TYPE".toByteArray()),
            RESPValue.BulkString("mylist".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("list")
    }

    @Test
    fun `Set 타입을 반환한다`() {
        val setOps = SetOperations(store)
        setOps.sadd("myset", listOf("value".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("TYPE".toByteArray()),
            RESPValue.BulkString("myset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("set")
    }

    @Test
    fun `Hash 타입을 반환한다`() {
        val hashOps = HashOperations(store)
        hashOps.hset("myhash", listOf("field" to "value".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("TYPE".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("hash")
    }

    @Test
    fun `ZSet 타입을 반환한다`() {
        val zsetOps = ZSetOperations(store)
        zsetOps.zadd("myzset", listOf(1.0 to "value".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("TYPE".toByteArray()),
            RESPValue.BulkString("myzset".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("zset")
    }

    @Test
    fun `만료된 키는 none을 반환한다`() {
        val stringOps = StringOperations(store)
        stringOps.set("mykey", "value".toByteArray())
        store.setExpiration("mykey", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("TYPE".toByteArray()),
            RESPValue.BulkString("mykey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("none")
    }
}
