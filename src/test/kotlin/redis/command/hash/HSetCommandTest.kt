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

class HSetCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var hashOps: HashOperations
    private lateinit var command: HSetCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        hashOps = HashOperations(store)
        command = HSetCommand(hashOps)
    }

    @Test
    fun `새 해시에 필드를 추가하면 1을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HSET".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray()),
            RESPValue.BulkString("value1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }

    @Test
    fun `기존 필드를 업데이트하면 0을 반환한다`() {
        hashOps.hset("myhash", listOf("field1" to "old".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("HSET".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray()),
            RESPValue.BulkString("new".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0L)
    }

    @Test
    fun `여러 필드를 한번에 추가할 수 있다`() {
        val args = listOf(
            RESPValue.BulkString("HSET".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray()),
            RESPValue.BulkString("value1".toByteArray()),
            RESPValue.BulkString("field2".toByteArray()),
            RESPValue.BulkString("value2".toByteArray()),
            RESPValue.BulkString("field3".toByteArray()),
            RESPValue.BulkString("value3".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(3L)
    }

    @Test
    fun `새 필드와 기존 필드를 함께 설정하면 새 필드 개수만 반환한다`() {
        hashOps.hset("myhash", listOf("field1" to "old".toByteArray()))

        val args = listOf(
            RESPValue.BulkString("HSET".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray()),
            RESPValue.BulkString("updated".toByteArray()),
            RESPValue.BulkString("field2".toByteArray()),
            RESPValue.BulkString("new".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HSET".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `필드와 값이 짝이 맞지 않으면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("HSET".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("field1".toByteArray()),
            RESPValue.BulkString("value1".toByteArray()),
            RESPValue.BulkString("field2".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `String 타입의 키에 hset하면 WrongTypeException을 던진다`() {
        val stringOps = StringOperations(store)
        stringOps.set("stringkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("HSET".toByteArray()),
            RESPValue.BulkString("stringkey".toByteArray()),
            RESPValue.BulkString("field".toByteArray()),
            RESPValue.BulkString("value".toByteArray())
        )

        assertThatThrownBy { command.execute(args) }
            .isInstanceOf(WrongTypeException::class.java)
    }

    @Test
    fun `만료된 키에 hset하면 새 해시를 생성한다`() {
        hashOps.hset("myhash", listOf("old" to "value".toByteArray()))
        store.setExpiration("myhash", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("HSET".toByteArray()),
            RESPValue.BulkString("myhash".toByteArray()),
            RESPValue.BulkString("newfield".toByteArray()),
            RESPValue.BulkString("newvalue".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1L)
    }
}
