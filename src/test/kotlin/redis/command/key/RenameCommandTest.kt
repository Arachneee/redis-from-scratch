package redis.command.key

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.KeyOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class RenameCommandTest {
    private lateinit var clock: FakeClock
    private lateinit var store: RedisStore
    private lateinit var keyOps: KeyOperations
    private lateinit var stringOps: StringOperations
    private lateinit var command: RenameCommand

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        store = RedisStore(clock)
        keyOps = KeyOperations(store)
        stringOps = StringOperations(store)
        command = RenameCommand(keyOps)
    }

    @Test
    fun `키 이름을 변경하면 OK를 반환한다`() {
        stringOps.set("oldkey", "value".toByteArray())

        val args = listOf(
            RESPValue.BulkString("RENAME".toByteArray()),
            RESPValue.BulkString("oldkey".toByteArray()),
            RESPValue.BulkString("newkey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("OK")
        assertThat(stringOps.get("oldkey")).isNull()
        assertThat(stringOps.get("newkey")).isEqualTo("value".toByteArray())
    }

    @Test
    fun `TTL도 함께 이동한다`() {
        stringOps.set("oldkey", "value".toByteArray())
        store.setExpiration("oldkey", 5000)

        val args = listOf(
            RESPValue.BulkString("RENAME".toByteArray()),
            RESPValue.BulkString("oldkey".toByteArray()),
            RESPValue.BulkString("newkey".toByteArray())
        )

        command.execute(args)

        assertThat(store.expirationTimes.containsKey("oldkey")).isFalse()
        assertThat(store.expirationTimes.containsKey("newkey")).isTrue()
    }

    @Test
    fun `존재하지 않는 키를 rename하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("RENAME".toByteArray()),
            RESPValue.BulkString("nonexistent".toByteArray()),
            RESPValue.BulkString("newkey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
        assertThat((result as RESPValue.Error).message).contains("no such key")
    }

    @Test
    fun `만료된 키를 rename하면 에러를 반환한다`() {
        stringOps.set("oldkey", "value".toByteArray())
        store.setExpiration("oldkey", 1000)
        clock.advanceBy(1001)

        val args = listOf(
            RESPValue.BulkString("RENAME".toByteArray()),
            RESPValue.BulkString("oldkey".toByteArray()),
            RESPValue.BulkString("newkey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `새 키에 기존 값이 있으면 덮어쓴다`() {
        stringOps.set("oldkey", "oldvalue".toByteArray())
        stringOps.set("newkey", "existingvalue".toByteArray())

        val args = listOf(
            RESPValue.BulkString("RENAME".toByteArray()),
            RESPValue.BulkString("oldkey".toByteArray()),
            RESPValue.BulkString("newkey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat(stringOps.get("newkey")).isEqualTo("oldvalue".toByteArray())
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("RENAME".toByteArray()),
            RESPValue.BulkString("oldkey".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
