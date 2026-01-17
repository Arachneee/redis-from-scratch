package redis.command.key

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.KeyOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class DeleteCommandTest {
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var keyOps: KeyOperations
    private lateinit var command: DeleteCommand

    @BeforeEach
    fun setUp() {
        store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        keyOps = KeyOperations(store)
        command = DeleteCommand(keyOps)
    }

    @Test
    fun `키를 삭제하고 삭제된 개수를 반환한다`() {
        stringOps.set("key", "value".toByteArray())
        val args = listOf(
            RESPValue.BulkString("DEL".toByteArray()),
            RESPValue.BulkString("key".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(1)
        assertThat(stringOps.get("key")).isNull()
    }

    @Test
    fun `여러 키를 한번에 삭제할 수 있다`() {
        stringOps.set("key1", "value1".toByteArray())
        stringOps.set("key2", "value2".toByteArray())
        stringOps.set("key3", "value3".toByteArray())
        val args = listOf(
            RESPValue.BulkString("DEL".toByteArray()),
            RESPValue.BulkString("key1".toByteArray()),
            RESPValue.BulkString("key2".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(2)
    }

    @Test
    fun `존재하지 않는 키를 삭제하면 0을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("DEL".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("DEL".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }
}
