package redis.command.server

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.ServerOperations
import redis.storage.StringOperations

class FlushDbCommandTest {
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var serverOps: ServerOperations
    private lateinit var command: FlushDbCommand

    @BeforeEach
    fun setUp() {
        store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        serverOps = ServerOperations(store)
        command = FlushDbCommand(serverOps)
    }

    @Test
    fun `모든 키를 삭제하고 OK를 반환한다`() {
        stringOps.set("key1", "value1".toByteArray())
        stringOps.set("key2", "value2".toByteArray())
        val args = listOf(RESPValue.BulkString("FLUSHDB".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("OK")
        assertThat(store.size()).isEqualTo(0)
    }

    @Test
    fun `빈 데이터베이스에서도 OK를 반환한다`() {
        val args = listOf(RESPValue.BulkString("FLUSHDB".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("OK")
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("FLUSHDB")
        assertThat(command.arity).isEqualTo(1)
        assertThat(command.flags).contains("write")
    }
}
