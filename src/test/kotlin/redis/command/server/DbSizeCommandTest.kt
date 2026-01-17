package redis.command.server

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.ServerOperations
import redis.storage.StringOperations

class DbSizeCommandTest {
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var serverOps: ServerOperations
    private lateinit var command: DbSizeCommand

    @BeforeEach
    fun setUp() {
        store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        serverOps = ServerOperations(store)
        command = DbSizeCommand(serverOps)
    }

    @Test
    fun `저장된 키의 개수를 반환한다`() {
        stringOps.set("key1", "value1".toByteArray())
        stringOps.set("key2", "value2".toByteArray())
        val args = listOf(RESPValue.BulkString("DBSIZE".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(2)
    }

    @Test
    fun `빈 데이터베이스에서 0을 반환한다`() {
        val args = listOf(RESPValue.BulkString("DBSIZE".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Integer::class.java)
        assertThat((result as RESPValue.Integer).value).isEqualTo(0)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("DBSIZE")
        assertThat(command.arity).isEqualTo(1)
        assertThat(command.flags).contains("readonly", "fast")
    }
}
