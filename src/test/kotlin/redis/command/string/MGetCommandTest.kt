package redis.command.string

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisStore
import redis.storage.StringOperations

class MGetCommandTest {
    private lateinit var stringOps: StringOperations
    private lateinit var command: MGetCommand

    @BeforeEach
    fun setUp() {
        val store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        command = MGetCommand(stringOps)
    }

    @Test
    fun `여러 키의 값을 한번에 조회한다`() {
        stringOps.set("key1", "value1".toByteArray())
        stringOps.set("key2", "value2".toByteArray())
        val args = listOf(
            RESPValue.BulkString("MGET".toByteArray()),
            RESPValue.BulkString("key1".toByteArray()),
            RESPValue.BulkString("key2".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val array = (result as RESPValue.Array).elements!!
        assertThat(array).hasSize(2)
        assertThat((array[0] as RESPValue.BulkString).asString).isEqualTo("value1")
        assertThat((array[1] as RESPValue.BulkString).asString).isEqualTo("value2")
    }

    @Test
    fun `존재하지 않는 키는 null을 반환한다`() {
        stringOps.set("key1", "value1".toByteArray())
        val args = listOf(
            RESPValue.BulkString("MGET".toByteArray()),
            RESPValue.BulkString("key1".toByteArray()),
            RESPValue.BulkString("non-existent".toByteArray())
        )

        val result = command.execute(args)

        val array = (result as RESPValue.Array).elements!!
        assertThat(array).hasSize(2)
        assertThat((array[0] as RESPValue.BulkString).asString).isEqualTo("value1")
        assertThat((array[1] as RESPValue.BulkString).data).isNull()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("MGET".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("MGET")
        assertThat(command.arity).isEqualTo(-2)
        assertThat(command.flags).contains("readonly", "fast")
    }
}
