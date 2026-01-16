package redis.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.RedisRepository

class MSetCommandTest {
    private lateinit var repository: RedisRepository
    private lateinit var command: MSetCommand

    @BeforeEach
    fun setUp() {
        repository = RedisRepository(FakeClock())
        command = MSetCommand(repository)
    }

    @Test
    fun `여러 키-값을 한번에 저장한다`() {
        val args = listOf(
            RESPValue.BulkString("MSET".toByteArray()),
            RESPValue.BulkString("key1".toByteArray()),
            RESPValue.BulkString("value1".toByteArray()),
            RESPValue.BulkString("key2".toByteArray()),
            RESPValue.BulkString("value2".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.SimpleString::class.java)
        assertThat((result as RESPValue.SimpleString).value).isEqualTo("OK")
        assertThat(repository.get("key1")?.toString(Charsets.UTF_8)).isEqualTo("value1")
        assertThat(repository.get("key2")?.toString(Charsets.UTF_8)).isEqualTo("value2")
    }

    @Test
    fun `기존 값을 덮어쓴다`() {
        repository.set("key1", "old".toByteArray())
        val args = listOf(
            RESPValue.BulkString("MSET".toByteArray()),
            RESPValue.BulkString("key1".toByteArray()),
            RESPValue.BulkString("new".toByteArray())
        )

        command.execute(args)

        assertThat(repository.get("key1")?.toString(Charsets.UTF_8)).isEqualTo("new")
    }

    @Test
    fun `키-값 쌍이 맞지 않으면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("MSET".toByteArray()),
            RESPValue.BulkString("key1".toByteArray()),
            RESPValue.BulkString("value1".toByteArray()),
            RESPValue.BulkString("key2".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("MSET".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("MSET")
        assertThat(command.arity).isEqualTo(-3)
        assertThat(command.flags).contains("write", "denyoom")
    }
}
