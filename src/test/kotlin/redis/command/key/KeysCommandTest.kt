package redis.command.key

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.KeyOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class KeysCommandTest {
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var keyOps: KeyOperations
    private lateinit var command: KeysCommand

    @BeforeEach
    fun setUp() {
        store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        keyOps = KeyOperations(store)
        command = KeysCommand(keyOps)
    }

    @Test
    fun `패턴과 일치하는 모든 키를 반환한다`() {
        stringOps.set("user:1", "a".toByteArray())
        stringOps.set("user:2", "b".toByteArray())
        stringOps.set("order:1", "c".toByteArray())
        val args = listOf(
            RESPValue.BulkString("KEYS".toByteArray()),
            RESPValue.BulkString("user:*".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val keys = (result as RESPValue.Array).elements!!
            .map { (it as RESPValue.BulkString).asString }
        assertThat(keys).containsExactlyInAnyOrder("user:1", "user:2")
    }

    @Test
    fun `모든 키를 조회할 수 있다`() {
        stringOps.set("key1", "a".toByteArray())
        stringOps.set("key2", "b".toByteArray())
        val args = listOf(
            RESPValue.BulkString("KEYS".toByteArray()),
            RESPValue.BulkString("*".toByteArray())
        )

        val result = command.execute(args)

        val keys = (result as RESPValue.Array).elements!!
            .map { (it as RESPValue.BulkString).asString }
        assertThat(keys).containsExactlyInAnyOrder("key1", "key2")
    }

    @Test
    fun `물음표 패턴을 지원한다`() {
        stringOps.set("key1", "a".toByteArray())
        stringOps.set("key2", "b".toByteArray())
        stringOps.set("key10", "c".toByteArray())
        val args = listOf(
            RESPValue.BulkString("KEYS".toByteArray()),
            RESPValue.BulkString("key?".toByteArray())
        )

        val result = command.execute(args)

        val keys = (result as RESPValue.Array).elements!!
            .map { (it as RESPValue.BulkString).asString }
        assertThat(keys).containsExactlyInAnyOrder("key1", "key2")
    }

    @Test
    fun `일치하는 키가 없으면 빈 배열을 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("KEYS".toByteArray()),
            RESPValue.BulkString("nonexistent:*".toByteArray())
        )

        val result = command.execute(args)

        assertThat((result as RESPValue.Array).elements).isEmpty()
    }

    @Test
    fun `인자가 부족하면 에러를 반환한다`() {
        val args = listOf(RESPValue.BulkString("KEYS".toByteArray()))

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("KEYS")
        assertThat(command.arity).isEqualTo(2)
    }
}
