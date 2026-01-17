package redis.command.key

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.protocol.RESPValue
import redis.storage.FakeClock
import redis.storage.KeyOperations
import redis.storage.RedisStore
import redis.storage.StringOperations

class ScanCommandTest {
    private lateinit var store: RedisStore
    private lateinit var stringOps: StringOperations
    private lateinit var keyOps: KeyOperations
    private lateinit var command: ScanCommand

    @BeforeEach
    fun setUp() {
        store = RedisStore(FakeClock())
        stringOps = StringOperations(store)
        keyOps = KeyOperations(store)
        command = ScanCommand(keyOps)
    }

    @Test
    fun `커서 기반으로 키를 순회한다`() {
        repeat(5) { i ->
            stringOps.set("key$i", "value$i".toByteArray())
        }
        val args = listOf(
            RESPValue.BulkString("SCAN".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("COUNT".toByteArray()),
            RESPValue.BulkString("3".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Array::class.java)
        val array = (result as RESPValue.Array).elements!!
        assertThat(array).hasSize(2)

        val nextCursor = (array[0] as RESPValue.BulkString).asString
        val keys = (array[1] as RESPValue.Array).elements!!
        assertThat(keys).hasSize(3)
        assertThat(nextCursor).isNotEqualTo("0")
    }

    @Test
    fun `모든 키를 순회하면 커서 0을 반환한다`() {
        stringOps.set("key1", "a".toByteArray())
        stringOps.set("key2", "b".toByteArray())
        val args = listOf(
            RESPValue.BulkString("SCAN".toByteArray()),
            RESPValue.BulkString("0".toByteArray())
        )

        val result = command.execute(args)

        val array = (result as RESPValue.Array).elements!!
        val nextCursor = (array[0] as RESPValue.BulkString).asString
        val keys = (array[1] as RESPValue.Array).elements!!
        assertThat(keys).hasSize(2)
        assertThat(nextCursor).isEqualTo("0")
    }

    @Test
    fun `MATCH 옵션으로 패턴 매칭을 지원한다`() {
        stringOps.set("user:1", "a".toByteArray())
        stringOps.set("user:2", "b".toByteArray())
        stringOps.set("order:1", "c".toByteArray())
        val args = listOf(
            RESPValue.BulkString("SCAN".toByteArray()),
            RESPValue.BulkString("0".toByteArray()),
            RESPValue.BulkString("MATCH".toByteArray()),
            RESPValue.BulkString("user:*".toByteArray())
        )

        val result = command.execute(args)

        val array = (result as RESPValue.Array).elements!!
        val keys = (array[1] as RESPValue.Array).elements!!
            .map { (it as RESPValue.BulkString).asString }
        assertThat(keys).containsExactlyInAnyOrder("user:1", "user:2")
    }

    @Test
    fun `빈 데이터베이스에서 빈 결과를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SCAN".toByteArray()),
            RESPValue.BulkString("0".toByteArray())
        )

        val result = command.execute(args)

        val array = (result as RESPValue.Array).elements!!
        val nextCursor = (array[0] as RESPValue.BulkString).asString
        val keys = (array[1] as RESPValue.Array).elements!!
        assertThat(keys).isEmpty()
        assertThat(nextCursor).isEqualTo("0")
    }

    @Test
    fun `잘못된 커서 값이면 에러를 반환한다`() {
        val args = listOf(
            RESPValue.BulkString("SCAN".toByteArray()),
            RESPValue.BulkString("invalid".toByteArray())
        )

        val result = command.execute(args)

        assertThat(result).isInstanceOf(RESPValue.Error::class.java)
    }

    @Test
    fun `command info가 올바르다`() {
        assertThat(command.name).isEqualTo("SCAN")
        assertThat(command.arity).isEqualTo(-2)
        assertThat(command.flags).contains("readonly")
    }
}
