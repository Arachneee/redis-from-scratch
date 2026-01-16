package redis.storage

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RedisRepositoryTest {
    private lateinit var clock: FakeClock
    private lateinit var repository: RedisRepository

    @BeforeEach
    fun setUp() {
        clock = FakeClock()
        repository = RedisRepository(clock)
    }

    @Test
    fun `set과 get이 정상 동작한다`() {
        repository.set("key", "value".toByteArray())

        val result = repository.get("key")

        assertThat(result).isNotNull
        assertThat(String(result!!)).isEqualTo("value")
    }

    @Test
    fun `존재하지 않는 키는 null을 반환한다`() {
        val result = repository.get("non-existent")

        assertThat(result).isNull()
    }

    @Test
    fun `delete는 키를 삭제하고 1을 반환한다`() {
        repository.set("key", "value".toByteArray())

        val result = repository.delete("key")

        assertThat(result).isEqualTo(1)
        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `delete는 존재하지 않는 키에 대해 0을 반환한다`() {
        val result = repository.delete("non-existent")

        assertThat(result).isEqualTo(0)
    }

    @Test
    fun `expire는 키에 TTL을 설정하고 1을 반환한다`() {
        repository.set("key", "value".toByteArray())

        val result = repository.expire("key", 10)

        assertThat(result).isEqualTo(1)
    }

    @Test
    fun `expire는 존재하지 않는 키에 대해 0을 반환한다`() {
        val result = repository.expire("non-existent", 10)

        assertThat(result).isEqualTo(0)
    }

    @Test
    fun `expire에 0이하 값을 전달하면 키가 삭제된다`() {
        repository.set("key", "value".toByteArray())

        val result = repository.expire("key", 0)

        assertThat(result).isEqualTo(1)
        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `만료된 키는 get 호출 시 null을 반환한다`() {
        repository.set("key", "value".toByteArray())
        repository.expire("key", 1)

        assertThat(repository.get("key")).isNotNull

        clock.advanceBy(1001)

        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `cleanupExpiredKeys는 만료된 키를 삭제한다`() {
        repeat(30) { index ->
            repository.set("key-$index", "value".toByteArray())
            repository.expire("key-$index", 1)
        }

        clock.advanceBy(1001)

        var needsMoreCleanup = repository.cleanupExpiredKeys()
        while (needsMoreCleanup) {
            needsMoreCleanup = repository.cleanupExpiredKeys()
        }

        repeat(30) { index ->
            assertThat(repository.get("key-$index")).isNull()
        }
    }

    @Test
    fun `set은 기존 TTL을 제거한다`() {
        repository.set("key", "value".toByteArray())
        repository.expire("key", 1)

        repository.set("key", "new-value".toByteArray())

        clock.advanceBy(1001)

        assertThat(repository.get("key")).isNotNull
        assertThat(String(repository.get("key")!!)).isEqualTo("new-value")
    }

    @Test
    fun `여러 키를 한번에 삭제할 수 있다`() {
        repository.set("key1", "value1".toByteArray())
        repository.set("key2", "value2".toByteArray())
        repository.set("key3", "value3".toByteArray())

        val result = repository.delete(listOf("key1", "key2", "non-existent"))

        assertThat(result).isEqualTo(2)
        assertThat(repository.get("key1")).isNull()
        assertThat(repository.get("key2")).isNull()
        assertThat(repository.get("key3")).isNotNull
    }

    @Test
    fun `ttl은 남은 시간을 초 단위로 반환한다`() {
        repository.set("key", "value".toByteArray())
        repository.expire("key", 10)

        clock.advanceBy(3000)

        assertThat(repository.ttl("key")).isEqualTo(7)
    }

    @Test
    fun `ttl은 TTL이 설정되지 않은 키에 대해 -1을 반환한다`() {
        repository.set("key", "value".toByteArray())

        assertThat(repository.ttl("key")).isEqualTo(-1)
    }

    @Test
    fun `ttl은 존재하지 않는 키에 대해 -2를 반환한다`() {
        assertThat(repository.ttl("non-existent")).isEqualTo(-2)
    }

    @Test
    fun `setWithTtlSeconds는 TTL과 함께 값을 저장한다`() {
        repository.setWithTtlSeconds("key", "value".toByteArray(), 5)

        assertThat(repository.get("key")).isNotNull

        clock.advanceBy(5001)

        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `setWithTtlMillis는 밀리초 TTL과 함께 값을 저장한다`() {
        repository.setWithTtlMillis("key", "value".toByteArray(), 500)

        assertThat(repository.get("key")).isNotNull

        clock.advanceBy(501)

        assertThat(repository.get("key")).isNull()
    }
}
