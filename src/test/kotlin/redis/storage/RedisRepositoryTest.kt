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

    @Test
    fun `exists는 존재하는 키의 개수를 반환한다`() {
        repository.set("key1", "value1".toByteArray())
        repository.set("key2", "value2".toByteArray())

        val result = repository.exists(listOf("key1", "key2", "non-existent"))

        assertThat(result).isEqualTo(2)
    }

    @Test
    fun `exists는 만료된 키를 제외하고 카운트한다`() {
        repository.set("key1", "value1".toByteArray())
        repository.set("key2", "value2".toByteArray())
        repository.expire("key1", 1)

        clock.advanceBy(1001)

        val result = repository.exists(listOf("key1", "key2"))

        assertThat(result).isEqualTo(1)
    }

    @Test
    fun `size는 저장된 키의 개수를 반환한다`() {
        repository.set("key1", "value1".toByteArray())
        repository.set("key2", "value2".toByteArray())
        repository.set("key3", "value3".toByteArray())

        assertThat(repository.size()).isEqualTo(3)
    }

    @Test
    fun `flushAll은 모든 키를 삭제한다`() {
        repository.set("key1", "value1".toByteArray())
        repository.set("key2", "value2".toByteArray())
        repository.expire("key1", 10)

        repository.flushAll()

        assertThat(repository.get("key1")).isNull()
        assertThat(repository.get("key2")).isNull()
        assertThat(repository.size()).isEqualTo(0)
    }

    @Test
    fun `persist는 TTL을 제거하고 1을 반환한다`() {
        repository.set("key", "value".toByteArray())
        repository.expire("key", 1)

        val result = repository.persist("key")

        assertThat(result).isEqualTo(1)

        clock.advanceBy(1001)

        assertThat(repository.get("key")).isNotNull
    }

    @Test
    fun `persist는 TTL이 없는 키에 대해 0을 반환한다`() {
        repository.set("key", "value".toByteArray())

        val result = repository.persist("key")

        assertThat(result).isEqualTo(0)
    }

    @Test
    fun `persist는 존재하지 않는 키에 대해 0을 반환한다`() {
        val result = repository.persist("non-existent")

        assertThat(result).isEqualTo(0)
    }

    @Test
    fun `incr은 키가 없으면 0에서 1로 증가시킨다`() {
        val result = repository.incr("counter")

        assertThat(result.isSuccess).isTrue()
        assertThat(result.getOrNull()).isEqualTo(1)
        assertThat(repository.get("counter")?.toString(Charsets.UTF_8)).isEqualTo("1")
    }

    @Test
    fun `incr은 기존 숫자 값을 1 증가시킨다`() {
        repository.set("counter", "10".toByteArray())

        val result = repository.incr("counter")

        assertThat(result.isSuccess).isTrue()
        assertThat(result.getOrNull()).isEqualTo(11)
    }

    @Test
    fun `incr은 숫자가 아닌 값에 대해 실패한다`() {
        repository.set("key", "not-a-number".toByteArray())

        val result = repository.incr("key")

        assertThat(result.isFailure).isTrue()
    }

    @Test
    fun `decr은 키가 없으면 0에서 -1로 감소시킨다`() {
        val result = repository.decr("counter")

        assertThat(result.isSuccess).isTrue()
        assertThat(result.getOrNull()).isEqualTo(-1)
    }

    @Test
    fun `decr은 기존 숫자 값을 1 감소시킨다`() {
        repository.set("counter", "10".toByteArray())

        val result = repository.decr("counter")

        assertThat(result.isSuccess).isTrue()
        assertThat(result.getOrNull()).isEqualTo(9)
    }

    @Test
    fun `incrBy는 지정한 값만큼 증가시킨다`() {
        repository.set("counter", "10".toByteArray())

        val result = repository.incrBy("counter", 5)

        assertThat(result.isSuccess).isTrue()
        assertThat(result.getOrNull()).isEqualTo(15)
    }

    @Test
    fun `incrBy는 음수로 감소시킬 수 있다`() {
        repository.set("counter", "10".toByteArray())

        val result = repository.incrBy("counter", -3)

        assertThat(result.isSuccess).isTrue()
        assertThat(result.getOrNull()).isEqualTo(7)
    }

    @Test
    fun `setNx는 키가 없을 때만 값을 설정하고 true를 반환한다`() {
        val result = repository.setNx("key", "value".toByteArray())

        assertThat(result).isTrue()
        assertThat(repository.get("key")?.toString(Charsets.UTF_8)).isEqualTo("value")
    }

    @Test
    fun `setNx는 키가 이미 존재하면 false를 반환하고 값을 변경하지 않는다`() {
        repository.set("key", "original".toByteArray())

        val result = repository.setNx("key", "new-value".toByteArray())

        assertThat(result).isFalse()
        assertThat(repository.get("key")?.toString(Charsets.UTF_8)).isEqualTo("original")
    }

    @Test
    fun `setNx는 만료된 키에 대해서는 값을 설정한다`() {
        repository.set("key", "old".toByteArray())
        repository.expire("key", 1)

        clock.advanceBy(1001)

        val result = repository.setNx("key", "new".toByteArray())

        assertThat(result).isTrue()
        assertThat(repository.get("key")?.toString(Charsets.UTF_8)).isEqualTo("new")
    }

    @Test
    fun `mGet은 여러 키의 값을 한번에 조회한다`() {
        repository.set("key1", "value1".toByteArray())
        repository.set("key2", "value2".toByteArray())

        val result = repository.mGet(listOf("key1", "key2", "non-existent"))

        assertThat(result).hasSize(3)
        assertThat(result[0]?.toString(Charsets.UTF_8)).isEqualTo("value1")
        assertThat(result[1]?.toString(Charsets.UTF_8)).isEqualTo("value2")
        assertThat(result[2]).isNull()
    }

    @Test
    fun `mSet은 여러 키-값을 한번에 저장한다`() {
        repository.mSet(
            mapOf(
                "key1" to "value1".toByteArray(),
                "key2" to "value2".toByteArray()
            )
        )

        assertThat(repository.get("key1")?.toString(Charsets.UTF_8)).isEqualTo("value1")
        assertThat(repository.get("key2")?.toString(Charsets.UTF_8)).isEqualTo("value2")
    }

    @Test
    fun `mSet은 기존 TTL을 제거한다`() {
        repository.set("key", "old".toByteArray())
        repository.expire("key", 1)

        repository.mSet(mapOf("key" to "new".toByteArray()))

        clock.advanceBy(1001)

        assertThat(repository.get("key")?.toString(Charsets.UTF_8)).isEqualTo("new")
    }

    @Test
    fun `pttl은 남은 시간을 밀리초 단위로 반환한다`() {
        repository.set("key", "value".toByteArray())
        repository.expire("key", 10)

        clock.advanceBy(3000)

        val pttl = repository.pttl("key")
        assertThat(pttl).isBetween(6000L, 7000L)
    }

    @Test
    fun `pttl은 TTL이 설정되지 않은 키에 대해 -1을 반환한다`() {
        repository.set("key", "value".toByteArray())

        assertThat(repository.pttl("key")).isEqualTo(-1)
    }

    @Test
    fun `pttl은 존재하지 않는 키에 대해 -2를 반환한다`() {
        assertThat(repository.pttl("non-existent")).isEqualTo(-2)
    }

    @Test
    fun `pexpire는 밀리초 단위로 TTL을 설정한다`() {
        repository.set("key", "value".toByteArray())

        val result = repository.pexpire("key", 500)

        assertThat(result).isEqualTo(1)
        assertThat(repository.get("key")).isNotNull

        clock.advanceBy(501)

        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `pexpire는 존재하지 않는 키에 대해 0을 반환한다`() {
        val result = repository.pexpire("non-existent", 1000)

        assertThat(result).isEqualTo(0)
    }

    @Test
    fun `pexpire에 0이하 값을 전달하면 키가 삭제된다`() {
        repository.set("key", "value".toByteArray())

        val result = repository.pexpire("key", 0)

        assertThat(result).isEqualTo(1)
        assertThat(repository.get("key")).isNull()
    }

    @Test
    fun `keys는 패턴과 일치하는 모든 키를 반환한다`() {
        repository.set("user:1", "a".toByteArray())
        repository.set("user:2", "b".toByteArray())
        repository.set("order:1", "c".toByteArray())

        val result = repository.keys("user:*")

        assertThat(result).containsExactlyInAnyOrder("user:1", "user:2")
    }

    @Test
    fun `keys는 모든 키를 조회할 수 있다`() {
        repository.set("key1", "a".toByteArray())
        repository.set("key2", "b".toByteArray())

        val result = repository.keys("*")

        assertThat(result).containsExactlyInAnyOrder("key1", "key2")
    }

    @Test
    fun `keys는 물음표 패턴을 지원한다`() {
        repository.set("key1", "a".toByteArray())
        repository.set("key2", "b".toByteArray())
        repository.set("key10", "c".toByteArray())

        val result = repository.keys("key?")

        assertThat(result).containsExactlyInAnyOrder("key1", "key2")
    }

    @Test
    fun `keys는 만료된 키를 제외한다`() {
        repository.set("key1", "a".toByteArray())
        repository.set("key2", "b".toByteArray())
        repository.expire("key1", 1)

        clock.advanceBy(1001)

        val result = repository.keys("*")

        assertThat(result).containsExactly("key2")
    }

    @Test
    fun `scan은 커서 기반으로 키를 순회한다`() {
        repeat(5) { i ->
            repository.set("key$i", "value$i".toByteArray())
        }

        val (nextCursor, keys) = repository.scan(0, null, 3)

        assertThat(keys).hasSize(3)
        assertThat(nextCursor).isNotEqualTo(0)
    }

    @Test
    fun `scan은 모든 키를 순회하면 커서 0을 반환한다`() {
        repository.set("key1", "a".toByteArray())
        repository.set("key2", "b".toByteArray())

        val (nextCursor, keys) = repository.scan(0, null, 10)

        assertThat(keys).hasSize(2)
        assertThat(nextCursor).isEqualTo(0)
    }

    @Test
    fun `scan은 패턴 매칭을 지원한다`() {
        repository.set("user:1", "a".toByteArray())
        repository.set("user:2", "b".toByteArray())
        repository.set("order:1", "c".toByteArray())

        val (_, keys) = repository.scan(0, "user:*", 10)

        assertThat(keys).containsExactlyInAnyOrder("user:1", "user:2")
    }

    @Test
    fun `scan은 빈 데이터베이스에서 빈 결과를 반환한다`() {
        val (nextCursor, keys) = repository.scan(0, null, 10)

        assertThat(keys).isEmpty()
        assertThat(nextCursor).isEqualTo(0)
    }
}
