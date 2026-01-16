package redis.storage

class FakeClock(
    private var currentTime: Long = 0L,
) : Clock {
    override fun currentTimeMillis(): Long = currentTime

    fun advanceBy(millis: Long) {
        currentTime += millis
    }

    fun set(millis: Long) {
        currentTime = millis
    }
}
