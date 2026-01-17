package redis.error

class WrongTypeException(
    val key: String,
    val expectedType: String,
    val actualType: String,
) : RuntimeException("Key '$key' holds wrong type: expected $expectedType but got $actualType")
