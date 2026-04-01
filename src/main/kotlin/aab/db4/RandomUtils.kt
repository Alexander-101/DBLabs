package aab.db4

import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.random.Random
import kotlin.random.nextInt
import kotlin.uuid.Uuid.Companion.random


val letters = "qwertyuiopasdfghjklZxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890".toCharArray()


/**
 * Generate random string
 */
fun Random.nextString(lenMin: Int = 4, lenMax: Int = 64, chars: CharArray = letters): String {
    val len = this.nextInt(lenMin, lenMax)
    return (1..len).map { chars[nextInt(chars.size)] }.joinToString("")
}

fun Random.nextBoolean(prob: Double): Boolean {
    return (this.nextDouble() < prob)
}

/**
 * Generate random UUID
 */
fun Random.nextUuid(): UUID {
    return UUID(nextLong(), nextLong())
}

fun Random.nextInstant(
    from: Instant = OffsetDateTime.of(1000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
    to: Instant = OffsetDateTime.of(3000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
): Instant {
    return Instant.ofEpochMilli(this.nextLong(from.toEpochMilli(), to.toEpochMilli()))
}

fun Random.genTimeRanges(
    n: Int,
    from: Instant = OffsetDateTime.of(1000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
    to: Instant = OffsetDateTime.of(3000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(),
): List<Pair<Instant, Instant>> {
    return (1..n).map {
        val f = nextInstant(from, to)
        f to nextInstant(f, to)
    }
}

/**
 * Generate list of cnt random UUIDs
 */
fun Random.genUuids(cnt: Int): List<UUID> {
    return (1..cnt).map { this.nextUuid() }
}

inline fun <T> gen(n: Int, block: () -> T): List<T> {
    return (1..<n).map { block() }
}

inline fun <T> genDistinct(n: Int, crossinline block: () -> T): List<T> {
    return (1..n).asSequence().map { block() }.distinct().toList()
}

/**
 * Generate list of cnt random strings
 */
fun Random.genStrings(cnt: Int, lenMin: Int = 4, lenMax: Int = 64, chars: CharArray = letters): List<String> {
    return (1..cnt).map { this.nextString(lenMin, lenMax, chars) }
}

/**
 * Generate list of cnt random ints
 */
fun Random.genInts(cnt: Int, min: Int, max: Int): List<Int> {
    return (1..cnt).map { this.nextInt(min, max) }
}

/**
 * Generate list of cnt random doubles
 */
fun Random.genDoubles(cnt: Int, min: Double, max: Double): List<Double> {
    return (1..cnt).map { this.nextDouble(min, max) }
}

/**
 * Generate list of cnt elements where each element is random element of samples list
 */
fun <T> Random.chooseFrom(cnt: Int, samples: List<T>): List<T> {
    return (1..cnt).map { samples[nextInt(samples.size)] }
}

/**
 * Choose random element from samples list
 */
fun <T> Random.choose(samples: List<T>): T {
    return samples[nextInt(samples.size)]
}

fun <T> Random.chooseOrNull(samples: List<T>, nullProb: Float = 0.2f): T? {
    return if (this.nextInt(100) > 100 * nullProb) samples[nextInt(samples.size)] else null
}


fun Random.genUuidsOrNulls(cnt: Int, nullProb: Float = 0.2f): List<UUID?> {
    return (1..cnt).map { if (this.nextInt(100) > 100 * nullProb) this.nextUuid() else null }
}

fun Random.genStringsOrNulls(cnt: Int, lenMin: Int = 4, lenMax: Int = 64, chars: CharArray = letters, nullProb: Float = 0.2f): List<String?> {
    return (1..cnt).map { if (this.nextInt(100) > 100 * nullProb) this.nextString(lenMin, lenMax, chars) else null }
}

fun Random.genIntsOrNulls(cnt: Int, min: Int, max: Int, nullProb: Float = 0.2f): List<Int?> {
    return (1..cnt).map { if (this.nextInt(100) > 100 * nullProb) this.nextInt(min, max) else null }
}

fun Random.genDoublesOrNulls(cnt: Int, min: Double, max: Double, nullProb: Float = 0.2f): List<Double?> {
    return (1..cnt).map { if (this.nextInt(100) > 100 * nullProb) this.nextDouble(min, max) else null }
}

fun <T> Random.chooseFromOrNulls(cnt: Int, samples: List<T>, nullProb: Float = 0.2f): List<T?> {
    return (1..cnt).map { if (this.nextInt(100) > 100 * nullProb) samples[nextInt(samples.size)] else null }
}

