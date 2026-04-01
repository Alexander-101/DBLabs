package aab.db4

import java.sql.Timestamp
import java.time.Instant

fun <T : Comparable<T>> Pair<T, T>.sorted(): Pair<T, T> {
    if (first <= second) {
        return this
    } else return Pair(second, first)
}

inline fun <T> Iterable<T>.forEachWithActionBetween(actionBetween: () -> Unit, action: (T) -> Unit) {
    val iterator = iterator()
    if (!iterator.hasNext())
        return
    action(iterator.next())
    while (iterator.hasNext()) {
        actionBetween()
        action(iterator.next())
    }
}

fun Instant.toSqlTimestamp(): Timestamp {
    return Timestamp.from(this)
}