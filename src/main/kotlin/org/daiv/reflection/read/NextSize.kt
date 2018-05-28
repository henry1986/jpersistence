package org.daiv.reflection.read

internal data class NextSize<T>(val t: T, val i: Int) {
    fun <R> transform(transformer: (T) -> R): NextSize<R> {
        return NextSize(transformer(t), i)
    }
}
