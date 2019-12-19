package arcs.schemaperf.model

import java.util.*

typealias StorageKey = String

fun createStorageKey(): StorageKey = UUID.randomUUID().toString()
