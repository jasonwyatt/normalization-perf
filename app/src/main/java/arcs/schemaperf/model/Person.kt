package arcs.schemaperf.model

import kotlinx.serialization.Serializable

@Serializable
data class Person(
    val firstName: String,
    val lastName: String,
    val age: Int,
    val hometown: String,
    val friends: List<StorageKey>
)
