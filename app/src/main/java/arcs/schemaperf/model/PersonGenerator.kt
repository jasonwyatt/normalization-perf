package arcs.schemaperf.model

import android.content.Context
import java.io.BufferedReader
import java.io.InputStreamReader
import kotlin.random.Random

class PersonGenerator(context: Context, private val random: Random) {
    val firstNames: List<String> =
        BufferedReader(InputStreamReader(context.assets.open("firstnames.csv"))).use {
            it.readLines()
        }
    val lastNames: List<String> =
        BufferedReader(InputStreamReader(context.assets.open("lastnames.csv"))).use {
            it.readLines()
        }
    val homeTowns: List<String> =
        BufferedReader(InputStreamReader(context.assets.open("hometowns.csv"))).use {
            it.readLines()
        }

    fun generate(num: Int): Sequence<Pair<StorageKey, Person>> {
        val keys = Array(num) { createStorageKey() }
        var current = 0
        return generateSequence {
            keys[current++] to createPerson(random.nextInt(1, 6), keys)
        }.take(num)
    }

    fun createPerson(numFriends: Int, possibleFriends: Array<StorageKey>): Person {
        val friends = mutableListOf<StorageKey>()
        repeat(numFriends) { friends.add(possibleFriends.random(random)) }
        return Person(
            firstNames.random(random),
            lastNames.random(random),
            random.nextInt(18, 80),
            homeTowns.random(random),
            friends
        )
    }
}
