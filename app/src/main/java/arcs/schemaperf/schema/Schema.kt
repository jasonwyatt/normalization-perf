package arcs.schemaperf.schema

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import arcs.schemaperf.model.Person
import arcs.schemaperf.model.StorageKey

interface Schema {
    fun createTables(db: SQLiteDatabase)
    fun dropTables(db: SQLiteDatabase)
    fun insertPeople(db: SQLiteDatabase, sequence: Sequence<Pair<StorageKey, Person>>)
    fun insertPerson(db: SQLiteDatabase, storageKey: StorageKey, person: Person): Long
    fun getSize(context: Context, dbName: String): Long =
        context.dataDir
            .resolve("databases")
            .resolve(dbName).length()
}
