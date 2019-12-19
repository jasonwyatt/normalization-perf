package arcs.schemaperf.schema

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import arcs.schemaperf.model.Person
import arcs.schemaperf.model.StorageKey

class DatabaseHelper(
    private val context: Context,
    private val dbName: String,
    private val schema: Schema
) : SQLiteOpenHelper(context, dbName, 1, PARAMS) {

    override fun onCreate(db: SQLiteDatabase) = Unit

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) = Unit

    companion object {
        private var PARAMS = SQLiteDatabase.OpenParams.Builder()
            .setOpenFlags(SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING)
            .build()
    }

    fun reset(database: SQLiteDatabase = writableDatabase) {
        database.use {
            schema.dropTables(it)
            schema.createTables(it)
        }
    }

    fun getSize(): Long {
        writableDatabase.rawQuery("PRAGMA wal_checkpoint(FULL)", arrayOf()).use {  }
        return schema.getSize(context, dbName)
    }

    fun insertPeople(sequence: Sequence<Pair<StorageKey, Person>>) =
        writableDatabase.use { schema.insertPeople(it, sequence) }

    fun insertPerson(storageKey: StorageKey, person: Person): Long =
        writableDatabase.use { schema.insertPerson(it, storageKey, person) }
}
