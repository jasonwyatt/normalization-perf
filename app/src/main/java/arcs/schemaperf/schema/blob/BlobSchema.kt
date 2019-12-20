package arcs.schemaperf.schema.blob

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.util.Log
import arcs.schemaperf.model.Person
import arcs.schemaperf.model.StorageKey
import arcs.schemaperf.schema.Schema
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import kotlinx.serialization.protobuf.ProtoBuf
import java.io.File

class BlobSchema(val useJson: Boolean) : Schema {
    val jsonSerializer = Json(JsonConfiguration.Stable)
    val protoSerializer = ProtoBuf()

    override fun createTables(db: SQLiteDatabase) {
        db.execSQL(CREATE_STATEMENT)
    }

    override fun dropTables(db: SQLiteDatabase) {
        db.execSQL(DROP_STATEMENT)
    }

    override fun insertPeople(db: SQLiteDatabase, sequence: Sequence<Pair<StorageKey, Person>>) {
        val statement = db.compileStatement(INSERT_STATEMENT)
        try {
            db.beginTransaction()
            sequence.forEach { (storageKey, person) ->
                statement.bindString(1, storageKey)
                if (useJson) {
                    statement.bindBlob(
                        2,
                        jsonSerializer.stringify(Person.serializer(), person).toByteArray()
                    )
                } else {
                    statement.bindBlob(2, protoSerializer.dump(Person.serializer(), person))
                }
                statement.executeInsert()
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
    }

    override fun insertPerson(db: SQLiteDatabase, storageKey: StorageKey, person: Person): Long {
        val statement = db.compileStatement(INSERT_STATEMENT)
        statement.bindString(1, storageKey)
        if (useJson) {
            statement.bindBlob(
                2,
                jsonSerializer.stringify(Person.serializer(), person).toByteArray()
            )
        } else {
            statement.bindBlob(2, protoSerializer.dump(Person.serializer(), person))
        }
        return statement.executeInsert()
    }

    override fun findPerson(db: SQLiteDatabase, storageKey: StorageKey): Person? =
        db.rawQuery(QUERY_STATEMENT, arrayOf(storageKey)).use {
            if (!it.moveToNext()) return@use null

            val raw = it.getBlob(0)
            if (useJson) {
                jsonSerializer.parse(Person.serializer(), raw.toString(Charsets.UTF_8))
            } else {
                protoSerializer.load(Person.serializer(), raw)
            }
        }

    companion object {
        private val QUERY_STATEMENT = """
            SELECT serialized_data FROM arcs_data WHERE storage_key = ?
        """.trimIndent()

        private val CREATE_STATEMENT = """
            CREATE TABLE arcs_data (
                storage_key TEXT NOT NULL UNIQUE PRIMARY KEY,
                serialized_data BLOB NOT NULL
            )
        """.trimIndent()

        private val DROP_STATEMENT = """
            DROP TABLE IF EXISTS arcs_data
        """.trimIndent()

        private val INSERT_STATEMENT = """
            INSERT INTO arcs_data (storage_key, serialized_data) VALUES (?, ?)
        """.trimIndent()
    }
}
