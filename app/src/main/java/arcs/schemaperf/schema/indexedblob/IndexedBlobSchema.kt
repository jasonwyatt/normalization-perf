package arcs.schemaperf.schema.indexedblob

import android.database.sqlite.SQLiteDatabase
import arcs.schemaperf.model.Person
import arcs.schemaperf.model.StorageKey
import arcs.schemaperf.schema.Schema
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import kotlinx.serialization.protobuf.ProtoBuf

class IndexedBlobSchema(val useJson: Boolean) : Schema {
    val jsonSerializer = Json(JsonConfiguration.Stable)
    val protoSerializer = ProtoBuf()

    override fun createTables(db: SQLiteDatabase) {
        CREATE_TABLES.forEach { db.execSQL(it) }
    }

    override fun dropTables(db: SQLiteDatabase) {
        DROP_TABLES.forEach { db.execSQL(it) }
    }

    override fun insertPeople(db: SQLiteDatabase, sequence: Sequence<Pair<StorageKey, Person>>) {
        val personStatement = db.compileStatement(INSERT_PERSON_STATEMENT)
        val nameStatement = db.compileStatement(INSERT_PERSON_NAME_STATEMENT)
        val ageStatement = db.compileStatement(INSERT_PERSON_AGE_STATEMENT)
        val hometownStatement = db.compileStatement(INSERT_PERSON_HOMETOWN_STATEMENT)
        try {
            db.beginTransaction()

            sequence.forEach { (storageKey, person) ->
                val personBlob = if (useJson) {
                    jsonSerializer.stringify(Person.serializer(), person).toByteArray()
                } else {
                    protoSerializer.dump(Person.serializer(), person)
                }
                personStatement.bindString(1, storageKey)
                nameStatement.bindString(1, storageKey)
                ageStatement.bindString(1, storageKey)
                hometownStatement.bindString(1, storageKey)

                personStatement.bindBlob(2, personBlob)
                nameStatement.bindString(2, person.firstName)
                ageStatement.bindLong(2, person.age.toLong())
                hometownStatement.bindString(2, person.hometown)

                personStatement.executeInsert()
                nameStatement.executeInsert()
                ageStatement.executeInsert()
                hometownStatement.executeInsert()
            }

            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
    }

    override fun insertPerson(db: SQLiteDatabase, storageKey: StorageKey, person: Person): Long {
        val personStatement = db.compileStatement(INSERT_PERSON_STATEMENT)
        val nameStatement = db.compileStatement(INSERT_PERSON_NAME_STATEMENT)
        val ageStatement = db.compileStatement(INSERT_PERSON_AGE_STATEMENT)
        val hometownStatement = db.compileStatement(INSERT_PERSON_HOMETOWN_STATEMENT)

        val personBlob = if (useJson) {
            jsonSerializer.stringify(Person.serializer(), person).toByteArray()
        } else {
            protoSerializer.dump(Person.serializer(), person)
        }
        personStatement.bindString(1, storageKey)
        nameStatement.bindString(1, storageKey)
        ageStatement.bindString(1, storageKey)
        hometownStatement.bindString(1, storageKey)

        personStatement.bindBlob(2, personBlob)
        nameStatement.bindString(2, person.firstName)
        ageStatement.bindLong(2, person.age.toLong())
        hometownStatement.bindString(2, person.hometown)

        val personRowId: Long
        try {
            db.beginTransaction()
            personRowId = personStatement.executeInsert()
            nameStatement.executeInsert()
            ageStatement.executeInsert()
            hometownStatement.executeInsert()
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return personRowId
    }

    companion object {
        private val CREATE_TABLES = """
            CREATE TABLE arcs_data (
                storage_key TEXT NOT_NULL UNIQUE PRIMARY KEY,
                serialized_data BLOB NOT NULL
            );
            
            CREATE TABLE person_names (
                storage_key TEXT NOT_NULL UNIQUE PRIMARY KEY,
                value TEXT NOT NULL
            );
            
            CREATE INDEX person_names_index ON person_names (value);
            
            CREATE TABLE person_ages (
                storage_key TEXT NOT_NULL UNIQUE PRIMARY KEY,
                value NUMBER NOT NULL
            );
            
            CREATE INDEX person_ages_index ON person_ages (value);
            
            CREATE TABLE person_hometowns (
                storage_key TEXT NOT_NULL UNIQUE PRIMARY KEY,
                value TEXT NOT NULL
            );
            
            CREATE INDEX person_hometowns_index ON person_hometowns (value);
        """.trimIndent().split("\n\n")

        private val DROP_TABLES = """
            DROP INDEX IF EXISTS person_names_index;
            
            DROP INDEX IF EXISTS person_ages_index;
            
            DROP INDEX IF EXISTS person_hometowns_index;
            
            DROP TABLE IF EXISTS arcs_data;
            
            DROP TABLE IF EXISTS person_names;
            
            DROP TABLE IF EXISTS person_ages;
            
            DROP TABLE IF EXISTS person_hometowns;
        """.trimIndent().split("\n\n")

        private val INSERT_PERSON_STATEMENT = """
            INSERT INTO arcs_data (storage_key, serialized_data) VALUES (?, ?)
        """.trimIndent()

        private val INSERT_PERSON_NAME_STATEMENT = """
            INSERT INTO person_names (storage_key, value) VALUES (?, ?)
        """.trimIndent()

        private val INSERT_PERSON_AGE_STATEMENT = """
            INSERT INTO person_ages (storage_key, value) VALUES (?, ?)
        """.trimIndent()

        private val INSERT_PERSON_HOMETOWN_STATEMENT = """
            INSERT INTO person_hometowns (storage_key, value) VALUES (?, ?)
        """.trimIndent()
    }
}
