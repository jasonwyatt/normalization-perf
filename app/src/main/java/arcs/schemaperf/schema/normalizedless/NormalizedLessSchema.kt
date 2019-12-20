package arcs.schemaperf.schema.normalizedless

import android.database.sqlite.SQLiteDatabase
import arcs.schemaperf.model.Person
import arcs.schemaperf.model.StorageKey
import arcs.schemaperf.model.createStorageKey
import arcs.schemaperf.schema.Schema

class NormalizedLessSchema : Schema {
    override fun createTables(db: SQLiteDatabase) {
        CREATE_TABLES.forEach { db.execSQL(it) }
    }

    override fun dropTables(db: SQLiteDatabase) {
        DROP_TABLES.forEach { db.execSQL(it) }
    }

    override fun insertPeople(db: SQLiteDatabase, sequence: Sequence<Pair<StorageKey, Person>>) {
        try {
            db.beginTransaction()
            sequence.forEach { insertPerson(db, it.first, it.second) }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
    }

    override fun insertPerson(db: SQLiteDatabase, storageKey: StorageKey, person: Person): Long {
        return try {
            db.beginTransaction()

            val storageKeyId = db.maybeInsertStorageKey(storageKey)
            // assume we already know the type values, since we could pre-load those in the drivers.
            val personTypeId = 1L
            val stringTypeId = 2L
            val numberTypeId = 3L
            val collectionTypeId = 4L
            db.compileStatement(INSERT_PERSON_ENTITY)
                .apply {
                    bindLong(1, storageKeyId)
                    bindLong(2, personTypeId)
                }.executeInsert()

            // Assume we already know the field ids, since we could pre-load those in the drivers.
            val firstNameFieldId = 1L
            val lastNameFieldId = 2L
            val ageFieldId = 3L
            val hometownFieldId = 4L
            val friendsFieldId = 5L

            val fieldStatement = db.compileStatement(INSERT_PERSON_FIELD_VALUE)
            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, firstNameFieldId)
            fieldStatement.bindLong(3, stringTypeId)
            fieldStatement.bindString(4, person.firstName)
            fieldStatement.executeInsert()

            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, lastNameFieldId)
            fieldStatement.bindLong(3, stringTypeId)
            fieldStatement.bindString(4, person.lastName)
            fieldStatement.executeInsert()

            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, ageFieldId)
            fieldStatement.bindLong(3, numberTypeId)
            fieldStatement.bindLong(4, person.age.toLong())
            fieldStatement.executeInsert()

            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, hometownFieldId)
            fieldStatement.bindLong(3, stringTypeId)
            fieldStatement.bindString(4, person.hometown)
            fieldStatement.executeInsert()

            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, friendsFieldId)
            fieldStatement.bindLong(3, collectionTypeId)
            fieldStatement.bindString(4, person.friends.joinToString(","))
            fieldStatement.executeInsert()

            db.setTransactionSuccessful()
            storageKeyId
        } finally {
            db.endTransaction()
        }
    }

    private val storageKeyIds = mutableMapOf<String, Long>()

    private fun SQLiteDatabase.maybeInsertStorageKey(storageKey: StorageKey): Long {
        storageKeyIds[storageKey]?.let { return it }
        val keyId = compileStatement(INSERT_STORAGE_KEY)
            .apply { bindString(1, storageKey) }
            .executeInsert()
        if (keyId >= 0) return keyId
        return compileStatement(SELECT_STORAGE_KEY_ID)
            .apply { bindString(1, storageKey) }
            .simpleQueryForLong().also { storageKeyIds[storageKey] = it }
    }

    override fun findPerson(db: SQLiteDatabase, storageKey: StorageKey): Person? {
        val storageKeyId = db.maybeInsertStorageKey(storageKey)
        return db.rawQuery(
            SELECT_PERSON_FIELDS + storageKeyId, null
        ).use {
            if (!it.moveToNext()) return null

            Person(
                it.getString(0),
                it.getString(1),
                it.getInt(2),
                it.getString(3),
                it.getString(4).split(",")
            )
        }
    }

    companion object {
        private val INSERT_STORAGE_KEY = """
            INSERT OR IGNORE INTO storage_keys (storage_key) VALUES (?)
        """.trimIndent()

        private val SELECT_STORAGE_KEY_ID = """
            SELECT id FROM storage_keys WHERE storage_key = ?;
        """.trimIndent()

        private val INSERT_PERSON_ENTITY = """
            INSERT INTO entities (storage_key_id, type_id) VALUES (?, ?)
        """.trimIndent()

        private val INSERT_FRIENDS_COLLECTION = """
            INSERT INTO collections (storage_key_id, type_id) VALUES (?, ?)
        """.trimIndent()

        private val INSERT_FRIEND_INTO_COLLECTION = """
            INSERT INTO collection_entries (collection_storage_key_id, entity_storage_key_id) VALUES (?, ?)
        """.trimIndent()

        private val INSERT_PERSON_FIELD_VALUE = """
            INSERT INTO field_values (entity_storage_key_id, field_id, type_id, value_id) values (?, ?, ?, ?)
        """.trimIndent()

        private val SELECT_PERSON_FIELDS = """
            SELECT 
                first_name.value AS first_name,
                last_name.value AS last_name,
                hometown.value AS hometown_name,
                age.value AS age,
                friends.value AS friend_collection_id
            FROM
                entities AS e
            LEFT JOIN
                field_values AS first_name
                ON first_name.entity_storage_key_id = e.storage_key_id 
                AND first_name.field_id = 1
            LEFT JOIN
                field_values AS last_name
                ON last_name.entity_storage_key_id = e.storage_key_id 
                AND last_name.field_id = 2
            LEFT JOIN
                field_values AS age
                ON age.entity_storage_key_id = e.storage_key_id 
                AND age.field_id = 3
            LEFT JOIN
                field_values AS hometown
                ON hometown.entity_storage_key_id = e.storage_key_id 
                AND hometown.field_id = 4
            LEFT JOIN
                field_values AS friends 
                ON friends.entity_storage_key_id = e.storage_key_id 
                AND friends.field_id = 5
            WHERE
                e.storage_key_id =  
        """.trimIndent()

        private val CREATE_TABLES = """
            CREATE TABLE types (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                is_primitive INTEGER NOT NULL DEFAULT 0
            );
            
            CREATE INDEX type_name_index ON types (name, id);
            
            INSERT INTO types (name, is_primitive) values 
                ("Person", 0), ("string", 1), ("number", 1), ("collection", 0);

            CREATE TABLE storage_keys (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                storage_key TEXT UNIQUE NOT NULL
            ); 
            
            CREATE INDEX storage_key_index ON storage_keys (storage_key, id);

            CREATE TABLE entities (
                storage_key_id INTEGER NOT NULL,
                type_id INTEGER NOT NULL,
                PRIMARY KEY (storage_key_id)
            ) WITHOUT ROWID;

            CREATE TABLE collections (
                storage_key_id INTEGER NOT NULL PRIMARY KEY,
                type_id INTEGER NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE collection_entries (
                collection_storage_key_id INTEGER NOT NULL,
                entity_storage_key_id INTEGER NOT NULL
            );
            
            CREATE INDEX 
                collection_entries_collection_storage_key_index 
            ON collection_entries (collection_storage_key_id);

            CREATE TABLE fields (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                parent_type_id INTEGER NOT NULL,
                name TEXT NOT NULL
            );
            
            CREATE INDEX field_names_by_parent_type ON fields (parent_type_id, name);
            
            INSERT INTO fields (parent_type_id, name) VALUES 
                (1, "first_name"), (1, "last_name"), (1, "age"), (1, "hometown"), (1, "friends");

            CREATE TABLE field_values (
                entity_storage_key_id INTEGER NOT NULL,
                field_id INTEGER NOT NULL,
                type_id INTEGER NOT NULL,
                value TEXT
            );
            
            CREATE INDEX 
                field_values_index
            ON field_values (entity_storage_key_id, field_id, value);
        """.trimIndent().split("\n\n")

        private val DROP_TABLES = """
            DROP INDEX IF EXISTS type_name_index;
            DROP INDEX IF EXISTS storage_key_index;
            DROP INDEX IF EXISTS collection_entries_collection_storage_key_index;
            DROP INDEX IF EXISTS field_names_by_parent_type;
            DROP INDEX IF EXISTS field_values_by_entity_storage_key;
            DROP TABLE IF EXISTS types;
            DROP TABLE IF EXISTS storage_keys;
            DROP TABLE IF EXISTS entities;
            DROP TABLE IF EXISTS collections;
            DROP TABLE IF EXISTS collection_entries;
            DROP TABLE IF EXISTS fields;
            DROP TABLE IF EXISTS field_values;
        """.trimIndent().split("\n")
    }
}
