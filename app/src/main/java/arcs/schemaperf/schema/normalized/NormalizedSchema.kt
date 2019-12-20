package arcs.schemaperf.schema.normalized

import android.database.sqlite.SQLiteDatabase
import arcs.schemaperf.model.Person
import arcs.schemaperf.model.StorageKey
import arcs.schemaperf.model.createStorageKey
import arcs.schemaperf.schema.Schema

class NormalizedSchema : Schema {
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

            val collectionStorageKeyId = db.maybeInsertStorageKey(createStorageKey())
            db.compileStatement(INSERT_FRIENDS_COLLECTION)
                .apply {
                    bindLong(1, collectionStorageKeyId)
                    bindLong(2, personTypeId)
                }.executeInsert()

            val friendInsertStatement = db.compileStatement(INSERT_FRIEND_INTO_COLLECTION)
            person.friends.forEach {
                friendInsertStatement.bindLong(1, collectionStorageKeyId)
                friendInsertStatement.bindLong(2, db.maybeInsertStorageKey(it))
                friendInsertStatement.executeInsert()
            }

            // Assume we already know the field ids, since we could pre-load those in the drivers.
            val firstNameFieldId = 1L
            val lastNameFieldId = 2L
            val ageFieldId = 3L
            val hometownFieldId = 4L
            val friendsFieldId = 5L

            val firstNameValueId = db.maybeInsertText(person.firstName)
            val lastNameValueId = db.maybeInsertText(person.lastName)
            val ageValueId = db.maybeInsertInt(person.age)
            val hometownValueId = db.maybeInsertText(person.hometown)

            val fieldStatement = db.compileStatement(INSERT_PERSON_FIELD_VALUE)
            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, firstNameFieldId)
            fieldStatement.bindLong(3, stringTypeId)
            fieldStatement.bindLong(4, firstNameValueId)
            fieldStatement.executeInsert()

            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, lastNameFieldId)
            fieldStatement.bindLong(3, stringTypeId)
            fieldStatement.bindLong(4, lastNameValueId)
            fieldStatement.executeInsert()

            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, ageFieldId)
            fieldStatement.bindLong(3, numberTypeId)
            fieldStatement.bindLong(4, ageValueId)
            fieldStatement.executeInsert()

            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, hometownFieldId)
            fieldStatement.bindLong(3, stringTypeId)
            fieldStatement.bindLong(4, hometownValueId)
            fieldStatement.executeInsert()

            fieldStatement.bindLong(1, storageKeyId)
            fieldStatement.bindLong(2, friendsFieldId)
            fieldStatement.bindLong(3, collectionTypeId)
            fieldStatement.bindLong(4, collectionStorageKeyId)
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

    private fun SQLiteDatabase.maybeInsertInt(number: Int): Long {
        val keyId = compileStatement(INSERT_NUMBER)
            .apply { bindLong(1, number.toLong()) }
            .executeInsert()
        if (keyId >= 0) return keyId
        return compileStatement(SELECT_NUMBER_ID)
            .apply { bindLong(1, number.toLong()) }
            .simpleQueryForLong()
    }

    private fun SQLiteDatabase.maybeInsertText(text: String): Long {
        val keyId = compileStatement(INSERT_TEXT)
            .apply { bindString(1, text) }
            .executeInsert()
        if (keyId >= 0) return keyId
        return compileStatement(SELECT_TEXT_ID)
            .apply { bindString(1, text) }
            .simpleQueryForLong()
    }

    private val params = arrayOf("")
    override fun findPerson(db: SQLiteDatabase, storageKey: StorageKey): Person? {
        params[0] = storageKey
        try {
            db.beginTransaction()
            val storageKeyId = db.maybeInsertStorageKey(storageKey)
            val (person, friends, friendsCollectionId) = db.rawQuery(
                SELECT_PERSON_FIELDS + storageKeyId, null
            ).use {
                if (!it.moveToNext()) return null

                val friends = mutableListOf<StorageKey>()
                Triple(
                    Person(
                        it.getString(0),
                        it.getString(1),
                        it.getInt(2),
                        it.getString(3),
                        friends
                    ),
                    friends,
                    it.getInt(4)
                )
            }

            params[0] = friendsCollectionId.toString()
            db.rawQuery(SELECT_FRIENDS + friendsCollectionId, null).use {
                while (it.moveToNext()) {
                    friends.add(it.getString(0))
                }
            }
            db.setTransactionSuccessful()
            return person
        } finally {
            db.endTransaction()
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

        private val INSERT_NUMBER = """
            INSERT OR IGNORE INTO number_primitive_values (value) values (?) 
        """.trimIndent()

        private val SELECT_NUMBER_ID = """
            SELECT id FROM number_primitive_values WHERE value = ?;
        """.trimIndent()

        private val INSERT_TEXT = """
            INSERT OR IGNORE INTO text_primitive_values (value) values (?) 
        """.trimIndent()

        private val SELECT_TEXT_ID = """
            SELECT id FROM text_primitive_values WHERE value = ?;
        """.trimIndent()

        private val SELECT_PERSON_FIELDS = """
            SELECT 
                first_name_text.value AS first_name,
                last_name_text.value AS last_name,
                hometown_text.value AS hometown_name,
                age_number.value AS age,
                friends.value_id AS friend_collection_id
            FROM
                entities AS e
            LEFT JOIN
                field_values AS first_name_value 
                ON first_name_value.entity_storage_key_id = e.storage_key_id 
                AND first_name_value.field_id = 1
            LEFT JOIN
                text_primitive_values AS first_name_text 
                ON first_name_text.id = first_name_value.value_id
            LEFT JOIN
                field_values AS last_name_value 
                ON last_name_value.entity_storage_key_id = e.storage_key_id 
                AND last_name_value.field_id = 2
            LEFT JOIN
                text_primitive_values AS last_name_text 
                ON last_name_text.id = last_name_value.value_id
            LEFT JOIN
                field_values AS age_value 
                ON age_value.entity_storage_key_id = e.storage_key_id 
                AND age_value.field_id = 3
            LEFT JOIN
                number_primitive_values AS age_number 
                ON age_number.id = age_value.value_id
            LEFT JOIN
                field_values AS hometown_value 
                ON first_name_value.entity_storage_key_id = e.storage_key_id 
                AND hometown_value.field_id = 4
            LEFT JOIN
                text_primitive_values AS hometown_text 
                ON hometown_text.id = hometown_value.value_id
            LEFT JOIN
                field_values AS friends 
                ON friends.entity_storage_key_id = e.storage_key_id 
                AND friends.field_id = 5
            WHERE
                e.storage_key_id =  
        """.trimIndent()

        private val SELECT_FRIENDS = """
            SELECT
                member_keys.storage_key
            FROM
                collection_entries AS ce
            JOIN
                storage_keys AS member_keys ON member_keys.id = ce.entity_storage_key_id
            WHERE ce.collection_storage_key_id =  
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
                value_id INTEGER
            );
            
            CREATE INDEX 
                field_values_by_entity_storage_key 
            ON field_values (entity_storage_key_id, value_id);

            CREATE TABLE text_primitive_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value TEXT NOT NULL UNIQUE
            );
            
            CREATE INDEX text_primitive_value_index ON text_primitive_values (value);

            CREATE TABLE number_primitive_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value REAL NOT NULL UNIQUE
            );
            
            CREATE INDEX number_primitive_value_index ON number_primitive_values (value);
        """.trimIndent().split("\n\n")

        private val DROP_TABLES = """
            DROP INDEX IF EXISTS type_name_index;
            DROP INDEX IF EXISTS storage_key_index;
            DROP INDEX IF EXISTS collection_entries_collection_storage_key_index;
            DROP INDEX IF EXISTS field_names_by_parent_type;
            DROP INDEX IF EXISTS field_values_by_entity_storage_key;
            DROP INDEX IF EXISTS text_primitive_value_index;
            DROP INDEX IF EXISTS number_primitive_value_index;
            DROP TABLE IF EXISTS types;
            DROP TABLE IF EXISTS storage_keys;
            DROP TABLE IF EXISTS entities;
            DROP TABLE IF EXISTS collections;
            DROP TABLE IF EXISTS collection_entries;
            DROP TABLE IF EXISTS fields;
            DROP TABLE IF EXISTS field_values;
            DROP TABLE IF EXISTS text_primitive_values;
            DROP TABLE IF EXISTS number_primitive_values;
        """.trimIndent().split("\n")
    }
}
