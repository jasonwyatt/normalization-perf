package arcs.schemaperf.benchmark

import android.content.Context
import androidx.benchmark.junit4.BenchmarkRule
import androidx.benchmark.junit4.measureRepeated
import androidx.test.core.app.ApplicationProvider
import arcs.schemaperf.model.PersonGenerator
import arcs.schemaperf.model.StorageKey
import arcs.schemaperf.schema.DatabaseHelper
import arcs.schemaperf.schema.blob.BlobSchema
import arcs.schemaperf.schema.indexedblob.IndexedBlobSchema
import arcs.schemaperf.schema.normalized.NormalizedSchema
import org.junit.After
import org.junit.Before
import org.junit.Rule
import kotlin.random.Random

abstract class BaseSchemaTest {
    @get:Rule
    val benchmarkRule = BenchmarkRule()

    lateinit var context: Context
        private set
    lateinit var blobJsonHelper: DatabaseHelper
        private set
    lateinit var blobProtoHelper: DatabaseHelper
        private set
    lateinit var indexedBlobJsonHelper: DatabaseHelper
        private set
    lateinit var indexedBlobProtoHelper: DatabaseHelper
        private set
    lateinit var normalizedHelper: DatabaseHelper
        private set

    @Before
    fun setUp() {
        context = ApplicationProvider.getApplicationContext()
        blobJsonHelper =
            DatabaseHelper(context, "blob_json", BlobSchema(true))
        blobProtoHelper =
            DatabaseHelper(context, "blob_proto", BlobSchema(false))
        indexedBlobJsonHelper =
            DatabaseHelper(context, "indexed_blob_json", IndexedBlobSchema(true))
        indexedBlobProtoHelper =
            DatabaseHelper(context, "indexed_blob_proto", IndexedBlobSchema(false))
        normalizedHelper =
            DatabaseHelper(context, "normalized", NormalizedSchema())
    }

    @After
    fun tearDown() {
        blobJsonHelper.close()
        blobProtoHelper.close()
        indexedBlobJsonHelper.close()
        indexedBlobProtoHelper.close()
        normalizedHelper.close()

        context.dataDir.resolve("databases").listFiles().forEach {
            log("${it.absolutePath}: Size: ${it.length()} bytes")
            it.delete()
        }
    }

    fun benchmarkBulkInsertion(helper: DatabaseHelper, name: String) {
        val personGenerator = PersonGenerator(context, Random(1337))
        val count = 1000
        var iteration = 0
        benchmarkRule.measureRepeated {
            runWithTimingDisabled { helper.reset() }
            helper.insertPeople(personGenerator.generate(count))
            iteration++
        }
        log("$name: completed $iteration iterations of $count entity insertions.")
        log("$name: size: ${helper.getSize()} bytes.")
    }

    fun benchmarkIndividualInsertion(helper: DatabaseHelper, name: String) {
        val personGenerator = PersonGenerator(context, Random(1337))
        val count = 100
        var iteration = 0
        helper.reset()
        benchmarkRule.measureRepeated {
            personGenerator.generate(count)
                .forEach { helper.insertPerson(it.first, it.second) }
            iteration++
        }
        log("$name: completed $iteration iterations resulting in (${iteration * count} entities).")
        log("$name: size: ${helper.getSize()} bytes.")
    }

    fun benchmarkSelection(helper: DatabaseHelper, name: String) {
        val random = Random(1337)
        val personGenerator = PersonGenerator(context, random)
        val count = 1000
        helper.reset()
        val storageKeys = mutableListOf<StorageKey>()
        log("$name: populating database with $count entities")
        helper.insertPeople(personGenerator.generate(count).onEach { storageKeys.add(it.first) })
        log("$name: $count insertions complete")
        var iteration = 0
        val itemsPerSelect = 100
        benchmarkRule.measureRepeated {
            val keysToUse = runWithTimingDisabled {
                (0 until itemsPerSelect).map { storageKeys.random() }
            }
            keysToUse.forEach { helper.findPerson(it).also { runWithTimingDisabled { log("$it") } } }
            iteration++
        }
        log("$name: completed $iteration iterations resulting in (${iteration * itemsPerSelect} selections).")
    }
}
