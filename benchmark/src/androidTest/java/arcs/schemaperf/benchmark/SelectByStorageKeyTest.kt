package arcs.schemaperf.benchmark

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.LargeTest
import org.junit.Test
import org.junit.runner.RunWith

@LargeTest
@RunWith(AndroidJUnit4::class)
class SelectByStorageKeyTest : BaseSchemaTest() {
    @Test
    fun testSelections_jsonBlob() {
        benchmarkSelection(blobJsonHelper, "jsonBlob")
    }

    @Test
    fun testSelections_jsonBlob_indexed() {
        benchmarkSelection(indexedBlobJsonHelper, "jsonBlob_indexed")
    }

    @Test
    fun testSelections_protoBlob() {
        benchmarkSelection(blobProtoHelper, "protoBlob")
    }

    @Test
    fun testSelections_protoBlob_indexed() {
        benchmarkSelection(indexedBlobProtoHelper, "protoBlob_indexed")
    }

    @Test
    fun testSelections_normalized() {
        benchmarkSelection(normalizedHelper, "normalized")
    }

    @Test
    fun testSelections_normalizedLess() {
        benchmarkSelection(normalizedLessHelper, "normalized_less")
    }
}
