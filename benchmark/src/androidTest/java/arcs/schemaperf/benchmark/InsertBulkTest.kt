package arcs.schemaperf.benchmark

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.LargeTest
import org.junit.Test
import org.junit.runner.RunWith

@LargeTest
@RunWith(AndroidJUnit4::class)
class InsertBulkTest : BaseSchemaTest() {
    @Test
    fun testInsertions_jsonBlob() {
        benchmarkBulkInsertion(blobJsonHelper, "jsonBlob")
    }

    @Test
    fun testInsertions_jsonBlob_indexed() {
        benchmarkBulkInsertion(indexedBlobJsonHelper, "jsonBlob_indexed")
    }

    @Test
    fun testInsertions_protoBlob() {
        benchmarkBulkInsertion(blobProtoHelper, "protoBlob")
    }

    @Test
    fun testInsertions_protoBlob_indexed() {
        benchmarkBulkInsertion(indexedBlobProtoHelper, "protoBlob_indexed")
    }

    @Test
    fun testInsertions_normalized() {
        benchmarkBulkInsertion(normalizedHelper, "normalized")
    }
}
