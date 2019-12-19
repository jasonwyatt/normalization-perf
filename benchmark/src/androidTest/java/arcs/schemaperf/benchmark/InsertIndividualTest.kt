package arcs.schemaperf.benchmark

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.LargeTest
import org.junit.Test
import org.junit.runner.RunWith

@LargeTest
@RunWith(AndroidJUnit4::class)
class InsertIndividualTest : BaseSchemaTest() {
    @Test
    fun testInsertions_jsonBlob() {
        benchmarkIndividualInsertion(blobJsonHelper, "jsonBlob")
    }

    @Test
    fun testInsertions_jsonBlob_indexed() {
        benchmarkIndividualInsertion(indexedBlobJsonHelper, "jsonBlob_indexed")
    }

    @Test
    fun testInsertions_protoBlob() {
        benchmarkIndividualInsertion(blobProtoHelper, "protoBlob")
    }

    @Test
    fun testInsertions_protoBlob_indexed() {
        benchmarkIndividualInsertion(indexedBlobProtoHelper, "protoBlob_indexed")
    }

    @Test
    fun testInsertions_normalized() {
        benchmarkIndividualInsertion(normalizedHelper, "normalized")
    }
}
