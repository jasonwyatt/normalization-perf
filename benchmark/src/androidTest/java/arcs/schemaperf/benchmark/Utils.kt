package arcs.schemaperf.benchmark

import android.util.Log
import androidx.benchmark.junit4.BenchmarkRule

fun log(message: String) {
    Log.i("Benchmark", message)
}

fun BenchmarkRule.Scope.log(message: () -> String) {
    runWithTimingDisabled { log(message()) }
}
