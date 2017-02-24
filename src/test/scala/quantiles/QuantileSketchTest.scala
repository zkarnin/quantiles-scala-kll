package quantiles


import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest._


class QuantileSketchTest extends FlatSpec with Matchers {


  "sketch" should "not crash or have a huge error for simple stream" in {
    for (k <- Array(100,1000,50000)) {
      val qSketch = new QuantileSketch[Int](k)
      val memSize = qSketch.itemMemSize()
      val streamLen = 1000000
      (1 to streamLen / 2).foreach { i =>
        // test on zoom-in sketch: 1, n, 2, n-1, ...
        qSketch.update(i)
        qSketch.update(streamLen + 1 - i)
      }

      // number of quantiles we need
      var qCount = 20.0

      val err = (qSketch.quantiles(qCount.toInt) zip (1 until qCount.toInt))
        .map{ case (estimateQuantile, qIndex) =>
          val normalizedEstimate = estimateQuantile.toDouble / streamLen
          val trueQuantile = qIndex / qCount
          math.abs(normalizedEstimate - trueQuantile)
        }.max
      err*memSize<20 should be(true)
    }
  }

  "sketch" should "not crash or have a huge error for merged stream" in {
    for (k <- Array(100,1000,50000)) {
      var lastMerge = new QuantileSketch[Int](k)
      val partitionLen = 100000
      val merges = 10
      for (merge <- 0 until merges) {
        var newMerge = new QuantileSketch[Int](k)
        (1 to partitionLen / 2).foreach { i =>
          newMerge.update(i+merge*partitionLen)
          newMerge.update(merge*partitionLen+partitionLen + 1 - i)
        }
        lastMerge = lastMerge.merge(newMerge)
      }

      // number of quantiles we need
      var qCount = 20.0

      val err = (lastMerge.quantiles(qCount.toInt) zip (1 until qCount.toInt))
        .map{case (estimateQuantile, qIndex) =>
          val normalizedEstimate = estimateQuantile.toDouble / (partitionLen*merges)
          val trueQuantile = qIndex / qCount
          math.abs(normalizedEstimate - trueQuantile)
        }.max
      err*k<10 should be(true)
    }
  }


}