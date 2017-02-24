package quantiles.vectors

import org.scalatest.{FlatSpec, Matchers}



class VectorQuantileSketchTest extends FlatSpec with Matchers {


  "sketch" should "not crash or have a huge error for simple stream" in {
    for (k <- Array(100,1000,50000)) {
      val qSketch = new VectorQuantileSketch[Int](k)
      val memSize = qSketch.itemMemSize()
      val streamLen = 100000
      (1 to streamLen / 2).foreach { i =>
        // 4 sequences:
        //  (1) ordered: 1,2,...,n
        //  (2) zoom in: 1,n,2,n-1,...
        //  (3) skip list: 13, 26, .., 13*i % n, ..  (13 is chosen since its a prime so all numbers are covered)
        //  (4) reverse ordered n,n-1,...,1
        qSketch.update(Array(i,(((2*i-1)*13) % streamLen)+1,2*i-1,streamLen-2*i+1 ))
        qSketch.update(Array(streamLen + 1 - i,(((2*i)*13) % streamLen)+1,2*i,streamLen-2*i))
      }

      // number of quantiles we need
      val qCount = 20.0

      val errors = (0 until 4).map { column =>
        (qSketch.quantiles(qCount.toInt, column) zip (1 until qCount.toInt))
          .map { case (estimateQuantile, qIndex) =>
            val normalizedEstimate = estimateQuantile.toDouble / streamLen
            val trueQuantile = qIndex / qCount
            math.abs(normalizedEstimate - trueQuantile)
          }.max
      }
      assert(errors.forall(err=>err*memSize<20))
    }


  }

  "sketch" should "not crash or have a huge error for merged stream" in {
    for (k <- Array(100,1000,50000)) {
      var lastMerge = new VectorQuantileSketch[Int](k)
      val memSize = lastMerge.itemMemSize()

      val partitionLen = 10000
      val merges = 10
      for (merge <- 0 until merges) {
        var newMerge = new VectorQuantileSketch[Int](k)
        (1 to partitionLen / 2).foreach { i =>
          // same streams as in test above
          newMerge.update(Array(merge*partitionLen+i, merge*partitionLen+(((2*i-1)*13) % partitionLen)+1,merge*partitionLen+2*i-1,merge*partitionLen+partitionLen-2*i+1 ))
          newMerge.update(Array(merge*partitionLen+partitionLen + 1 - i,merge*partitionLen+(((2*i)*13) % partitionLen)+1,merge*partitionLen+2*i,merge*partitionLen+partitionLen-2*i))
        }
        lastMerge = lastMerge.merge(newMerge)
      }

      // number of quantiles we need
      val qCount = 20.0

      val errors = (0 until 4).map { column =>
        (lastMerge.quantiles(qCount.toInt, column) zip (1 until qCount.toInt))
          .map { case (estimateQuantile, qIndex) =>
            val normalizedEstimate = estimateQuantile.toDouble / (merges*partitionLen)
            val trueQuantile = qIndex / qCount
            math.abs(normalizedEstimate - trueQuantile)
          }.max
      }

      assert(errors.forall(err=>err*memSize<20))
    }


  }


}