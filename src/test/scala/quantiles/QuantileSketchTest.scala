package quantiles


import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest._


class QuantileSketchTest extends FlatSpec with Matchers {


  "sketch" should "not crash or have a huge error for simple stream" in {
    val k = 40000
    val m = new QuantileSketch[Int](k)
    val memSize = m.itemMemSize()
    val n = 1000000
    (1 to n / 2).foreach { x =>
      val i = x //Random.nextInt(100000)
      m.update(i)
      m.update(n + 1 - i)
    }
    var q = 20.0

    val err = (m.quantiles(q.toInt) zip (1 until q.toInt))
      .map{x =>
        math.abs(x._1.toDouble / n - x._2 / q)
      }.max
    err < 0.05 should be (true)
    err*memSize<10 should be(true)
  }

  "sketch" should "not crash or have a huge error for merged" in {
    val k = 400
    var lastMerge = new QuantileSketch[Int](k)
    val n = 100000
    val merges = 10
    for (merge <- 0 until merges) {
      var newMerge = new QuantileSketch[Int](k)
      (1 to n / 2).foreach { i =>
        newMerge.update(i+merge*n)
        newMerge.update(merge*n+n + 1 - i)
      }
      lastMerge = lastMerge.merge(newMerge)
    }

    var q = 20.0

    val err = (lastMerge.quantiles(q.toInt) zip (1 until q.toInt))
      .map{x =>
        math.abs(x._1.toDouble / (n*merges) - x._2 / q)
      }.max
    err < 0.05 should be (true)
    err*k<10 should be(true)
  }


}