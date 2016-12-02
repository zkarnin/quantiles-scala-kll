package quantiles.vectors

import org.scalatest.{FlatSpec, Matchers}



class VectorQuantileSketchTest extends FlatSpec with Matchers {


  "sketch" should "not crash or have a huge error for simple stream" in {
    val k = 40000
    val m = new VectorQuantileSketch[Int](k)
    val memSize = m.itemMemSize()
    val n = 100000
    (1 to n / 2).foreach { x =>
      val i = x //Random.nextInt(100000)
      m.update(Array(i,(((2*i-1)*13) % n)+1,2*i-1,n-2*i+1 ))
      m.update(Array(n + 1 - i,(((2*i)*13) % n)+1,2*i,n-2*i))
    }
    var q = 20.0

    val errs = (0 until 4).map { col =>
      (m.quantiles(q.toInt, col) zip (1 until q.toInt))
        .map { x =>
          math.abs(x._1.toDouble / n - x._2 / q)
        }.max
    }
    println(errs.mkString(","))
    println(memSize)
    println(errs.map(_*memSize).mkString(","))

    assert(errs.forall(err=> err < 0.05))
    assert(errs.forall(err=>err*memSize<20))
  }

  "sketch" should "not crash or have a huge error for merged" in {
    val k = 400
    var lastMerge = new VectorQuantileSketch[Int](k)
    val memSize = lastMerge.itemMemSize()

    val n = 10000
    val merges = 10
    for (merge <- 0 until merges) {
      var newMerge = new VectorQuantileSketch[Int](k)
      (1 to n / 2).foreach { i =>

        newMerge.update(Array(merge*n+i, merge*n+(((2*i-1)*13) % n)+1,merge*n+2*i-1,merge*n+n-2*i+1 ))
        newMerge.update(Array(merge*n+n + 1 - i,merge*n+(((2*i)*13) % n)+1,merge*n+2*i,merge*n+n-2*i))
      }
      lastMerge = lastMerge.merge(newMerge)
    }

    var q = 20.0

    val errs = (0 until 4).map { col =>
      (lastMerge.quantiles(q.toInt, col) zip (1 until q.toInt))
        .map { x =>
          math.abs(x._1.toDouble / (merges*n) - x._2 / q)
        }.max
    }
    println(errs.mkString(","))
    println(memSize)
    println(errs.map(_*memSize).mkString(","))

    assert(errs.forall(err=> err < 0.05))
    assert(errs.forall(err=>err*memSize<20))

  }


}