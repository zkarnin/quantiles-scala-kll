package quantiles.util

import org.scalatest.{FlatSpec, Matchers}
import scala.util.Random


class ColumnWiseSortTest extends FlatSpec with Matchers {

  "columnWiseSort" should "Correctly sort" in {
    for (m <- Array(8,16,60,200,1000)) {
      val n = 1000

      val arr = (1 to n).map{_ =>
        (1 to m).map{_ =>
          Random.nextDouble()
        }.toArray
      }.toArray
      ColumnWiseSort.sortAll(arr,m,n)

      for (col <- 0 until m) {
        val colArr = (0 until n).map{row =>
          arr(row)(col)
        }.toArray
        val sortedColArr = colArr.sorted

        assert(colArr.sameElements(sortedColArr))
      }
    }
  }



}