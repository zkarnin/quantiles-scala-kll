package quantiles.util

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by zkarnin on 12/2/16.
  */
object ColumnWiseQuickSort {

  def sort[T](arr: Array[Array[T]], col: Int, len:Int)(implicit ordering: Ordering[T],
                                                       ct: ClassTag[T]) {
    def swap(i: Int, j: Int) {
      val t = arr(i)(col)
      arr(i)(col) = arr(j)(col)
      arr(j)(col) = t
    }

    // sort elements in interval [low,high)
    def sort1(low: Int, high: Int) : Unit = {
      if (high <= low+1) return

      val pivot = arr(Random.nextInt(high-low)+low)(col)

      var i = low; var j = high-1
      while (i <= j) {
        while (i<high && ordering.compare(arr(i)(col),pivot)<0) i += 1
        while (j>= low && ordering.compare(arr(j)(col),pivot)>0) j -= 1

        if (i <= j) {
          swap(i, j)
          i += 1
          j -= 1
        }
      }

      if (low < j) sort1(low, j+1)
      if (j <= high) sort1(i, high)
    }

    sort1(0, len)
  }
}
