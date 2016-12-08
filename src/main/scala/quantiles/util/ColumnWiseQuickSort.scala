package quantiles.util

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by zkarnin on 12/2/16.
  */
object ColumnWiseQuickSort {

  def sort[T](xs: Array[Array[T]],col: Int, len:Int)(implicit ordering: Ordering[T],
                              ct: ClassTag[T]) {
    def swap(i: Int, j: Int) {
      val t = xs(i)(col); xs(i)(col) = xs(j)(col); xs(j)(col) = t
    }
    def sort1(low: Int, high: Int) {

      //val pivot = xs((l + r) / 2)(col)
      val pivot = xs(if (high>low+1) Random.nextInt(high-low)+low else 0)(col)

      var i = low; var j = high-1
      while (i < j) {

        while (ordering.compare(xs(i)(col),pivot)<0) i += 1
        while (j>= low && ordering.compare(xs(j)(col),pivot)>=0) j -= 1
        if (i <= j) {
          swap(i, j)
          i += 1
          j -= 1
        }
      }
      if (low < i-1) sort1(low, i)
      if (i < high-1) sort1(i, high)
    }
    sort1(0, len)
  }
}
