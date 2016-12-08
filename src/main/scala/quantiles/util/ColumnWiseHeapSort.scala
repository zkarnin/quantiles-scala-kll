package quantiles.util

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by zkarnin on 12/2/16.
  */
object ColumnWiseHeapSort {
  private def buildMaxHeap[T] (a: Array[Array[T]], col: Int, size: Int)
                              (implicit ordering: Ordering[T],
                               ct: ClassTag[T]): Unit = {
    val hs = size / 2
    for (i <- 0 to hs) {
      maxHeap(a, col, hs - i, size)
    }
  }                                         //> buildMaxHeap: (a: Array[Int], size: Int)Unit

  private def maxHeap [T] (a: Array[Array[T]], col: Int, i: Int, size: Int)
                          (implicit ordering: Ordering[T],
                           ct: ClassTag[T]): Unit = {
    val l = 2*i+1
    val r = 2*i+2
    var m = -1


    m = if (l < size && ordering.compare(a(l)(col),a(i)(col)) >= 0) {l} else i
    m = if (r < size && ordering.compare(a(r)(col),a(m)(col)) >= 0) {r} else m
    if (m != i) {
      swap(a, col, i, m)
      maxHeap(a, col, m, size)
    }
  }                                         //> maxHeap: (a: Array[Int], i: Int, size: Int)Unit

  private def swap[T](a: Array[Array[T]], col: Int, i: Int, j: Int)
                     (implicit ordering: Ordering[T],
                      ct: ClassTag[T]): Unit = {
    val t = a(i)(col)
    a(i)(col) = a(j)(col)
    a(j)(col) = t
  }                                         //> swap: (a: Array[Int], i: Int, j: Int)Unit

  def sort[T](a: Array[Array[T]], col: Int, len: Int)
             (implicit ordering: Ordering[T],
                   ct: ClassTag[T]){
    buildMaxHeap(a, col, len)
    for (i <- len-1 to 0 by -1) {
      swap(a, col, 0, i)
      maxHeap(a, col, 0, i)
    }
  }

}
