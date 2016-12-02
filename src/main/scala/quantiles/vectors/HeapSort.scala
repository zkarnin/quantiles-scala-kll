package quantiles.vectors

import scala.reflect.ClassTag

/**
  * Created by zkarnin on 12/2/16.
  */
object HeapSort {
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

  def heapSort[T] (a: Array[Array[T]], col: Int, len: Int)
                  (implicit ordering: Ordering[T],
                   ct: ClassTag[T]){
    buildMaxHeap(a, col, len)
    for (i <- len-1 to 0 by -1) {
      swap(a, col, 0, i)
      maxHeap(a, col, 0, i)
    }
  }

  def main(args: Array[String]): Unit = {
    val a = Array(
      Array(1,2,3,4),
      Array(-1,23,42,324),
      Array(234,10,55,23),
      Array(-129,4,5,58)
    )

    heapSort(a,0,4)
    heapSort(a,1,4)
    heapSort(a,2,4)
    heapSort(a,3,4)
    println(a.map(b=>b.mkString("\t")).mkString("\n"))
  }
}
