package quantiles.util

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by zkarnin on 12/2/16.
  */
object ColumnWiseSort {
  def sortAll[T](X: Array[Array[T]], colNum: Int, len: Int)(implicit ordering: Ordering[T],
                                                            ct: ClassTag[T]): Unit = {
    // For small lengths, heap sort seems to work better
    if (len < 32) {
      (0 until colNum).foreach { col => ColumnWiseHeapSort.sort(X, col, len) }
    } else {
      (0 until colNum).foreach { col => ColumnWiseQuickSort.sort(X, col, len) }
    }
  }

  def compareHeapsortQuickSort(): Unit = {
    // compare times for heapsort and quicksort
    val colLen = 32
    val colNum = 100000
    var b = (0 until colLen).map{i=>
      (0 to colNum).map(j => Random.nextDouble()).toArray
    }.toArray
    var start = System.nanoTime()
    for (i <- 0 until colNum) {
      ColumnWiseHeapSort.sort(b,i,colLen)
    }
    var end = System.nanoTime()
    val heapdiff = end-start

    b = (0 until colLen).map{i=>
      (0 to colNum).map(j => Random.nextDouble()).toArray
    }.toArray
    start = System.nanoTime()
    for (i <- 0 until colNum) {
      ColumnWiseQuickSort.sort(b,i,colLen)
    }
    end = System.nanoTime()
    val qdiff = end-start

    println(s"colLen = $colLen, colNum = $colNum: heap=$heapdiff, quick=$qdiff, heap-q=${heapdiff-qdiff}, heap/q=${heapdiff.toDouble/qdiff}")
  }

}
