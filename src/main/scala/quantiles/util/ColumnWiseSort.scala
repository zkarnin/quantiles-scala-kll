package quantiles.util

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by zkarnin on 12/2/16.
  */
object ColumnWiseSort {
  def sortall[T](xs: Array[Array[T]], colNum: Int, len: Int)(implicit ordering: Ordering[T],
                                                             ct: ClassTag[T]): Unit = {
    if (len > 32) {
      (0 until colNum).foreach { col => ColumnWiseHeapSort.sort(xs, col, len) }
    } else {
      (0 until colNum).foreach { col => ColumnWiseQuickSort.sort(xs, col, len) }
    }
  }

  def main(args: Array[String]): Unit = {
    val a = Array(
      Array(1,2,3,4),
      Array(-1,23,42,324),
      Array(234,10,55,23),
      Array(-129,4,5,58)
    )

    sortall(a,4,4)
    println(a.map(b=>b.mkString("\t")).mkString("\n"))
    println("ok...")

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
    //    println("ok...")
    //    println((0 until colLen).map{i=>b(i)(0)}.mkString("\n"))

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
    //    println((0 until colLen).map{i=>b(i)(0)}.mkString("\n"))
  }

}
