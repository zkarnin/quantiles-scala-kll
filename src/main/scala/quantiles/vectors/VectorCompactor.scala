package quantiles.vectors

import scala.reflect.ClassTag
import scala.util.Random

/**
  * A quantile sketcher whose output is half the size of its input.
  * Every input item is a vectorDim-dimensional vector. The quantiles
  * are computed independently per entry. This construction does the same
  * as vectorDim many sketches, one per entry, but uses significantly less
  * overhead
  *
  * @param bufferSize size of the memory buffer. Error is roughly 1/size
  * @param someVector Arbitrary vector of items
  * @tparam T type of the items being sketched. There should an ordering
  *           over this item type
  */
class VectorCompactor[T](var bufferSize: Int,
                         val someVector: Array[T])
                        (implicit ordering: Ordering[T],
                   ct: ClassTag[T]) extends Serializable {
  var buffer: Array[Array[T]] = Array.fill[Array[T]](bufferSize)(someVector)
  // TODO(zkarnin) start with a small size and double it as more items arrive
  private var items=0
  private var newBufferSize = bufferSize
  private var fractionalSize : Double = bufferSize
  private val vectorDim = someVector.length

  def this(bufferSize: Int, vectorDim: Int, someValue: T)(implicit ordering: Ordering[T],
                                         ct: ClassTag[T]) = {
    this(bufferSize, Array.fill[T](vectorDim)(someValue))
  }

  /**
    *
    * @return number of items currently in the buffer
    */
  def getItemCount : Int = items

  /**
    *
    * @return items currently in the buffer
    */
  def getItems : Array[Array[T]] = buffer.slice(0,items)


  /**
    * reset the compactor
    *
    * @param bufSize new size for the buffer
    */
  def reset(bufSize: Int) : Unit = {
    bufferSize=bufSize
    items=0
    newBufferSize=bufSize
    buffer =
      Array.fill[Array[T]](bufferSize)(someVector)
    fractionalSize = bufSize
  }

  private def sortEntrywiseInternally(buffLen: Int) : Unit = {
    for (col <- 0 until vectorDim) {
      // sort vector buff[.][i] inplace
      HeapSort.heapSort(buffer,col,buffLen)
    }
  }

  private def compact : Array[Array[T]] = {
    val len = items - (items % 2)
    val offset = if (Random.nextBoolean()) 1 else 0

    sortEntrywiseInternally(len)
    val output = (offset until len by 2).map(buffer(_)).toArray
    buffer(0)=buffer(math.max(items-1,0))
    items = items % 2
    if (newBufferSize<bufferSize) {
      val newBuffer = Array.fill[Array[T]](newBufferSize)(someVector)
      newBuffer(0)=buffer(0)
      buffer=newBuffer
    }
    output
  }

  /**
    * update the sketch with an item
    *
    * @param item item to be added to the sketch. Should be an array of length vectorDim
    * @return if buffer is filled, an array of items representing those
    *         previously in the buffer is given
    */
  def update(item: Array[T]) : Array[Array[T]] = {
    buffer(items)=item
    items+=1
    if (items>=newBufferSize)
      compact
    else
      null
  }

  /**
    * shrink the size of the buffer
    *
    * @param shrinkFactor the factor to shrink by
    */
  def shrink(shrinkFactor:Double): Unit = {
    fractionalSize *= shrinkFactor
    newBufferSize = math.ceil(fractionalSize/2).toInt*2
  }

}
