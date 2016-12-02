package quantiles

import scala.reflect.ClassTag
import scala.util.Random

/**
  * A quantile sketcher whose output is half the size of its input
  *
  * @param bufferSize size of the memory buffer. Error is roughly 1/size
  * @param someValue Arbitrary item
  * @tparam T type of the items being sketched. There should an ordering
  *           over this item type
  */
class Compactor[T](var bufferSize: Int,
                   val someValue: T)
                  (implicit ordering: Ordering[T],
                   ct: ClassTag[T]) extends Serializable {
  var buffer: Array[T] = Array.fill[T](bufferSize)(someValue)
  private var items=0
  private var newBufferSize = bufferSize
  var fractionalSize : Double = bufferSize

  /**
    *
    * @return number of items currently in the buffer
    */
  def getItemCount : Int = items

  /**
    *
    * @return items currently in the buffer
    */
  def getItems : Array[T] = buffer.slice(0,items)


  /**
    * reset the compactor
    *
    * @param bufSize new size for the buffer
    */
  def reset(bufSize: Int) : Unit = {
    bufferSize=bufSize
    items=0
    newBufferSize=bufSize
    buffer = Array.fill[T](bufferSize)(someValue)
    fractionalSize = bufSize
  }

  private def compact : Array[T] = {
    val len = items - (items % 2)
    val offset = if (Random.nextBoolean()) 1 else 0
    val sBuff = buffer.slice(0,len).sorted
    val output = (offset until len by 2).map(sBuff(_)).toArray
    buffer(0)=buffer(math.max(items-1,0))
    items = items % 2
    if (newBufferSize<bufferSize) {
      val newBuffer = Array.fill[T](bufferSize)(someValue)
      newBuffer(0)=buffer(0)
      buffer=newBuffer
    }
    output
  }

  /**
    * update the sketch with an item
    *
    * @param item item to be added to the sketch
    * @return if buffer is filled, an array of items representing those
    *         previously in the buffer is given
    */
  def update(item: T) : Array[T] = {
    buffer(items)=item
    items+=1
    if (items==bufferSize)
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
