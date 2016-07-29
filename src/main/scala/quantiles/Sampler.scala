package quantiles

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Sampling sketch that samples items (roughly) uniformly from a stream
  *
  * @param someValue an arbitrary item
  * @tparam T the type of items to be sampled
  */
class Sampler[T](val someValue: T)
                (implicit ordering: Ordering[T],
                 ct: ClassTag[T]) extends Serializable{
  var outputWeight = 1L // weight of outputted items
  var curWeight = 0L // weight of items seen from last output
  var reserved = someValue // value of the currently stored item

  /**
    * update the sketch with an item of integer weight
    *
    * @param item the new item
    * @param weight the weight of the item
    * @return Either None or a single item in the case where the overall weight of
    *         items observed is large enough
    */
  def update(item: T, weight: Long) : Option[T] = {
    if (curWeight+weight<=outputWeight) {
      curWeight += weight
      reserved =
        if ( curWeight==1 || (((Random.nextLong() % curWeight)+curWeight)%curWeight) < weight)
          item
        else
          reserved
      if (curWeight == outputWeight) {
        curWeight=0
        Some(reserved)
      } else
        None
    } else {
      // set it up so that newItem is the heavier item
      val newItem = if (curWeight>weight) reserved else item
      reserved = if (curWeight>weight) item else reserved
      val largerWeight = math.max(curWeight,weight)
      curWeight = math.min(curWeight,weight)


      if (  (((Random.nextLong() % outputWeight)+outputWeight)%outputWeight) < largerWeight)
        Some(newItem)
      else None
    }
  }

  /**
    * grow the output weight by a factor of 2
    */
  def grow() : Unit = {outputWeight *= 2}



}