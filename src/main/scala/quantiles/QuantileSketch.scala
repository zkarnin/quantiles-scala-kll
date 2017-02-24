package quantiles


import scala.reflect.ClassTag

/**
  * Created by zkarnin on 3/24/16.
  */


/**
  * sketching object for computing quantiles of a sequence of numbers
  *
  * @param sketchSize amount of memory that the sketcher can use
  * @param shrinkingFactor internal parameter, determines the shrinking
  *                        of the buffers in the different layers
  *                        (see code for further explanation)
  * @tparam T type of items to be observed in the sketch. The items should
  *           have an order. Specifically Ordering[T] should exist
  */
class QuantileSketch[T](val sketchSize: Int,
                        private val shrinkingFactor: Double = 0.64)
                       (implicit ordering: Ordering[T],
                        ct: ClassTag[T]) extends Serializable{

  private val maxCompactorNum = {
    var n : Double= sketchSize
    var ret=0
    while (n>2) {
      n = n*shrinkingFactor
      ret+=1
    }
    ret
  }
  private var curNumOfCompactors = 0
  private var maxCompactorHeight = 0

  private val compactors = Array.fill[Compactor[T]](maxCompactorNum)(null)
  private var sampler : Sampler[T] = null
  private val numberOfLayersWithMaximalSketchSize=1


  /**
    * update the sketch with a single item
    *
    * @param item new item observed by the sketch
    */
  def update(item: T): Unit = {
    if (maxCompactorHeight-curNumOfCompactors==0) {
      updateToCompactors(item,0)
    } else {
      updateSampler(item,1)
    }
  }

  /**
    *
    * @return memory currently consumed by the sketch
    */
  def itemMemSize(): Int = {
    var k: Double =sketchSize
    var ret : Int= (k*numberOfLayersWithMaximalSketchSize).toInt
    k*= math.pow(shrinkingFactor,numberOfLayersWithMaximalSketchSize)
    while (k>2) {
      ret += math.ceil(k/2).toInt*2
      k*=shrinkingFactor
    }
    ret+1
  }

  private def updateSampler(item:T,weight:Long) = {
    val out = sampler.update(item,weight)
    if (out.isDefined) {
      updateToCompactors(out.get,maxCompactorHeight-curNumOfCompactors)
    }
  }

  private def log2exact(weight: Long) : Int= {
    var w=weight
    if (w<=0)
      return -1
    var ret=0
    while(w>1) {
      if (w%2==1)
        return -1
      w = w >> 1
      ret+=1
    }
    ret
  }

  private def samplerOutputHeight = maxCompactorHeight-curNumOfCompactors
  private def update(item: T, weight: Long): Unit = {
    if (weight < (1L << samplerOutputHeight)) {
      updateSampler(item,weight)
    } else {
      val height = log2exact(weight)
      assert(height>=0)
      updateToCompactors(item,height)
    }
  }

  private def updateToCompactors(item: T, height: Int) : Unit= {
    // check if a new compactors needs to be added
    if (height >= maxCompactorHeight)
      addCompactor(item,height)
    // update. If something got back, feed it
    val output = compactors(height-samplerOutputHeight).update(item)
    if (output!=null) {
      output.foreach{subItem =>
        updateToCompactors(subItem,height+1)
      }
    }
  }


  private def growSampler() = {
    if (sampler==null) {
      sampler = new Sampler[T]
    }
    sampler.grow()
  }

  private def addCompactor(item: T, height: Int): Unit = {
    assert(height== maxCompactorHeight)
    maxCompactorHeight+=1

    // either we assign a new compactor or get rid of the smallest one
    if (curNumOfCompactors < maxCompactorNum) {
      // assign a new compactor
      compactors(curNumOfCompactors) = new Compactor[T](sketchSize,item)
      curNumOfCompactors+=1
    } else {
      // assign new compactor, discard bottom one, but keep its buffer
      val oldCompactorItems = compactors(0).getItems
      val newCompactor = compactors(0)
      for (i <- 1 until curNumOfCompactors) {
        compactors(i-1)=compactors(i)
      }
      newCompactor.reset(sketchSize)
      compactors(curNumOfCompactors-1)=newCompactor

      // handle the sampler: grow its output size, then feed it the buffer
      // of the discarded compactor
      growSampler()
      oldCompactorItems.foreach{it =>
        updateSampler(it, 1L << (maxCompactorHeight-curNumOfCompactors-1))
      }
    }

    // now that the height increased, everybody should shrink
    for (i <- 0 until curNumOfCompactors-numberOfLayersWithMaximalSketchSize-1) {
      compactors(i).shrink(shrinkingFactor)
    }
    if (curNumOfCompactors>=numberOfLayersWithMaximalSketchSize) {
      compactors(curNumOfCompactors-numberOfLayersWithMaximalSketchSize).shrink(
        math.pow(shrinkingFactor,numberOfLayersWithMaximalSketchSize))
    }

  }


  /**
    * merge two sketches into a single one
    *
    * @param that another sketch
    * @return the merged sketch
    */
  def merge(that: QuantileSketch[T]) : QuantileSketch[T] = {
    if (maxCompactorHeight < that.maxCompactorHeight) {
      return that.merge(this)
    }
    if (that.sampler!=null && that.sampler.getCurWeight>0) {
      update(that.sampler.getCurrentReserved , that.sampler.getCurWeight)
    }
    var height = that.maxCompactorHeight-that.curNumOfCompactors
    for (i<- 0 until that.curNumOfCompactors) {
      that
        .compactors(i)
        .getItems
        .foreach{item=>
          update(item,1L << height)
        }
      height +=1
    }
    this
  }

  private def output: Array[(T,Long)] = {
    compactors.slice(0,curNumOfCompactors).zipWithIndex.flatMap{case (compactor,i) =>
      compactor.getItems.map((_,1L << i))
    }
  }

  /**
    * the quantile values of the sketch
    *
    * @param q number of quantiles required
    * @return quantiles 1/q through (q-1)/q
    */
  def quantiles(q: Int) : Array[T] = {
    val sortedItems = output.sortBy(_._1)
    val size = sortedItems.map(_._2).sum
    var nextThresh = size/q
    var curq = 1
    var i=0
    var sumSoFar:Long=0
    val quantiles = Array.fill[T](q-1)(sortedItems(0)._1)


    while (i<sortedItems.length && curq<q) {
      while (sumSoFar < nextThresh) {
        sumSoFar += sortedItems(i)._2
        i+=1
      }
      quantiles(curq -1) = sortedItems(math.min(i,sortedItems.length-1))._1
      curq +=1
      nextThresh = curq * size / q
    }
    quantiles
  }

}

