package rajkumar.org.utils

// Often used Statistical functions
object Stats {

  def mean[T]( item:Traversable[T] )( implicit n:Numeric[T] ) =
    n.toDouble(item.sum) / item.size.toDouble

  def variance[T]( items:Traversable[T] )( implicit n:Numeric[T] ):Double = {
    val itemMean = mean(items)
    val count = items.size
    val sumOfSquares = items.foldLeft( 0.0d )( (total,item) => {
        val itemDbl = n.toDouble( item )
        val square = math.pow( itemDbl - itemMean, 2 )
        total + square
      }
    )
    sumOfSquares / count.toDouble
  }

  def stddev[T]( items:Traversable[T] )( implicit n:Numeric[T] ):Double =
    math.sqrt(variance(items))

}
