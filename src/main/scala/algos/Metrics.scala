package rajkumar.org.algos

// Collection of distance metrics

object Metrics {

  // Kendall-Tau distance between two ordered lists
  // (relative number of flips bubble-sort needs to transform a to b)
  // Distance ranges from 0.0 to 1.0 when both lists contain all tokens
  // Values from 1.0 to 2.0 indicate disjoint token-sets
  def KendallTau[A <% Ordered[A]]( a:Seq[A], b:Seq[A] ):Double = {

    val atokens   = a.distinct
    val btokens   = b.distinct
    val tokens    = (atokens union btokens).distinct
    val common    = (atokens intersect btokens)
    val unused    = tokens diff common

    val as        = atokens.filter( common contains _ )
    val bs        = btokens.filter( common contains _ )
    val bindices  = bs.map( x => as.indexOf( x ))

    // measure not clearly defined when duplicate and missing tokens
    // so use only first occurrence and penalize missing tokens max.
    val flips     = bubbleSort( bindices.toArray )
    val penalty   = unused.size * (tokens.size -1)
    val worstCase = tokens.size * (tokens.size -1 ) / 2

    (flips + penalty) * 1.0 / worstCase.max( 1 )
  }

  // - privates ----------------------------------------
  // bubble sort that returns count of flips required to sort
  private def bubbleSort[A <% Ordered[A]]( es:Array[A]):Int = {
    var iteration = 1
    var count = 0
    var done  = false
    while( iteration < es.size && ! done ){
      val flips = bubble( es )
      if( flips == 0 ) done = true
      else {
        iteration = iteration + 1
        count = count + flips
      }
    }
    count
  }
  // bubble a value to appropriate place in list and return flip count
  private def bubble[A <% Ordered[A]]( es:Array[A] ):Int = {
    var flips = 0
    for( i <- 1 until es.size )
      if( es( i-1 ) > es(i)){
        val temp = es(i)
        es(i) = es( i -1 )
        es( i-1) = temp
        flips = flips + 1
      }
      flips
  }

  // ---------
  // Haversine distance (great-circle distance) given two lat-long pairs
	val EarthRadius = 6371000
  def HaversineDistance(lat1:Double, lon1:Double, lat2: Double, lon2:Double,
		r:Int=EarthRadius):Double = {
    val p1 = lat1.toRadians
    val p2 = lat2.toRadians
    val pdelta = (lat2-lat1).toRadians
    val ldelta = (lon2-lon1).toRadians

    val a = Math.sin( pdelta / 2.0 ) * Math.sin( pdelta / 2.0 ) +
    Math.cos( p1 ) * Math.cos( p2 ) *
    Math.sin( ldelta / 2.0 ) * Math.sin( ldelta / 2.0 )
    val c = 2 * Math.atan2( Math.sqrt( a ), Math.sqrt( 1.0 - a ))
    val d = c * EarthRadius
    d
  }




  // -- Testing -----------------------------------------
  def main( args: Array[String]) = {
    val a = Seq( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
    val n = scala.util.Random.shuffle( a )
    println(s"Random:     ${KendallTau( a, n )}")
    println(s"Empty:      ${KendallTau( a, Seq() )}")
    println(s"Unmatched:  ${KendallTau( a, a.map( 0 - _))}")

  }
}
