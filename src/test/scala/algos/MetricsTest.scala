package rajkumar.org.algos

import org.scalatest._

class MetricsTest extends FlatSpec with Matchers {

  val ta = Seq( 1, 2, 3, 4, 5, 6, 7, 8, 9 )

  "KendallTau" should "return 0 for f(a,a)" in {
    val tres = Metrics.KendallTau( ta, ta )
    tres shouldBe 0.0
  }
  it should "return 0.0 for f( a, a ++ a)" in {
    val tres = Metrics.KendallTau( ta, ta ++ ta )
    tres shouldBe 0.0
  }
  it should "return 1.0 for f(a,a.reverse)" in {
    val tres = Metrics.KendallTau( ta, ta.reverse )
    tres shouldBe 1.0
  }
  it should "return small value for f(a, shift(a))" in {
    val tres = Metrics.KendallTau( ta, ta.tail :+ ta.head )
    tres should be < 0.5
  }
  it should "return 2.0 for f(a,Seq())" in {
    val tres = Metrics.KendallTau( ta, Seq() )
    tres shouldBe 2.0
  }
  it should "return 2.0 for f(a, negative(a))" in {
    val tres = Metrics.KendallTau( ta, ta.map( x => 0) )
    tres shouldBe 2.0
  }

  "HaversineDistance" should "compute distances correctly" in {
    val (nylat, nylon) = (40.7128, -74.0059)
    val (sflat, sflon) = (37.77493, -122.41942) 
    val NysfDist =  4129094
    val actualDist = Metrics.HaversineDistance( nylat, nylon, sflat, sflon ).toInt
    actualDist shouldBe NysfDist
  }

}
