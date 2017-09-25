package rajkumar.org.utils

import org.scalatest._

class StatsTest extends FlatSpec with Matchers {

  val s = Seq( 1,2,3,4,5,6,7,8,9,10,11)
  "mean" should "compute correctly" in {
    val tres = Stats.mean( s  )
    val eres = 6.0
    tres shouldBe eres
  }
  "variance" should "compute correctly" in {
    val tres = Stats.variance( s  )
    val eres = 10.0
    tres shouldBe eres
  }
  "stddev" should "compute correctly" in {
    val tres = Stats.stddev( s  )
    val eres = 3.1622776601683795
    tres shouldBe eres
  }

}
