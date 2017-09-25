package rajkumar.org.text

import org.scalatest._

class bitToolsTest extends FlatSpec with Matchers {

  val sa = "the spruce goose ran up to the moose; it was all a ruse"
  val sb = "spruce up, the ruse told the poles"

  "distance( x, x )" should "return 1.0" in {
    1.0 shouldBe (bitTools.distance( sa, sa ))
  }

  "distance( a, b )" should "return viable value" in {
    val result = bitTools.distance( sa, sb )
    result should be > 0.2f
    result should be < 0.3f
  }

  "distance()" should "be symmetric" in {
    val resab = bitTools.distance( sa, sb )
    val resba = bitTools.distance( sb, sa )
    resab shouldEqual resba
  }

}

