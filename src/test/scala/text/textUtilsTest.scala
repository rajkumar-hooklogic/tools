package rajkumar.org.text

import org.scalatest._

class textUtilsTest extends FlatSpec with Matchers {

  "tokenize( doc)" should "tokenize, unstop and stem" in {
    val doc = "This is a strange mattter of milking money out of unused user's names."
    val tokens = Seq("strang", "mattter", "milk", "monei", "unus", "user", "name")

    tokens should equal (textUtils.tokenize( doc ))
  }
}

