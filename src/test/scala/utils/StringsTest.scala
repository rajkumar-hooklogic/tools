package rajkumar.org.utils

import org.scalatest._

class StringsTest extends FlatSpec with Matchers {

  "field()" should "extract a key's value" in {
    val tres = Strings.field("key1:value1, key2:value2", "key2" )
    val eres = "value2"
    tres shouldBe eres
  }
  it should "extract a quoted key-value pair" in {
    val tres = Strings.field("""key1:value1, "key2":"value2", a:b""", "key2" )
    val eres = """"value2""""
    tres shouldBe eres
  }
  it should "extract the last key-value pair" in {
    val tres = Strings.field("""{key1:value1, "key2":"value2"}""", "key2" )
    val eres = """"value2""""
    tres shouldBe eres
  }

  "qfield()" should "extract a quoted key-value pair" in {
    val tres = Strings.qfield("""key1:value1, "key2":"value2", a:b""", "key2" )
    val eres = "value2"
    tres shouldBe eres
  }

  "trim()" should " trim quotes correctly" in {
    val tres = Strings.trim("""""foo"""", '"' )
    val eres = "foo"
    tres shouldBe eres
  }

}
