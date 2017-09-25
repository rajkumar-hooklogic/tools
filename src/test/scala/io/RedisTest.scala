package rajkumar.org.io

import org.scalatest._

class RedisTest extends FlatSpec with Matchers {

  val TestHost  = "ec2-4-3-8-6.compute-1.amazonaws.com" // jenkins
  val Namespace    = "test"
  Redis.connect( TestHost )
  val store   = new Redis( Namespace )
  val ostore  = new Redis( Namespace )

  val k   = "keyx"
  val ky  = "keyy"
  val js1 = """{"string":"a", "int":10, "array":[1,2,3,5] }"""
  val js2 = """{"string":"b", "int":11, "array":[10,2,3,5] }"""
  val b   = B( 10, "bar", Seq("a","b",""), Map("k1"-> 1, "k2"-> 2), A("a"))

  "Redis sync put, get" should "return the original value" in {
    assume( store.isReady )
    store.put( k, js1 )
    val res:String = store.get( k )
    res shouldBe js1
  }

  "Redis sync put, prepend, get" should "return concatenated value" in {
    assume( store.isReady )
    store.put( k, js1 )
    store.prepend( k, js2 )
    val res:String = store.get( k )
    res shouldBe (js1 + js2)
  }

  "Redis delete" should "remove key" in {
    assume( store.isReady )
    store.put( k, js1 )
    store.delete( k )
    val res:Boolean = store.exists( k )
    res shouldBe false
  }

  "Redis sync put/get on object" should "return an original value" in {
    assume( ostore.isReady )
    ostore.put[B](k, b)
    val res = ostore.get[B]( k ).get
    ostore.delete( k )
    res shouldBe b
  }

  "Redis keys()" should "return a just-added key" in {
    assume( ostore.isReady )
    store.put( ky, "keytest" )
    store.keys().toStream should contain ( ky )
    store.delete( k )
    store.delete( ky )
    store.keys().toStream should contain noneOf( k, ky )
  }


}
