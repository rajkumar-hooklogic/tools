package rajkumar.org.aws

import org.scalatest._
import scala.util.{Success,Failure}
import scala.concurrent.ExecutionContext.Implicits.global

case class A( a:String )
case class B( i:Int, s:String, l:Seq[String], m:Map[String,Int], b:A )

class DynamoTest extends FlatSpec with Matchers {

  Dynamo.connect( "" )
  val Bucket    = "Test"
  val store   = new SDynamo( Bucket )
  val ostore  = store

  val k   = "keyx"
  val ky  = "keyy"
  val ks  = "keys"
  val js1 = """ {"string":"a", "int":10, "array":[1,2,3,5] } """
  val js2 = """ {"string":"b", "int":11, "array":[10,2,3,5] } """
  val b   = B( 10, "bar", Seq("a","b",""), Map("k1"-> 1, "k2"-> 2), A("a"))

  "SDynamo sync put, get" should "return the original value" in {
    assume( store.isReady )
    store.put( k, js1 )
    val res:String = store.get( k )
    res shouldBe js1
  }

  "SDynamo sync put, prepend, get" should "return concatenated value" in {
    assume( store.isReady )
    store.put( k, js1 )
    store.prepend( k, js2 )
    val res:String = store.get( k )
    res shouldBe (js2 + js1)
  }

  "delete" should "remove key" in {
    assume( store.isReady )
    store.put( k, js1 )
    store.delete( k )
    val res:Boolean = store.exists( k )
    res shouldBe false
  }

  "Dynamo sync put/get on object" should "return an original value" in {
    assume( ostore.isReady )
    ostore.put[B](k, b)
    val res = ostore.get[B]( k ).get
    ostore.delete( k )
    res shouldBe b
  }

  "Dynamo keys()" should "return a just-added key" in {
    assume( ostore.isReady )
    store.put( ky, "keytest" )
    store.keys().toStream should contain ( ky )
    store.delete( k )
    store.delete( ky )
    store.keys().toStream should contain noneOf( k, ky )
  }

  "SDynamo async put, get" should "return the original value" in {
    assume( store.isReady )
    store.aput( k, js1 )
    Thread.sleep( 1000 )
    store.aget( k ) onComplete {
      case Success( s ) => s shouldBe js1
      case Failure( e ) => e.printStackTrace
    }
  }

  "SDynamo async put, append get" should "return the concatenated value" in {
    assume( store.isReady )
    store.aput( k, js1 )
    store.aprepend( k, js2 )
    Thread.sleep( 1000 )
    store.aget( k ) onComplete {
      case Success(s) => s shouldBe (js2 + js1)
      case Failure( e ) => e.printStackTrace
    }
  }

  "SDynamo async get on invalid key" should "return an empty string" in {
    assume( store.isReady )
    store.aget( "foobar" ) onComplete {
      case Success(s) => s shouldBe ""
      case Failure( e ) => e.printStackTrace
    }
  }

}
