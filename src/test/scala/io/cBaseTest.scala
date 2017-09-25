package rajkumar.org.io

import org.scalatest._
import scala.util.{Success,Failure}
import scala.concurrent.ExecutionContext.Implicits.global

class cBaseTest extends FlatSpec with Matchers {

  val TestHost  = "ec2-4-3-8-6.compute-1.amazonaws.com" // ds-jenkins
  val Bucket    = "default"
  cBase.connect( TestHost )
  val store = new cBase( Bucket )

  val k   = "keyx"
  val js1 = """ {"string":"a", "int":10, "array":[1,2,3,5] } """
  val js2 = """ {"string":"b", "int":11, "array":[10,2,3,5] } """
  val b   = B( 10, "bar", Seq("a","b",""), Map("k1"-> 1, "k2"-> 2), A("a"))


  "CBase sync put, get" should "return the original value" in {
    assume( store.isReady )
    store.put( k, js1 )
    val res:String = store.get( k )
    res shouldBe js1
  }

  "CBase sync put, prepend, get" should "return concatenated value" in {
    assume( store.isReady )
    store.put( k, js1 )
    store.prepend( k, js2 )
    val res:String = store.get( k )
    res shouldBe (js2 + js1)
  }

  "CBase delete" should "remove key" in {
    assume( store.isReady )
    store.put( k, js1 )
    store.delete( k )
    val res:Boolean = store.exists( k )
    res shouldBe false
  }

  "CBase async put, get" should "return the original value" in {
    assume( store.isReady )
    store.aput( k, js1 )
    Thread.sleep( 1000 )
    store.aget( k ) onComplete {
      case Success( s ) => s shouldBe js1
      case Failure( e ) => e.printStackTrace
    }
  }

  "CBase async put, append get" should "return the concatenated value" in {
    assume( store.isReady )
    store.aput( k, js1 )
    store.aprepend( k, js2 )
    Thread.sleep( 1000 )
    store.aget( k ) onComplete {
      case Success(s) => s shouldBe (js2 + js1)
      case Failure( e ) => e.printStackTrace
    }
  }

  "CBase async get on invalid key" should "return an empty string" in {
    assume( store.isReady )
    store.aget( "foobar" ) onComplete {
      case Success(s) => s shouldBe ""
      case Failure( e ) => e.printStackTrace
    }
  }

  "CBase sync put/get on object" should "return an original value" in {
    assume( store.isReady )
    store.put[B](k, b)
    val res = store.get[B]( k ).get
    store.delete( k )
    res shouldBe b
  }
}
