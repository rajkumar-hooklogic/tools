package rajkumar.org.io.aspike

import org.scalatest._
import scala.util.{Success,Failure}
import scala.concurrent.ExecutionContext.Implicits.global

case class A( a:String )
case class B( i:Int, s:String, l:Seq[String], m:Map[String,Int], b:A )

class SpikeTest extends FlatSpec with Matchers {

  val TestHost  = "ec2-1-7-1-1.compute-1.amazonaws.com" // jenkins
  val Bucket    = "test"
  Spike.connect( TestHost )
  val store   = new SSpike( Bucket )
  val ostore  = new Ospike( Bucket )

  val k   = "keyx"
  val ky  = "keyy"
  val ks  = "keys"
  val js1 = """ {"string":"a", "int":10, "array":[1,2,3,5] } """
  val js2 = """ {"string":"b", "int":11, "array":[10,2,3,5] } """
  val b   = B( 10, "bar", Seq("a","b",""), Map("k1"-> 1, "k2"-> 2), A("a"))

  "SSpike sync put, get" should "return the original value" in {
    assume( store.isReady )
    store.put( k, js1 )
    val res:String = store.get( k )
    res shouldBe js1
  }

  "SSpike sync put, prepend, get" should "return concatenated value" in {
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

  "Ospike sync put/get on object" should "return an original value" in {
    assume( ostore.isReady )
    ostore.put[B](k, b)
    val res = ostore.get[B]( k ).get
    ostore.delete( k )
    res shouldBe b
  }

  "Ospike keys()" should "return a just-added key" in {
    assume( ostore.isReady )
    store.put( ky, "keytest" )
    store.keys().toStream should contain ( ky )
    store.delete( k )
    store.delete( ky )
    store.keys().toStream should contain noneOf( k, ky )
  }

  "Ospike scan()" should "return a just-added key" in {
    assume( ostore.isReady )
    store.put( ks, "scantest" )
    var cnt = 0
    val sf = ( k:String, v:String ) => { cnt = cnt + 1; "" }
    store.scan( sf )
    cnt should be >= 1
  }

  "SSpike async put, get" should "return the original value" in {
    assume( store.isReady )
    store.aput( k, js1 )
    Thread.sleep( 1000 )
    store.aget( k ) onComplete {
      case Success( s ) => s shouldBe js1
      case Failure( e ) => e.printStackTrace
    }
  }

  "SSpike async put, append get" should "return the concatenated value" in {
    assume( store.isReady )
    store.aput( k, js1 )
    store.aprepend( k, js2 )
    Thread.sleep( 1000 )
    store.aget( k ) onComplete {
      case Success(s) => s shouldBe (js2 + js1)
      case Failure( e ) => e.printStackTrace
    }
  }

  "SSpike async get on invalid key" should "return an empty string" in {
    assume( store.isReady )
    store.aget( "foobar" ) onComplete {
      case Success(s) => s shouldBe ""
      case Failure( e ) => e.printStackTrace
    }
  }

}
