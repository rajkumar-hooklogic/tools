package rajkumar.org.io

import rajkumar.org.utils.Json

import com.couchbase.client.java._
import com.couchbase.client.java.document._
import com.couchbase.client.java.document.json._
import com.couchbase.client.java.query._

import rx.lang.scala.Observable
import rx.lang.scala.JavaConversions._

import java.util.logging._
import scala.util.{Try,Success,Failure}
import scala.concurrent.{Promise,Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._


case class K( id:String )

// couchBase implementation of KSStore and KOStore
class cBase( bname:String ) extends KSStore with KOStore {

  private val bucket  = cBase.cluster.openBucket( bname )
  private val abucket = bucket.async()
  bucket.bucketManager().createN1qlPrimaryIndex( true, false )

  // - General --
  def isReady():Boolean = ! bucket.isClosed

  override def close():Unit = { abucket.close; bucket.close }

  // - Sync ---
  def get( k:String ):String = {
    val o = bucket.get( k, classOf[StringDocument] )
    if( o != null ) o.content else ""
  }

  def put( k:String, v:String):Boolean =
    Try( bucket.upsert( StringDocument.create( k, v )) ).isSuccess

  def prepend( k:String, v:String ):Boolean =
    Try( bucket.prepend( StringDocument.create( k, v ) ) ).isSuccess

  def delete( k:String ):Boolean = Try( bucket.remove( k ) ).isSuccess

  def exists( k:String ):Boolean = bucket.exists( k )

  // All keys in this bucket
  def keys():Iterator[String] = {
    val q = s"SELECT META($bname).id from $bname"
    val res = bucket.query( N1qlQuery.simple( q ) )
    val ks = res.map( r => r.value.toString )
    println( ks.mkString("\t"))
    ks.map( k => Json.fromJS[K]( k ).getOrElse( ek )).map( _.id ).toIterator
  }

  // - Async -----
  // Couchbase uses the Observables pattern and rxJava
  // Since we expect a single value (rather than a stream) convert to Futures

  def aget( k:String ):Future[String] = {
    val o:Observable[StringDocument] = abucket.get( k, classOf[StringDocument] )
    val p = Promise[String]()
    o.subscribe(
      (sd:StringDocument)    => {val a = p.success( sd.content ) },
      (e:Throwable)          => {val a = p.failure( e ) },
      ()                     => {val a = p.trySuccess( "" )  }
    )
    p.future
  }

  def aput( k:String, v:String):Future[Boolean] = {
    val o:Observable[StringDocument] = abucket.upsert(
      StringDocument.create( k, v ) )
    val p = Promise[Boolean]()
    o.subscribe(
      (sd:StringDocument)    => {val a = p.success( true ) },
      (e:Throwable)          => {val a = p.failure( e ) },
      ()                     => {val a = p.trySuccess( true )  }
    )
    p.future
  }

  def aprepend( k:String, v:String ):Future[Boolean] = {
    val o:Observable[StringDocument] = abucket.prepend(
      StringDocument.create( k, v ) )
    val p = Promise[Boolean]()
    o.subscribe(
      (sd:StringDocument)    => {val a = p.success( true ) },
      (e:Throwable)          => {val a = p.failure( e ) },
      ()                     => {val a = p.trySuccess( true )  }
    )
    p.future
  }

  def adelete( k:String ):Future[Boolean] = {
    val o:Observable[StringDocument] = abucket.remove(
      StringDocument.create( k, "" ))
    val p = Promise[Boolean]()
    o.subscribe(
      (sd:StringDocument)    => {val a = p.success( true ) },
      (e:Throwable)          => {val a = p.failure( e ) },
      ()                     => {val a = p.trySuccess( true )  }
    )
    p.future
  }

  // Just use the sync versions in its own thread
  def aexists( k:String ):Future[Boolean] = Future { exists( k ) }

  def akeys():Future[Iterator[String]] = Future { keys() }


  // - KOStore implementation ---
  def get[A:Manifest]( k:String):Option[A] = {
    // val jd:JsonDocument = bucket.get( k, classOf[JsonDocument] )
    val jd:RawJsonDocument = bucket.get( k, classOf[RawJsonDocument] )
    val js = if( jd != null ) jd.content else ""
    Json.fromJS[A]( js )
  }

  // Only works for objects, no strings
  def put[A <: AnyRef:Manifest]( k:String, v:A):Boolean = {
    val js = Json.toJS[A]( v )
    val jo = JsonObject.fromJson( js )
    Try( bucket.upsert( JsonDocument.create( k, jo )) ).isSuccess
  }

  // ----
  val ek = K( "" )
}

// - Static Companion Object (to hold Connection) ---------------
object cBase extends Store {

  Logger.getLogger("com.couchbase.client").setLevel( Level.WARNING )
  var cluster = CouchbaseCluster.create()

  def connect( h:String ):Unit  = {
    cluster.disconnect
    cluster = CouchbaseCluster.create( h )
  }
  def disconnect():Unit         = cluster.disconnect
  def isConnected():Boolean     = Try( cluster.openBucket() ).isSuccess

  def getKSStore( name:String = "default"):KSStore = new cBase( name )
  def getKOStore( name:String = "default"):KOStore = new cBase( name )
}


