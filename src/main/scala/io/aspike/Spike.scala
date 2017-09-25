package rajkumar.org.io.aspike

import rajkumar.org.io.{Store,KVStore,KSStore,KOStore}

import com.aerospike.client._
import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy}
import com.aerospike.client.policy._
import com.aerospike.client.listener._

import scala.concurrent.{Future, Promise}
import scala.util.{Success,Failure}
import scala.concurrent.ExecutionContext.Implicits.global

import java.util.concurrent.{Executors,ThreadFactory}
import java.lang.{Runnable,Thread}

// Aerospike contants and generic-helpers and tools, convenience-classes
object Spike extends Store {

  // Sync. and Async. client (Note: Find alternative to using null)
  var asc:AerospikeClient = null
  var aasc:AsyncClient    = null

  // - Store implementation --
  def connect( h:String = LocalHost ) = {
    asc       = new AerospikeClient(      h, Port )
    aasc      = new AsyncClient( APolicy, h, Port )
  }
  def disconnect() = {}
  def isConnected():Boolean =  asc.isConnected

  def getKSStore(  name:String = "test"):KSStore   = new SSpike( name )
  def getKOStore(  name:String = "test"):KOStore   = new Ospike( name )

  // - Helpers ----------------
  // Key to string
  def ktos( k:Key ):String = if( k.userKey != null ) k.userKey.toString else ""

  // Record to String/Int/Geo/Generic
  def rtos( r:Record ):String =
    if( r != null && r.getString("string") != null ) r.getString("string")
    else ""

  def rtoi( r:Record ):Int =
    if( r != null && r.getInt("int") != null ) r.getInt("int") else -1

  def rtogeo( r:Record ):String =
    if( r != null && r.getGeoJSON("geo") != null ) r.getGeoJSON("geo") else ""

  def rtoa[A]( r:Record, b:String ):Option[A] =
    if( r == null ) None
    else r.getValue( b ) match {
      case x:A  => Some( x )
      case _    => None
  }

  // Convenience Callback Class
  class SCallback( f:(String,String) => Unit ) extends ScanCallback {
    def scanCallback( k:Key, r:Record ) = f( ktos( k), rtos( r ))
  }

  // - Constants ------------------------------------------
  val LocalHost = "127.0.0.1"
  val Port      = 3000
  val NS        = "ssd"

  // Thread Pool for async client
  val aThreadPool = Executors.newCachedThreadPool( new ThreadFactory() {
    def newThread( r:Runnable ):Thread = {
      val t = new Thread( r )
      t.setDaemon( true )
      t
    }
  })

  // Policy constants
  // Important: unless sendKey is set, string-keys are not available on scans
  val WPolicy   = new WritePolicy; WPolicy.sendKey = true
  val APolicy   = new AsyncClientPolicy()
    APolicy.asyncTaskThreadPool = aThreadPool
  val BPolicy   = new BatchPolicy
  val SPolicy   = new ScanPolicy; SPolicy.concurrentNodes = true;
    SPolicy.priority = Priority.LOW; SPolicy.scanPercent = 100
  val COPolicy  = new WritePolicy; COPolicy.sendKey = true
    COPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY
  val GPolicy   = new Policy


}

