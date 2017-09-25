package rajkumar.org.io

import redis.clients.jedis._

import rajkumar.org.utils.Json

import scala.util.{Try,Success,Failure}
import scala.concurrent.{Promise,Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._


// redis implementation of KSStore
class Redis( ns:String ) extends KSStore with KOStore {

  // Expiration in seconds
  var exps = 100000
  def setExpS( s:Int ) = { exps = s }

  // - General --
  def isReady():Boolean = true // Note: fix

  // - Sync ---
  def get( k:String ):String = {
    val jedis = Redis.pool.getResource
    val res = jedis.get( s"$ns:$k" )
    Redis.pool.returnResource( jedis )
    res
  }

  def put( k:String, v:String):Boolean = {
    val jedis = Redis.pool.getResource
    val key = s"$ns:$k"
    jedis.set( key, v )
    val res = jedis.expire( key, exps )
    Redis.pool.returnResource( jedis )
    res > 0
  }

  def prepend( k:String, v:String ):Boolean = {
    val jedis = Redis.pool.getResource
    val key = s"$ns:$k"
    jedis.append( key, v )
    val res = jedis.expire( key, exps )
    Redis.pool.returnResource( jedis )
    res > 0
  }

  def delete( k:String ):Boolean = {
    val jedis = Redis.pool.getResource
    val res = jedis.del( s"$ns:$k" )
    Redis.pool.returnResource( jedis )
    res > 0
  }

  def exists( k:String ):Boolean = {
    val jedis = Redis.pool.getResource
    val res = jedis.exists( s"$ns:$k" )
    Redis.pool.returnResource( jedis )
    res
  }

  // All keys in this bucket
  def keys():Iterator[String] = {
    val jedis = Redis.pool.getResource
    val res = jedis.keys( "*" ).toIterator
    Redis.pool.returnResource( jedis )
    res
  }

  // - Async -----
  // Wrap sync-callis in Futures

  def aget( k:String ):Future[String] = Future{ get(k) }

  def aput( k:String, v:String):Future[Boolean] = Future{ put(k,v) }

  def aprepend( k:String, v:String ):Future[Boolean] = Future{ prepend(k,v) }

  def adelete( k:String ):Future[Boolean] = Future { delete(k) }

  def aexists( k:String ):Future[Boolean] = Future { exists( k ) }

  def akeys():Future[Iterator[String]] = Future { keys() }



  // - KOStore implementation -----------------------
  def get[A:Manifest](  k:String ):Option[A] = {
    val jedis = Redis.pool.getResource
    val rs = jedis.get( s"$ns:$k" )
    Redis.pool.returnResource( jedis )
    Json.fromJS[A]( rs )
  }
  def put[  A <: AnyRef:Manifest](  k:String, v:A ):Boolean = {
    val key = s"$ns:$k"
    val js = Json.toJS[A]( v )
    val jedis = Redis.pool.getResource
    jedis.set( key, js )
    val res = jedis.expire( key, exps )
    Redis.pool.returnResource( jedis )
    res > 0
  }

}




// - Static Companion Object (to hold Connection) ---------------
object Redis extends Store {

  val Host = "127.0.0.1"
  var pool = new JedisPool( new JedisPoolConfig(), Host )

  def connect( h:String ):Unit  = {
    if( pool != null ) pool.close
    pool = new JedisPool( h )
  }
  def disconnect():Unit         = { pool.close }
  def isConnected():Boolean     = pool != null && ! pool.isClosed

  def getKSStore( ns:String = "default"):KSStore = new Redis( ns )
  def getKOStore( ns:String = "default"):KOStore = new Redis( ns )

}


