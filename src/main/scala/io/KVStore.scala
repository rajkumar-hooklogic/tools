package rajkumar.org.io

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

// Traits for KV-Stores to implement

// Generic Store
trait Store {
    def connect(  k:String):Unit
    def disconnect():Unit
    def isConnected():Boolean

    def getKOStore( n:String ):KOStore
    def getKSStore( n:String ):KSStore
}

// Generic KVStore
trait KVStore {
  def isReady():Boolean
  def close():Unit = {}

  def delete(   k:String):Boolean
  def exists(   k:String):Boolean
  def keys():Iterator[String]

  def adelete(  k:String):Future[Boolean]
  def aexists(  k:String):Future[Boolean]
  def akeys():Future[Iterator[String]]
}


// String Store (can do prepends)
trait KSStore extends KVStore {

  // - Synchronous
  def get(      k:String):String
  def get(      ks:Seq[String]):Map[String,String] =
    ks.map( k => (k, get(k))).toMap
  def put(      k:String, v:String):Boolean
  def prepend(  k:String, v:String):Boolean
  def scan(     f:(String,String) => String ):Unit =
    keys().map( k => process( k, f ))

  // - Asynchronous
  def aget(     k:String):Future[String]
  def aput(     k:String, v:String):Future[Boolean]
  def aprepend( k:String, v:String):Future[Boolean]
  def ascan( f:(String,String) => String ):Future[Unit] = Future {
    keys().map( k => process( k, f ))
  }

  // - Privates -
  private def process( k:String, f:(String,String) => String ):Unit = {
    val v = get( k )
    f( k, v ) match {
        case s if( s.size == 0 )  => delete( k )
        case s if( s != v )       => put( k, s )
        case  _                   =>
      }
  }
}

// Object (POJO) store
trait KOStore extends KVStore {
  def get[A:Manifest](  k:String ):Option[A]
  def get[A:Manifest](  ks:Seq[String] ):Map[String,A] = {
    val all = ks.map( k => (k,get[A](k)))
    all.filter( _._2.isDefined ).map{ case(k,vo) => ( k, vo.get )}.toMap
  }
  def put[  A <: AnyRef:Manifest](  k:String, v:A ):Boolean
  def scan[ A <: AnyRef:Manifest](  f:(String,A) => Option[A] ):Unit =
    keys().map( k => process[A]( k, f ))

  // - Privates -
  private def process[A <: AnyRef:Manifest]( k:String,
    f:(String,A) => Option[A] ):Unit = {
    val vo = get[A]( k )
    if( vo.isDefined )
      f( k, vo.get ) match {
        case None => delete( k )
        case Some(a) if( a != vo.get )  => put[A]( k, a )
      }
  }
}

