package rajkumar.org.io.aspike

import rajkumar.org.io.{Store,KVStore,KSStore,KOStore}

import com.aerospike.client._
import com.aerospike.client.policy._
import com.aerospike.client.query._

import scala.util.Try
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global


// Utility for storing and retrieving java/scala "objects" to ASpike.
// Use Strings, bytes and ints instead where possible as storing/reading
// objects is slow/inefficient

class Ospike( s:String ) extends KOStore {
  val n = Spike.NS
  val b = "object"

  def isReady():Boolean = Spike.isConnected()

  def delete( k:String):Boolean = Ospike.delete( k, n,s )
  def exists( k:String):Boolean = Ospike.exists( k, n,s )
  def keys():Iterator[String]   = Ospike.keyscan(   n,s,b )

  def adelete( k:String):Future[Boolean]  = Ospike.adelete( k, n,s )
  def aexists( k:String):Future[Boolean]  = Ospike.aexists( k, n,s )
  def akeys():Future[Iterator[String]]    = Future{ keys() }

  def get[A:Manifest]( k:String ):Option[A] = Ospike.geto[A]( k, n,s,b )
  override def get[A:Manifest]( ks:Seq[String] ):Map[String,A]
    = Ospike.getos[A]( ks, n,s,b )
  def put[A <: AnyRef:Manifest]( k:String, v:A ):Boolean
    = Try( Ospike.puto[A]( k,v, n,s,b) ).isSuccess
  override def scan[A <: AnyRef:Manifest]( f:(String,A) => Option[A]):Unit
    =  Ospike.scan[A]( f, n,s,b )
}

object Ospike  {

  // --- Synchronous helpers ---------------------------------------

  def exists( k:String, n:String, s:String):Boolean =
    Spike.asc.exists( null, new Key(n,s,k) )
 
  // return keys which exist
  def exists( ks:Seq[String], n:String, s:String ):Seq[String] = {
    val keys = ks.map( k => new Key( n, s, k )).toArray
    ks.zip( Spike.asc.exists( Spike.BPolicy, keys) ).filter( _._2 ).map( _._1 )
  }

  // Get object
  def geto[A]( k:String, n:String, s:String, b:String):Option[A] = {
    val r = Spike.asc.get( null, new Key(n,s,k), b )
    if( r == null ) None
    else r.getValue(b) match {
      case o:A  => Some( o )
      case _    => None
    }
  }

  // get objects
  def getos[A]( ks:Seq[String], n:String, s:String, b:String):Map[String,A] = {
    val rs = Spike.asc.get( Spike.BPolicy, ks.map( k => new Key(n,s,k)).toArray,
      Array(b):_* )
    val krm = ks.zip( rs ).filter( _._2 != null ).toMap
    val kom = krm.map{ case(k,r) => (k, r.getValue( b ))}
    val kam = kom.map{ case(k,o) => (k, aOpt[A](o))}.filter( _._2.isDefined )
    kam.map{ case(k,ao) => ( k,ao.get )}
  }

  // put opbject
  def puto[A]( k:String, v:A, n:String, s:String, b:String ):Unit =
    Spike.asc.put( Spike.WPolicy, new Key(n,s,k), new Bin(b,v) )

  // put objects
  def putos[A](kvs:Map[String,A], n:String, s:String, b:String ):Unit=
    kvs.foreach{ case(k,v) => Spike.asc.put( Spike.WPolicy, new Key(n,s,k),
      new Bin(b,v) )}

  // delete key
  def delete( k:String, n:String, s:String ):Boolean =
    Spike.asc.delete( Spike.WPolicy, new Key( n,s,k ))

  // delete set (need to specify bin name)
  def deleteset( n:String, s:String, b:String ):Unit =
    rscan( (k:Key,r:Record) => {val a = Spike.asc.delete( Spike.WPolicy, k )},
      n, s, b )

  // - scan all keys ---
  def rscan( f:( Key, Record ) => Unit, n:String, s:String, b:String ) = {
    object IO extends ScanCallback {
      def scanCallback( k:Key, r:Record ):Unit = f( k, r )
    }
    println( s"$n $s $b" )
    Spike.asc.scanAll( Spike.SPolicy, n, s, IO, b )
  }

  def keyscan( n:String, s:String, b:String ):Iterator[String] = {
    var keys = Seq[String]()
    object IO extends ScanCallback {
      def scanCallback( k:Key, r:Record ):Unit = keys = keys :+ Spike.ktos(k)
    }
    Spike.asc.scanAll( Spike.SPolicy, n, s, IO, b )
    keys.toIterator
  }

  // Note: Careful. If Record cannot be converted to type A, record is deleted
  def scan[A]( f:( String, A ) => Option[A], n:String, s:String, b:String ) = {
    object IO extends ScanCallback {
      def scanCallback( k:Key, r:Record ):Unit = {
        val oa = Spike.rtoa[A]( r, b )
        if( oa.isDefined )
          f( Spike.ktos(k), oa.get ) match {
            case None                       => Spike.asc.delete( null, k )
            case Some(o) if( o != oa.get )  => Spike.asc.put( Spike.WPolicy, k,
              new Bin(b,o) )
            case _                          =>
          }
        else Spike.asc.delete( null, k )
      }
    }
    Spike.asc.scanAll( Spike.SPolicy, n, s, IO, b )
  }

  // -- Asynchronous helpers ---------------------------------------

  def aexists( k:String, n:String, s:String):Future[Boolean] = {
    val p = Promise[Boolean]()
    Spike.aasc.exists( null, new Listeners.EListener( p ), new Key(n,s,k) )
    p.future
  }

  def adelete( k:String, n:String, s:String):Future[Boolean] = {
    val p = Promise[Boolean]()
    Spike.aasc.delete( null, new Listeners.DListener( p ), new Key(n,s,k) )
    p.future
  }

  // return keys which exist
  def aexists( ks:Seq[String], n:String, s:String ):Future[Seq[String]] = {
    val p = Promise[ Seq[String]]()
    val keys = ks.map( k => new Key( n, s, k )).toArray
    Spike.aasc.exists( Spike.BPolicy, new Listeners.EAListener( p ), keys)
    p.future
  }

  // put object (async) (put and go; should we return future? )
  def aputo[A]( k:String, v:A, n:String, s:String, b:String ):Unit =
    Spike.aasc.put( Spike.WPolicy, null, new Key(n,s,k), new Bin(b,v) )


  // - read-only scan all keys (async)
  def arscan[A]( f:(Key,A) => Unit, n:String, s:String,b:String ):Future[Unit] = {
    val p = Promise[Unit]()
    Spike.aasc.scanAll( Spike.SPolicy, new Listeners.GenericListener[A]( p, f, b ),
      n, s, b )
    p.future
  }

  // - Miscellany -----------------------------------------------------------
  // - Query Helpers -
  def filter( n:String, s:String, b:String, f:Filter ):Iterator[(Key, Record)] = {
    val t = new Statement()
    t.setNamespace( n )
    t.setSetName( s )
    t.setBinNames( b )
    t.setFilters( f )
    val rs = Spike.asc.query( null, t )
    Iterator.continually( if( rs.next) get( rs ) else (null,null) ).
      takeWhile( _._1 != null )
  }

  // - Private ----
  // key, record from RecordSet
  private def get( rs:RecordSet):(Key,Record) = (rs.getKey, rs.getRecord)

  private def aOpt[A]( o:Any ):Option[A] = o match {
    case a:A  => Some(a)
    case _    => None
  }

  def main( args:Array[String]):Unit = {
    if( args.size > 2 && args(0) == "deleteset" ) {
      Spike.connect()
      deleteset( "ssd", args(1), args(2) )
    }
  }
}

