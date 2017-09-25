package rajkumar.org.io.aspike

import com.aerospike.client.{Key, Bin, Record, AerospikeException, ScanCallback}
import com.aerospike.client.listener._

import scala.concurrent.{Future, Promise}
import scala.util.{Try, Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

import rajkumar.org.io.KSStore

// Aerospike String Key-Values Store
class SSpike(s:String) extends KSStore {

  val ns = Spike.NS

  // KVStore implemnetation
  def isReady():Boolean = Spike.isConnected()

  def delete(   k:String):Boolean = SSpike.delete( k, ns, s )
  def exists(   k:String):Boolean = SSpike.exists( k, ns, s )
  def keys():Iterator[String]     = SSpike.keys( ns, s)

  def adelete(  k:String):Future[Boolean] = SSpike.adelete( k, ns, s )
  def aexists(  k:String):Future[Boolean] = SSpike.aexists( k, ns, s )
  def akeys():Future[Iterator[String]]    = Future{ SSpike.keys( ns, s) }

  // KSStore implementation
  // - Synchronous
  def get( k:String):String = SSpike.get( k, ns, s )
  override def get( ks:Seq[String]):Map[String,String] = SSpike.get( ks, ns, s )
  def put( k:String, v:String):Boolean = Try( SSpike.put( k, v, ns, s )).isSuccess
  def prepend( k:String, v:String):Boolean =
  Try( SSpike.prepend( k, v, ns, s)).isSuccess
  override def scan(   f:(String,String) => String ):Unit = SSpike.scan( f, ns, s )

  // - Asynchronous
  def aget(     k:String):Future[String] = SSpike.aget( k, ns, s )
  def aput(     k:String, v:String):Future[Boolean] = SSpike.aput( k, v, ns, s )
  def aprepend( k:String, v:String):Future[Boolean] = SSpike.aprepend( k,v,ns,s)
  override def ascan( f:(String,String) => String ):Future[Unit] =
    SSpike.ascan( f, ns, s )
}

// -----------
// Aerospike Helper Object for single-bin String IO
object SSpike  {

  val SBin = "string"
  val wpolicy = Spike.WPolicy

  // - Synchronous -------------
  // get keys
  def get(  ks:Seq[String], ns:String, s:String ):Map[String,String] = {
    val keys = ks.map( k => new Key( ns, s, k )).toArray
    val rs = Spike.asc.get( Spike.BPolicy, keys )
    ks.zip( rs ).map{ case(k,r) => (k,Spike.rtos(r))}.filter(_._2.size > 0 ).toMap
  }

  // get key
  def get( k:String, ns:String, s:String ):String =
  Spike.rtos( Spike.asc.get( null, new Key(ns,s,k), SBin ) )

  // prepend string to key
  def prepend( k:String, v: String, ns:String, s:String):Unit =
  Spike.asc.prepend( wpolicy, new Key(ns,s,k), new Bin( SBin,v) )

  // put to key
  def put( k:String, v: String, ns:String, s:String): Unit =
  Spike.asc.put( wpolicy, new Key( ns, s, k ), new Bin( SBin,v) )

  // delete key
  def delete( k:String, ns:String, s:String ):Boolean =
  Spike.asc.delete( null, new Key( ns, s, k))

  def exists( k:String, ns:String, s:String ):Boolean =
  Spike.asc.exists( null, new Key( ns, s, k))

  // scan all keys and apply f() to each (k,v)
  // if f(k,v) returns "" the key is deleted. If f(k,v) is new,  new value is
  //   associated with the key
  def scan( f:( String, String ) => String, ns:String, s:String ) = {
    object IO extends ScanCallback {
      def scanCallback( k:Key, r:Record ):Unit = {
        val rs = Spike.rtos(r)
        val ks = Spike.ktos(k)
        f( ks, rs) match {
          case res:String if( res.size == 0 ) => Spike.aasc.delete( wpolicy, k )
          case res:String if( res != rs )     => Spike.aasc.put( wpolicy, k,
            new Bin( SBin, res ))
          case _                              =>
        }
      }
    }
    Spike.asc.scanAll( Spike.SPolicy, ns, s, IO, SBin )
  }

  // read-only scan; apply f() to each (k,v)
  def rscan( f:( String, String ) => Unit, ns:String, s:String ) =
    Spike.asc.scanAll( Spike.SPolicy, ns, s, new Spike.SCallback( f ) )

  def keys( ns:String, s:String ):Iterator[String] = {
    var ks = Seq[String]()
    val f = (k:String, v:String) => ks = ks :+ k
    Spike.asc.scanAll( Spike.SPolicy, ns, s, new Spike.SCallback( f ) )
    ks.toIterator
  }



  // - Async ---
  // get keys
  def aget( k:String, ns:String, s:String ):Future[String] = {
    val p = Promise[String]()
    Spike.aasc.get( null, new Listeners.RListener( p ), new Key(ns,s,k), SBin )
    p.future
  }
  def aget( ks:Seq[String], ns:String, s:String ):Future[Map[String,String]] = {
    val keys = ks.map( k => new Key( ns, s, k )).toArray
    val p = Promise[Map[String,String]]()
    Spike.aasc.get( Spike.BPolicy, new Listeners.RAListener( p ), keys, SBin )
    p.future
  }

  // prepend string to key
  def aprepend( k:String, v:String, ns:String, s:String ):Future[Boolean] = {
    val p = Promise[Boolean]()
    Spike.aasc.prepend( wpolicy, new Listeners.WListener(p), new Key( ns,s,k),
      new Bin( SBin,v) )
    p.future
  }

  // put key-value
  def aput(k: String, v:String, ns:String, s:String ):Future[Boolean] = {
    val p = Promise[Boolean]()
    Spike.aasc.put( wpolicy, new Listeners.WListener(p), new Key(ns,s,k),
      new Bin( SBin,v) )
    p.future
  }

  // delete key
  def adelete( k:String, ns:String, s:String ):Future[Boolean] = {
    val p = Promise[Boolean]()
    Spike.aasc.delete( wpolicy, new Listeners.DListener(p), new Key( ns, s, k ))
    p.future
  }

  // exists
  def aexists( k:String, ns:String, s:String ):Future[Boolean] = {
    val p = Promise[Boolean]()
    Spike.aasc.exists( null, new Listeners.EListener(p), new Key( ns, s, k ))
    p.future
  }

  // scan all keys and apply f to each (k,v)
  // if f(k,v) == "" delete record. If new, put new value
  def ascan( f:(String,String) => String, ns:String, s:String ):Future[Unit] = {
    val p = Promise[Unit]()
    Spike.aasc.scanAll( Spike.SPolicy,
      new Listeners.RSListener( p, f ), ns, s, SBin )
    p.future
  }

  // read-only scan all keys and apply f to each (k,v)
  def arscan( r:(String,String) => Unit, ns:String, s:String ):Future[Unit] = {
    val p = Promise[Unit]()
    Spike.aasc.scanAll( Spike.SPolicy, new Listeners.RRSListener(p,r), ns, s, SBin )
    p.future
  }
}

