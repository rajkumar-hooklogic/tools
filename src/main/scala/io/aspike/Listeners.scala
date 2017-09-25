package rajkumar.org.io.aspike

import com.aerospike.client._
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.policy._
import com.aerospike.client.listener._

import scala.concurrent.{Future, Promise}
import scala.util.{Success,Failure}
import scala.concurrent.ExecutionContext.Implicits.global

object Listeners  {

  // Listener classes
  class RListener( p:Promise[String]) extends RecordListener {
    def onFailure( e:AerospikeException ):Unit = p.failure( e )
    def onSuccess( k:Key, r:Record ):Unit = p.success( Spike.rtos( r ))
  }
  class WListener( p:Promise[Boolean]) extends WriteListener {
    def onFailure( e:AerospikeException ):Unit = p.failure( e )
    def onSuccess( k:Key ):Unit = p.success( true )
  }

  class EListener( p:Promise[Boolean]) extends ExistsListener {
    def onFailure( e:AerospikeException ):Unit = p.failure( e )
    def onSuccess( k:Key, e:Boolean ):Unit = p.success( e )
  }

  // return keys which exist
  class EAListener( p:Promise[Seq[String]]) extends ExistsArrayListener {
    def onFailure( e:AerospikeException ):Unit = p.failure( e )
    def onSuccess( ks:Array[Key], es:Array[Boolean] ):Unit = {
      val gks = ks.zip( es ).filter( _._2 ).map( _._1 ).map( Spike.ktos( _ )).toSeq
      p.success( gks )
    }
  }

  class DListener( p:Promise[Boolean]) extends DeleteListener {
    def onFailure( e:AerospikeException ):Unit = p.failure( e )
    def onSuccess( k:Key, e:Boolean ):Unit = p.success( true )
  }

  /*
  class CListener( p:Promise[Int]) extends RecordListener {
    def onFailure( e:AerospikeException ):Unit = p.failure( e )
    def onSuccess( k:Key, r:Record ):Unit = p.success( Spike.rtoi( r ))
  }
  */

  class RAListener( p:Promise[Map[String,String]])  extends RecordArrayListener {
    def onFailure( e:AerospikeException ):Unit = p.failure( e )
    def onSuccess( ks:Array[Key], rs:Array[Record] ):Unit = {
      val krs = ks.zip( rs )
      val m = krs.map{ case(k,r) => (Spike.ktos(k), Spike.rtos(r))}.toMap
      p.success( m )
    }
  }

  class GenericListener[A]( p:Promise[Unit], f:(Key,A) => Unit, b:String )
    extends RecordSequenceListener {
    def onFailure( e: AerospikeException ):Unit = p.failure( e )
    def onSuccess() = p.success()
    def onRecord( k:Key, r:Record ) =
      Spike.rtoa[A](r,b) match { case Some( x ) => f( k, x )  }
  }

  // --- String specific Scan Listeners -----
  class RSListener( p:Promise[Unit], f:(String,String) => String )
    extends RecordSequenceListener {
    def onFailure( e: AerospikeException ):Unit = p.failure( e )
    def onSuccess() = p.success()
    def onRecord( k:Key, r:Record ) = {
      val rs = Spike.rtos( r )
      val ks = Spike.ktos( k )
      f( ks, rs ) match {
        case s if( s.size == 0 ) => Spike.aasc.delete( Spike.WPolicy, null, k )
        case s if( s != rs )  => Spike.aasc.put( Spike.WPolicy, null, k,
          new Bin(SSpike.SBin,s))
        case _                =>
      }
    }
  }
  class RRSListener( p:Promise[Unit], f:(String,String) => Unit )
    extends RecordSequenceListener {
    def onFailure( e: AerospikeException ):Unit = p.failure( e )
    def onSuccess() = p.success()
    def onRecord( k:Key, r:Record ) =  f( Spike.ktos( k ), Spike.rtos(r) )
  }
}

