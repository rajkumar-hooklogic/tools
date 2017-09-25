package rajkumar.org.io

import rajkumar.org.aws._
import rajkumar.org.utils.Strings
import scala.util._

object IOTests {

  private val N     = 100000

  type ITup   = (Int,Int)
  type STup   = (String,String)
  type IOF    = (STup)    => Boolean

  // api
  def iotest( db:KSStore, n:Int = N ):Unit = {

   val wf = (kv:STup)  => db.put( kv._1, kv._2 )
   val rf = (kv:STup ) => db.get( kv._1 ) == kv._2
   val df = (kv:STup ) => db.delete( kv._1 )
   val fmap = Map( "Write" -> wf, "Read"  -> rf, "Delete"-> df )

    val rmap  = io( fmap, n )
    val rss   = rmap.map{ case(n,r) => s" ${n}s::\t ${r._1}%, ${r._2} rs/ms"}
    println( rss.mkString("\n") )
    println( s"\nDone running ${fmap.size} tests, $n count\n" )
  }

  private val f = "/dataOut.txt"
  private val ls = scala.io.Source.fromURL( getClass.getResource(f)).
    getLines.toSeq

  private def key():String    = Strings.guid
  private def value():String  = ls( scala.util.Random.nextInt( ls.size ) )
  private def kv():STup       = ( key(), value() )
  private def nks(  n:Int ):Seq[String] = (1 to n).map( x => key() )
  private def nkvs( n:Int ):Seq[STup]   = (1 to n).map( x => kv() )

  // Calculate success-rate(%) and iorate(recs / millis)
  private def time( is:Seq[STup], f:IOF ):ITup = {
    val st = System.currentTimeMillis
    val gs = is.map( i => f(i) ).filter( x => x ).size
    val et = System.currentTimeMillis - st

    val spct    = gs * 100 / is.size.max(1)
    val iorate  = gs / et.max(1)
    (spct, iorate.toInt)
  }

  private def io( fs: Map[String,IOF], n:Int ):Map[String,ITup] = {
    val kvs   = nkvs( n )
    fs.map{ case(n,f) => (n, time( kvs, f ) ) }
  }

  // - Example of use -----
  def test( n:Int) = {

    val Host = "127.0.0.1"

    println("\n\nDynamo")
    var store:Store = Dynamo
    store.connect( Host )
    val ddb = store.getKSStore("Test")
    iotest( ddb, n )
    ddb.close
    store.disconnect()

    println("\n\nAerospike")
    store = aspike.Spike
    store.connect( Host )
    val adb = store.getKSStore("test")
    iotest( adb, n )
    adb.close
    store.disconnect()

    println("\n\nRedis")
    store = Redis
    store.connect( Host )
    val rdb = Redis.getKSStore("test")
    iotest( rdb, n )
    rdb.close()
    store.disconnect()

    println("\n\nCouchBase")
    store = cBase
    store.connect( Host )
    val cdb = cBase.getKSStore("default")
    iotest( cdb, n )
    cdb.close()
    store.disconnect()
  }

  def main( args:Array[String]):Unit = {
    val n = if( args.size > 0 ) args(0).toInt else N
    test( n )
  }
}

