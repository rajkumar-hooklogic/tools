package rajkumar.org.algos

// BloomFilter Inplementation

import collection.mutable.BitSet
import scala.util._
import java.io._

class BloomFilter[A] private (val s:Int, val w:Int, val k:Int, var bs:BitSet ){
  val r = scala.util.Random

  def this( es: Seq[A] ) = {
    this( es.size, BloomFilter.bestW( es.size),
      BloomFilter.optimalK( es.size, BloomFilter.bestW( es.size )), BitSet())
      add( es )
      println("W: " + w )
  }

  def this( es: Iterator[A], size: Int ) = {
    this( size, BloomFilter.bestW( size),
      BloomFilter.optimalK( size, BloomFilter.bestW( size )), BitSet())
      print("adding...")
      add( es )
      println("W: " + w )
  }


  //-API------
  def +(e: A) = add( e )
  def ++(es: Seq[A]) = for( e <- es ) add( e )
  def apply(e: A):Boolean = contains( e )

  def contains( e: A ):Boolean = {
    r.setSeed( e.hashCode )
    val is = Stream.continually( bs( r.nextInt( w )) ).take( k )
    is.takeWhile( _ == true).size == k
  }

  def filter( es: Iterator[A]): Iterator[A] = es.filter( contains( _ ))

  def add( e:Any ):Unit = {
    r.setSeed( e.hashCode )
    for( v <- Stream.continually( r.nextInt( w )).take( k )) bs.add( v )
  }
  def add( es: Seq[Any]):Unit = for( e <- es ) add( e )
  def add( es: Iterator[Any]):Unit = for( e <- es ) add( e )
 

  // -Utils---------
  lazy val accuracy = {
    val exp = ((k:Double) * s ) / w
    val p = Math.pow( 1 - Math.exp( -exp), k )
    1d - p
  }

  // -IO ---------
  def store( f: String){
    val dos = new DataOutputStream(new BufferedOutputStream(
      new FileOutputStream(f)))
    dos.writeInt(s)
    dos.writeInt(w)
    dos.writeInt(k)
    // println( s +","+ w +","+ k +","+ bs.size )
    for( l <- bs.toBitMask ) dos.writeLong( l )
      dos.close
  }
}
// ---------
object BloomFilter {

  def bestW( s:Int ):Int = 8 * s

  def optimalK(s:Int,w:Int):Int =
    Math.max( (Math.round( 9.0  / 13.0 * w / s )).toInt, 1)

  def hash( e:Any, its: Int, bounds: Int):Int = {
    Math.abs(
      if (its == 0) e.hashCode
      else its ^ hash( e, its - 1, bounds )
    ) % bounds
  }

  def load[A](f: String):BloomFilter[A] = {
    val dis = new DataInputStream(new BufferedInputStream(
      new FileInputStream(f)))
    val s = dis.readInt
    val w = dis.readInt
    val k = dis.readInt
    val st = Stream.continually( Try( dis.readLong )).takeWhile( _.isSuccess )
    val la = st.map( _ match {
        case Success(r) => r
        case _ => 0L
      }).toArray
    dis.close
    // println("Loaded "+ f +" "+ s +" entries with k="+ k )
    val bs = BitSet.fromBitMask( la )
    new BloomFilter[A](s, w, k, bs)
  }

  // -- Testing -----------------------------------------

  def cleank( s: String ): String = {
    val pi = s.indexOf("(")
    if( pi > 0 ) s.substring(0,pi).trim
    else s
  }

  def rtest() = {
    val of = "/tmp/test.bf"
    val cf = load[String]( of )
    val somekeys = io.Source.fromFile("/data/enwiki.titles").getLines.take( 10000 )
    val badkeys = Seq("foo","aar","baz","roo","moo","blue","rue","hoe")
    val testkeys = somekeys ++ badkeys
    val a = System.currentTimeMillis
    val r = for( s <- testkeys ) yield cf(s)
    val i = System.currentTimeMillis - a
    for( s <- testkeys.take(10) ) println( s +" "+ cf( s ) +" " )
    println("\n" + cf.accuracy + " for 10000 lookups took " + i + " ms")
  }

  def perftest( n: Int) = {
    val ks = io.Source.fromFile( "/tmp/gs.keys" ).getLines.take( n )
    val bf = BloomFilter.load[String]( "/tmp/gs.bf" )
    val st  = System.currentTimeMillis
    val good = bf.filter( ks ).size
    val t   = System.currentTimeMillis - st
    println( n +" checks took "+ t +" ms (" + (n/t.max(1)) + " cs/ms)" )

  }
  def ctest() = {
    val bf = BloomFilter.load[String]( "/tmp/gs.bf" )
    while( true ){
      print("> ")
      println( bf( readLine ))
    }
  }

  def main( args: Array[String]) = {
    perftest( 1000000 )
    ctest()
  }
}
