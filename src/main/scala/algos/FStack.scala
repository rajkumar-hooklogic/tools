package rajkumar.org.algos

import scala.reflect.ClassTag
import collection.JavaConversions._


// Fixed size stack implementation

class FStack[T:ClassTag]( ss:Int) {

  // Note: there is no way to differentiate between empty and full
  //       therefore size returns 0 when full
  private val buf   = new Array[T]( ss )
  private var start = 0
  private var cur   = 0

  // - api -
  def push( s:T ) = {
    buf( cur ) = s
    cur = (cur + 1) % ss
    if( start == cur ) start = start + 1
  }
  def pop():Option[T]    =  {
    if( cur == start ) None      // Empty
    else {
      cur = (cur - 1 + ss) % ss
      Option( buf( cur ))
    }
  }
  def size():Int    = ( cur + ss - start ) % ss
  // Is this right
  def elements():Array[T] = if( cur >= start ) buf.slice( start, cur ).reverse
    else (buf.slice( start, buf.size ) ++ buf.slice( 0, cur )).reverse
}

// - Testing ---
object FStack {

  def stest() = {
    val fs = new FStack[Int](10)
    for( i <- 1 to 7) fs.push( i )
    println( fs.pop() )
    fs.elements().foreach( println )
  }
  def  main( args:Array[String]):Unit = stest()
 
}
