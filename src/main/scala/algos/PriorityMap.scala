package rajkumar.org.algos

// Ordered Map (similar to PriorityQueue, but only the latest (K,V) is kept)
class PriorityMap[K,V,W:Ordering] {

    val m   = collection.mutable.Map[K,(V,W)]()
    var om  = collection.immutable.TreeMap[W,Set[K]]()

    def enqueue( k:K, v:V, w:W):Unit = {
      remove( k )
      val ks = om.getOrElse( w, Set())
      val nks = ks + k
      om = om - w
      om += (w -> nks)
      m( k ) = (v,w)
    }
    def dequeue( smallest:Boolean = true):(K,V,W) = {
      val (w,ks) = if( smallest) om.head else om.last
      om = om - w
      val k = ks.head
      val nks = ks - k
      if ( ! nks.isEmpty ) om += ( w -> nks )
      val (v,_) = m.remove( k ).get
      (k,v,w)
    }
    def contains(k:K):Boolean = m.contains( k )
    def get(k:K):Option[(V,W)] = m.get( k )
    def remove(k:K):Boolean = {
      if( m.contains( k )){
        val (v,w) = m.remove( k ).get
        val ks = om( w ); om -= w
        val nks = ks - k
        if( ! nks.isEmpty ) om += ( w -> nks)
        true
      }
      else false
    }
    def modify(k:K, v:V, w:W) = { remove(k); enqueue( k,v,w) }

    def isEmpty:Boolean   =   m.isEmpty
    def nonEmpty:Boolean  =   m.nonEmpty
    def size:Int          =   m.size
}

// - Testing ------------------
object PriorityMap {
  def main( args: Array[String]):Unit = {

    val a = new PriorityMap[ String, String, Int]()
    val s = Seq(
        ("a", "1", 5 ), ("a", "2", 5 ),
        ("b", "2", 5 ),
        ("c", "3", 0 ), ("c", "3", 3 ),
        ("d", "4", 3 ),
        ("e", "2", 1 ))

    s.foreach{ case(k,v,w) => a.enqueue( k, v, w ) }
    while( a.nonEmpty ) println( a.dequeue() )
  }
}
