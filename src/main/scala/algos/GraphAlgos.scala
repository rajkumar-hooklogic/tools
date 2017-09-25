package  rajkumar.org.algos

import rajkumar.org.utils.Json

import scala.math.Ordering
import scala.collection.mutable.{Stack,SortedSet,TreeSet}
import scala.concurrent.Await
import scala.concurrent.duration._

// - Graph Algorithms (using companion class Graph.scala)
//
// -  Minimum Spanning Forest (MSF)
// -  Connected Components
// -  Depth First Search (DFS)
// -  Limited DFS (to limited set of target vertices)

// Note: Needs unit-tests and examples

object GraphAlgos {

  // PriorityMap  version of Prim's algorithm for Min/Max Spanning Forest
  // O( V x Log(E) )

  def primsMSF( g:Graph ):Graph = {
    println(s"Calc. MSF for graph(V,E): (${g.vertices.size},${g.edges.size})")

    val msf = new Graph
    var cs  = new PriorityMap[String,String,Int]()

    // Init cs with all nodes in g
    val entries = g.vertices.map{ case(id,_) =>
      cs.enqueue( id, "", Int.MinValue )}

    val Frequency = 100000       // ( only for update notifications )

    // Add vertex in cs with best edge to forest; if valid add edge to forest
    // If new edge has better edge to vertices in cs, update
    while( cs.nonEmpty ){

      val (bestvid, bestovid, bestw) = cs.dequeue( false )
      val bestv = g.vertices( bestvid )
      msf.addVertex( bestvid )
      if( bestw != Int.MinValue ) msf.addEdge( bestvid, bestovid, bestw )

      for( e <- bestv.adj){
        val ov = Graph.otherVertexOption( e, bestv ).get
        if( cs.contains( ov.id )){
          val (_,w) = cs.get( ov.id ).get
          if( e.wt > w )   // log n
            cs.modify( ov.id, bestvid, e.wt )
        }
      }

     if( cs.size % Frequency == 0 ) print(s"${cs.size}\t")
    }
    msf
  }

  // Depth First Search (DFS)
  // Returns map of distances from source node to all reachables
  def dfs( t:Graph, from:String):Map[String,Int] = {
    var discovered = collection.mutable.Map[String,Int]()
    var s = Stack[(String,Int)]( (from,0) )
    while( s.nonEmpty ){
      val (v,d) = s.pop()
      if( ! discovered.contains( v )) discovered(v) = d
      for((n,w) <- t.vertices(v).adj.map( e =>
        Graph.otherVertexDist( e, t.vertices(v) )))
        if( ! discovered.contains( n.id )) s.push( (n.id, w+d) )
    }
    discovered.toMap
  }

  // DFS to target vertices (only)
  def dfs( t:Graph, from:String, to:Seq[String]):Map[String,Int] = {
    var discovered  = collection.mutable.Map[String,Int]()
    var out         = collection.mutable.Map[String,Int]()
    var todo        = to
    var s = Stack[(String,Int)]( (from,0) )
    while( s.nonEmpty && todo.nonEmpty){
      val (v,d) = s.pop()
      if( ! discovered.contains( v )){
        discovered(v) = d
        if( to.contains( v )) out(v) = d
        todo = todo diff Seq( v )
      }
      for((n,w) <- t.vertices(v).adj.map( e =>
        Graph.otherVertexDist( e, t.vertices(v) )))
        if( ! discovered.contains( n.id )) s.push( (n.id, w+d) )
    }
    out.toMap
  }

  // List of connected-components
  def connectedComponents( t:Graph ):Seq[ Seq[String]] = {
    var ccs = Seq[Seq[String]]()
    var unseen = t.vertices.keys.toSeq
    while (unseen.size > 0 ) {
      val seen = dfs( t, unseen.head ).keys.toSeq
      unseen = unseen diff seen
      ccs = ccs :+ seen
    }
    ccs
  }


  // - Testing --------
  def test() = {
    val g = new Graph
    g.addEdge( "b", "c", 2 )
    g.addEdge( "c", "e", 6 )
    g.addEdge( "e", "h", 5 )
    g.addEdge( "g", "f", 4 )
    g.addEdge( "f", "d", 4 )
    g.addEdge( "d", "a", 3 )
    g.addEdge( "b", "e", 4 )
    g.addEdge( "d", "g", 3 )
    print( g )

    val t = primsMSF( g ); println( t )

    g.addEdge( "a", "b", 5 )
    g.addEdge( "h", "g", 1 )
    g.addEdge( "d", "e", 7 )
    val r = primsMSF( g ); println( r )

    println( dfs( r, "a" ))
  }
  def main( args: Array[String]):Unit =  {}
}



