package rajkumar.org.algos

import rajkumar.org.utils.Json

import scala.util.Try
import scala.math.Ordering
import scala.collection.mutable.{Stack,SortedSet,TreeSet}
import scala.concurrent.Await
import scala.concurrent.duration._

// General purpose Graph class

// Note: Needs unit-tests and examples

// Case Classes for Vertex and Edge
//   Note: vertices are equal if the ids are, edges if ids and weights are
//         Edge( "a", "b", 10 ) is equal to Edge( "b","a", 10 )
case class Vertex( id:String ) {
  var adj: Set[Edge] = Set()
  def neighbors: Set[Vertex] = adj.map( Graph.otherVertexOption(_, this ).get )
  override def equals(o:Any) = o match {
    case n:Vertex => n.id == id
    case _      => false
  }
  override def hashCode() = id.hashCode
}

case class Edge( n1:Vertex, n2:Vertex, wt:Int ) {
  override def equals(o:Any) = o match {
    case e:Edge => e.wt == wt && ((e.n1.id == n1.id && e.n2.id == n2.id) ||
      (e.n2.id == n1.id && e.n1.id == n2.id))
    case _      => false
  }
  override def hashCode() = n1.id.hashCode + n2.id.hashCode + wt.hashCode * 41
  override def toString() = s"Edge(${n1.id},${n2.id},$wt)"
  def toTuple = (n1.id, n2.id, wt )
}


// - The Graph Class (stores vertices and edges )  --------------------
class Graph {

  // vertex-id -> Vertex(id) for fast lookup
  var vertices: Map[String,Vertex] = Map()
  var edges: Set[Edge] = Set()

  // - Api --
  def addVertex( id:String ):Vertex = {
    val vertex = vertices.getOrElse( id, Vertex( id ))
    vertices = vertices + (id -> vertex)
    vertex
  }

  def addVertex( nid:String, dists:Map[String,Int] ):Unit =
    dists.foreach{ case(tonid,wt) => addEdge( nid, tonid, wt ) }

  def addEdge( n1:String, n2:String, wt:Int ) = {
    addVertex( n1 ); addVertex( n2 )
    val e = new Edge( vertices( n1), vertices(n2), wt )
    edges = edges + e
    vertices(n1).adj = vertices(n1).adj + e
    vertices(n2).adj = vertices(n2).adj + e
  }

  def rmVertex( vid:String ):Boolean = {
    if( vertices.contains( vid )){
      val v = vertices( vid )
      for( e <- v.adj ){
        val o = Graph.otherVertexOption(e,v).get
        o.adj = o.adj - e
      }
      edges = edges -- v.adj
      v.adj = Set()
      vertices = vertices - vid
      true
    } else false
  }

  def rmEdge( ec:Edge ):Boolean = {
    if( edges.contains( ec )){
      // ec may have different vertex adjs but eql.
      val e = edges.find( _ == ec ).get

      e.n1.adj = e.n1.adj - e
      if( e.n1.adj.isEmpty ) vertices = vertices - e.n1.id

      e.n2.adj = e.n2.adj - e
      if( e.n2.adj.isEmpty ) vertices = vertices - e.n2.id

      edges = edges - e
      true
    } else false
  }

  // vertexids
  def vids():Seq[String] = vertices.keys.toSeq

  // Empty the graph
  def clear() = {
    vertices = Map()
    edges = Set()
  }

  // -- Overrides -------
  override def toString = {
    val ns = vids().sorted.mkString(", ")
    val es = edges.map( _.toString ).mkString("\n")
    s"Vertices: $ns \n\nEdges:\n$es\n"
  }
}

// - Helper methods and algorithms -------------------
object Graph {

  // Graph <-> Map[ source-vertex, Map[ target-vertex, distance]] representation
  // Important Warning: Ignores edges with vertices not in m.keys
  def createGraph( m:Map[String, Map[String,Int]] ):Graph = {
    val g = new Graph
    for( (fv, am) <- m )
      for( (tv,d) <- am )
        if( m.contains(tv)) g.addEdge( fv, tv, d )
    g
  }
  def createGraph( m:collection.mutable.Map[String, Map[String,Int]] ):Graph =
    createGraph( m.toMap )

  // get adjacency-map representation (without double counting edges)
  def adjMap( g:Graph ):Map[String,Map[String,Int]] = {
    val m = scala.collection.mutable.Map[String,Map[String,Int]]()
    for( e <- g.edges )
      m( e.n1.id ) = m.getOrElse( e.n1.id, Map() ) + ( e.n2.id ->  e.wt )
      m.toMap
  }

  // - Helpers --
  def otherVertexOption( e:Edge, n:Vertex):Option[Vertex] =
    if( e.n1 == n ) Some( e.n2 )
    else if (e.n2 == n ) Some( e.n1 )
    else None

  def otherVertexDist( e:Edge, n:Vertex):(Vertex,Int) =
    if( e.n1 == n ) ( e.n2, e.wt )
    else if (e.n2 == n ) ( e.n1, e.wt )
    else (EmptyVertex,0)

  def otherVertexIdDist( e:Edge, n:Vertex):(String,Int) =
    if( e.n1 == n ) ( e.n2.id, e.wt )
    else if (e.n2 == n ) ( e.n1.id, e.wt )
    else ("",0)


  // - IO - Save/load adjacency representation of graph -----------------------
  // File
  def toFile( g:Graph, f:String ):Unit = {
    val out = new java.io.FileWriter( f )
    val ss = adjMap(g).map{ case(uid,m) => Seq( uid,
      Json.toJS[ Map[String,Int]](m))}
    ss.map( s => Json.toJS[ Seq[String] ](s) +"\n").foreach( s => out.write( s ))
    out.close
  }
  def fromFile( f:String ):Graph = {
    val g = new Graph
    val ss = scala.io.Source.fromFile(f).getLines.map( l =>
      Json.fromJS[Seq[String]](l).get)
    val tups = ss.map( s => ( s.head, Json.fromJS[Map[String,Int]]( s.last )) )
    tups.foreach{ case(uid,am) => g.addVertex( uid, am.get )}
    g
  }

  // - Stats -------------------------------------------------------------------
  // Get some stats about graph
  def stats( g:Graph):String = {
    val meanWt  = g.edges.map( _.wt ).sum / g.edges.size.max(1)
    val maxWt   = g.edges.map( _.wt ).max
    val minWt   = g.edges.map( _.wt ).min
    val adjs    = g.vertices.map{ case(_,v) => v.adj.size }
    val minFo   = adjs.min
    val maxFo   = adjs.max
    val meanFo  = adjs.sum / g.vertices.size.max(1)
 
    "\n" +
    s"Vertices: ${g.vertices.size},\tFanout(Mean): $meanFo\n" +
    s"Edges:    ${g.edges.size},\tWts(Mean,Min,Max): $meanWt, $minWt, $maxWt\n" +
    s"Fanout:   (Mean,Min,Max):$meanFo, $minFo, $maxFo "
    // s"Connected Components: ${Algos.connectedComponents(g).size}\n"
  }

  // Useful Constants
  val EmptyVertex   = Vertex("")
  val EmptyEdge     = Edge( EmptyVertex, EmptyVertex, 0)
  val MinEdge       = Edge( EmptyVertex, EmptyVertex, Int.MinValue)
  val MaxEdge       = Edge( EmptyVertex, EmptyVertex, Int.MaxValue)

  // - Testing --------------------------------------------------------------
  val dg = Map( "a" -> Map( "d" -> 3 ),
                "b" -> Map( "c" -> 2, "e" -> 4 ),
                "c" -> Map( "e" -> 6 ),
                "d" -> Map( "f" -> 4, "g" -> 3 ),
                "e" -> Map( "h" -> 5 ),
                "f" -> Map( "g" -> 4 ))

  // dg + ( a:b:5, g:h:1, d:e:7 )
  val cg = Map( "a" -> Map( "d" -> 3, "b" -> 5 ),
                "b" -> Map( "c" -> 2, "e" -> 4 ),
                "c" -> Map( "e" -> 6 ),
                "d" -> Map( "f" -> 4, "g" -> 3, "e" -> 7 ),
                "e" -> Map( "h" -> 5 ),
                "f" -> Map( "g" -> 4 ),
                "g" -> Map( "h" -> 1))

}



