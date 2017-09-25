package rajkumar.org.utils

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import scala.util.Try
import scala.reflect.Manifest

// Json serialization helpers
object Json {
  implicit val formats = Serialization.formats(NoTypeHints)

  def toJS[ T <: AnyRef:Manifest]( t:T ):String = write( t )

  def fromJS[T:Manifest](js:String):Option[T] =
    if( js.size > 0 ) Try( read[T]( js )).toOption else None

  // For converting sequence of objects to/from delimited strings
  // Needed for serializing streams of objects
  val NDEL  = " | "
  val QNDEL = java.util.regex.Pattern.quote(NDEL)   // split() needs this

  def toJSS[T <: AnyRef:Manifest]( ts:Seq[T] ):String =
    ts.map( t => write( t ).replace(NDEL,"") ).mkString( NDEL )

  def fromJSS[T:Manifest](jss:String):Seq[T] =
    jss.split( QNDEL ).filter( _.size > 0 ).map( js => fromJS[T]( js )).
    filter( _.isDefined ).map( _.get )

}

