package rajkumar.org.utils

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

import scala.util.Success
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._

//
// Simple generic logger that logs data to Datadog and Graphite
// (uses utils.httpClient for DataDog)

// use one of:
// DataLogger.log( label, value, tag(optional), host(optional) )
// DataLogger.log( Seq( (l1,v1,t1,h1), (l2,v2,t2,h2) )
// DataLogger.event( title, txt, ... )

// Notes: Time is POSIX seconds (System.currentTimeMillis / 1000 )
//        The label "Prefix." is prepended to all labels and titles

case class Data     ( series:Seq[DDMetric] )
case class DDMetric ( metric:String, points:Seq[ Seq[Float] ],
  tags:Seq[String],  host:String )
case class DDEvent ( title:String, text:String, date_happened:Int,
  priority:String, host:String, tags:Seq[String], alert_type:String,
  aggregation_key:String, source_type_name: String)


object DataLogger {
  implicit val formats = Serialization.formats(NoTypeHints)

  var Prefix = "rajkumar.info"
  type Datum = (String,Float,String,String)
  type GDatum = (String,Float)

  // - api ---
  // data is seq of (name, tag, host, value) to be plotted at current time
  // Note: Datadog tags need to start with a character (no numeric tags)
  def log( data: Seq[Datum]):Unit = {
    val pdata = data.map{ case(n,v,t,h) => (s"$Prefix.$n", v, s"tag.$t", h) }
    ddadd( pdata )
    gadd( toGData( pdata ))
  }
  def log( name:String, value:Float, tag:String, host:String ):Unit =
    log( Seq( (name,value,tag,host) ))

  def event( name:String, txt:String, pri:String="normal", tags:Seq[String]=Seq(),
    host:String="", level:String="info", key:String="", source:String="" ) =
    ddevent( name, txt, pri, host, tags, level, key, source )

  def addPrefix( p:String ):Unit =
    if( p.startsWith(".")) Prefix = s"$Prefix$p"
    else Prefix = s"$Prefix.$p"

  // - Privates ------------
  // - Datadog -
  // add data to datadog
  private def ddadd( nvths: Seq[Datum] ):String = {
    val ct = time
    val ms = nvths.map{ case(n,v,t,h) =>
      DDMetric( n, Seq( Seq(ct,v)), Seq(t),h)}
    val js = toJson( ms )
    httpClient.post( purl, js )
  }
  // add events to Datadog (prefix added to title, "ds" to tags"
  private def ddevent(n:String, txt:String, p:String, h:String, ts:Seq[String],
    l:String, k:String, s:String):String = {
      val pn  = s"$Prefix.$n"
      val dts = ("ds" +: ts).distinct
      val js  = Json.toJS[ DDEvent ](DDEvent(pn, txt, time(), p, h, dts, l, k, s))
      httpClient.post( eurl, js )
  }

  private def toJson( d:Seq[DDMetric] ):String = write( Data( d ))

  private def time():Int = (System.currentTimeMillis / 1000).toInt

  // convert datadog format to graphite format
  // (label,v, tag, host) => (name.tag.host,v )
  private def toGDatum( d:Datum ):GDatum = {
    val p1 = d._1
    val p2 = if( d._3.size > 0 ) s".${d._3}" else ""
    val p3 = if( d._4.size > 0 ) s".${d._4}" else ""
    val v  = d._2
    ( s"$p1$p2$p3", v )
  }

  private def toGData( data:Seq[Datum]):Seq[GDatum] =
    data.map( d => toGDatum( d ))

  // Datadog REST api keys
  private val api_key = "12345"
  private val app_key = "2222db"
  private val prefix  = "https://app.datadoghq.com/api/v1/"
  private val purl    = prefix + "series?api_key="+ api_key
  private val eurl    = prefix + "events?api_key="+ api_key

  // - Graphite -----------------------------------------
  private val GServerIP       = "10.8.8.8"
  private val GServerIP_Dev   = "10.175.110.110"
  private val GPort           = 2003

  // add data to graphite; output format: "name value time-in-seconds \n"
  private def gadd( nvs: Seq[GDatum], ip:String=GServerIP_Dev ):Unit = {
    val s = new java.net.Socket( GServerIP_Dev, GPort )
    val w = new java.io.PrintWriter( s.getOutputStream, true )
    val t = time()
    val oss = nvs.map{ case(n,v) => s"$n $v $t" }
    val os = oss.mkString("\n")
    w.println( os )   // needs println rather than print
    w.flush
    w.close
  }

  // - Testing ----
  def main( args:Array[String]):Unit = {
    event( "Testing Title", "Test Body" )
  }

}
