package rajkumar.org.utils

import com.maxmind.geoip2._
import com.maxmind.geoip2.model.CityResponse
import com.maxmind.db.Reader.FileMode

import java.io._
import java.net._

import scala.collection.JavaConversions._
import scala.util._

// The dbs are updated every week at http://maxmind.com
// These files were downloaded 4/2016
object GeoIP {
  val DBF  = "/GeoLite2-City.mmdb"
  val citydb =new DatabaseReader.Builder(getClass.getResourceAsStream(DBF)).
    fileMode( FileMode.MEMORY).
    locales( List("en-US") ).
    build

  def zip( ip: String ):String =
    getCity( ip ) match {
      case Some( c ) => c.getPostal.getCode
      case _         => ""
    }
  def latLong( ip: String ):(Float,Float) =
    getCity( ip ) match {
      case Some( c ) => (c.getLocation.getLatitude.toFloat,
        c.getLocation.getLongitude.toFloat)
      case _         => (0.0f,0.0f)
    }
  def timezone( ip: String ):String =
    getCity( ip ) match {
      case Some( c ) => c.getLocation.getTimeZone
      case _         => ""
    }
  def llzt( ip: String ):(Float,Float,String,String) =
    getCity( ip ) match {
      case Some( c ) =>
      (c.getLocation.getLatitude.toFloat, c.getLocation.getLongitude.toFloat, c.getPostal.getCode, c.getLocation.getTimeZone)
      case _         => (0.0f,0.0f,"","")
    }

  // - Private -
  private def getCity( ip: String ):Option[CityResponse] = {
    val ipa = InetAddress.getByName( ip )
    Try( citydb.city( ipa )) match {
      case Success( resp ) => Some(resp)
      case Failure( e )    => None
    }
  }

  // - Testing -------------
  def myIP():String = {
    val is = NetworkInterface.getNetworkInterfaces
    val gis = is.filter( i =>  i.isUp && ! i.isLoopback ).toSeq
    val as = if( gis.size > 0 ) gis.head.getInetAddresses.toSeq else Seq()
    if( as.size > 0 ) as.head.getHostAddress else ""
  }
  def test() = {
    val ips = Seq( "10.210.161.99","161.170.236.10", "10.183.252.26",
      "161.170.236.10","10.183.252.26", "10.210.161.100","161.170.238.10")
    val gips = ips.filterNot( _.startsWith("10") )
    gips.map( ip => (ip, latLong( ip ))).foreach( println )
  }

  def main( args: Array[String]):Unit = {
    val ip = if( args.size > 0 ) args(0) else "128.59.10.20"
    val (la,ln,z,t) = llzt( ip )
    println(s"Info for $ip: zip:$z, lat/lng: $la/$ln, timezone:$t")
  }

}
