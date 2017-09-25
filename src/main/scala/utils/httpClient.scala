package rajkumar.org.utils

import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.protocol._
import org.apache.http.entity._
import org.apache.http.protocol._
import org.apache.http.message._
import org.apache.http.util._
import org.apache.http.params._
import org.apache.http.client.entity._
import org.apache.http.client.methods._
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.client.config._
import org.apache.http.impl.client._
import org.apache.http.impl.conn._
import org.apache.http.HttpHeaders

import java.io.InputStream

import scala.collection.JavaConversions._

// HttpClient tools
//  - make gets and posts simpler
//  - helps with cookie access
//  - eases logins and sessions

object httpClient {

  // some static httpclient stuff including timeouts
  val TOMS = 2 * 1000           // timeout in ms
  val MAX_CONNECTIONS = 200
  val useragent = "New Useragent/1.0"

  // val httpclient  = HttpClients.createDefault
  val cm = new PoolingHttpClientConnectionManager
  val httpclient  = HttpClients.custom.setConnectionManager(cm).build
  val responseHandler = new BasicResponseHandler()

  // - Api ----------------

  // get url contents as string
  def get( url: String ) : String = {
    val g = new HttpGet( url )
    val c = HttpClientContext.create
    val r = httpclient.execute( g, c)
    val e = r.getEntity
    val s = EntityUtils.toString( e)
    g.releaseConnection
    s
  }

  // get url contents as bytes
  def getbytes( url: String ) : Array[Byte] =
    EntityUtils.toByteArray( httpclient.execute( new HttpGet( url )).getEntity)

  // get url contents, but pass the properties passed int the map to header
  def get( url: String, hp: Map[String,String] ) : InputStream = {
    val g = new HttpGet( url )
    for((k,v) <- hp ) g.addHeader(k,v)
    val r = httpclient.execute( g )
    val is = r.getEntity.getContent()
    is
  }

  // post this map of data to this url
  def post( url: String, ps: Map[String,String]):String = {
    val prs = new java.util.ArrayList[NameValuePair]()
    for((k,v) <- ps) prs.add( new BasicNameValuePair( k, v ))
    val e = new UrlEncodedFormEntity( prs, "UTF-8")
    val httpPost = new HttpPost( url )
    httpPost.setEntity( e )
    val r = httpclient.execute(httpPost)
    val rs = EntityUtils.toString( r.getEntity )
    val sc = r.getStatusLine.getStatusCode
    r.close
    rs
  }

  // post json data to this url
  def post( url: String, js: String,
    ctype: ContentType = ContentType.APPLICATION_JSON ):String = {
    val httpPost = new HttpPost( url )
    val e = new StringEntity(js, ContentType.APPLICATION_JSON )
    // e.setContentType("application/json")
    httpPost.setEntity( e )
    val r = httpclient.execute(httpPost)
    val rs = EntityUtils.toString( r.getEntity )
    val sc = r.getStatusLine.getStatusCode
    r.close
    rs
  }

  // post l bytes of data from steam to url
  def post( url: String, is: java.io.InputStream, l: Long ):String = {
    val httpPost = new HttpPost( url )
    val e = new InputStreamEntity( is, l, ContentType.APPLICATION_OCTET_STREAM )
    httpPost.setEntity( e )
    val r = httpclient.execute( httpPost )
    val rs = EntityUtils.toString( r.getEntity )
    val sc = r.getStatusLine.getStatusCode
    r.close
    rs
  }

  // post a file to url
  def postf( url: String, f: String ):String = {
    val httpPost = new HttpPost( url )
    val e = new FileEntity( new java.io.File(f), ContentType.APPLICATION_OCTET_STREAM )
    httpPost.setEntity( e )
    val r = httpclient.execute( httpPost )
    val rs = EntityUtils.toString( r.getEntity )
    val sc = r.getStatusLine.getStatusCode
    r.close
    rs
  }


  // - Put -----
  // put json string to url
  def put( url: String, js: String,
    ctype: ContentType = ContentType.APPLICATION_JSON ):String = {
    val put = new HttpPut( url )
    val e = new StringEntity(js, ContentType.APPLICATION_JSON )
    put.setEntity( e )
    val r = httpclient.execute(put)
    val rs = EntityUtils.toString( r.getEntity )
    val sc = r.getStatusLine.getStatusCode
    r.close
    rs
  }

  // - delete ---
  // delete url
  def delete( url: String):String = {
    val del = new HttpDelete( url )
    val r = httpclient.execute( del )
    val rs = EntityUtils.toString( r.getEntity )
    val sc = r.getStatusLine.getStatusCode
    r.close
    rs
  }

  // get all cookies
  def getCookies(context:HttpClientContext):Seq[ Map[String, String]] = {
    val cs = context.getCookieStore.getCookies
    cs.map( c => Map(
      "domain" -> c.getDomain,
      "name" -> c.getName, "value" -> c.getValue, "path" -> c.getPath,
      "expiry" -> (if( c.getExpiryDate != null) c.getExpiryDate.toString else "" )))
  }

  // is this a valid/live url
  def isValid( url: String ) : Boolean = {
    val r = httpclient.execute( new HttpHead( url ))
    r.getStatusLine.getStatusCode == 200
  }

  // login to this url with given creds
  def login( url: String, user: String, pass: String ) =
    post( url, Map( "login" -> user, "password" -> pass ))

  def main( args: Array[String]):Unit = {}

}

