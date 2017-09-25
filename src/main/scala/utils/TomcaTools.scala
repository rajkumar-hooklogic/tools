package rajkumar.org.utils

import javax.servlet._
import javax.servlet.http._
import javax.servlet.http.HttpServletResponse._

import java.util.concurrent.Executors

import ch.qos.logback.classic.ViewStatusMessagesServlet

import org.apache.catalina.startup.Tomcat
import org.apache.catalina.connector.Connector

import java.io.File

import scala.collection.JavaConversions._


// Embedded tomcat
// Runs provided servlet at host:port/ and last-logs from Log at /lastlogs
// See main() for sample use

object TomcaTools {

  val Name = "Tomcat"
  def runTomcat( servlet:HttpServlet, port:Int=8080 ):Tomcat = {
    val t = new Tomcat(); t.setPort( port )
    val c = t.addContext( "",
      new File( System.getProperty("java.io.tmpdir")).getAbsolutePath)
    Tomcat.addServlet( c, Name, servlet )
    c.addServletMapping("/*", Name)
    t.start
    t
  }

  // For https
  //
  // You need a chain-certificate (fullchain.pem) and private-key
  // (privkey.pem) from https://certbot.eff.org/ or some such
  //
  // Add into into the keystore (.keystore)
  // # openssl pkcs12 -export -in fullchain.pem -inkey privkey.pem
  // -out cert_and_key.p12 -name tomcat -CAfile chain.pem -caname root
  // # keytool -importkeystore -deststorepass <changeit> -destkeypass
  // <changeit> -srckeystore cert_and_key.p12 -srcstoretype PKCS12
  // -srcstorepass <changeit> -alias tomcat
  // keytool -import -trustcacerts -alias root -file chain.pem
  def runTomcatSecure( servlet:HttpServlet, port:Int=8080, sport:Int=8443 ):Tomcat = {
    val t = new Tomcat(); t.setPort( port )
    val c = t.addContext( "",
      new File( System.getProperty("java.io.tmpdir")).getAbsolutePath)
    Tomcat.addServlet( c, Name, servlet )
    c.addServletMapping("/*", Name)

    val keyAlias      = "tomcat"
    val password      = "changeit"
    val keystorePath  = System.getProperty("user.home") + "/.keystore"

    val httpsConnector = new Connector()
    httpsConnector.setPort( sport )
    httpsConnector.setSecure(true)
    httpsConnector.setScheme("https")
    httpsConnector.setAttribute("keyAlias", keyAlias)
    httpsConnector.setAttribute("keystorePass", password)
    httpsConnector.setAttribute("keystoreFile", keystorePath )
    httpsConnector.setAttribute("clientAuth", "false")
    httpsConnector.setAttribute("sslProtocol", "TLS")
    httpsConnector.setAttribute("protocol",
      "org.apache.coyote.http11.Http11AprProtocol")
    httpsConnector.setAttribute("SSLEnabled", true)

    val service = t.getService()
    service.addConnector(httpsConnector)

    // Redirect
    t.getConnector().setRedirectPort(443)

    t.start
    t
  }

  // Utils
  def getQPath( q:HttpServletRequest ):String =
    q.getRequestURI.substring( q.getContextPath.size )
  def getParams( q:HttpServletRequest, qp:String ):Array[String] =
    q.getParameterMap.getOrElse( qp, Array(""))
  def getParam( q:HttpServletRequest, qp:String ):String = getParams(q,qp).head
  def getParaMap( q:HttpServletRequest ):Map[String,String] =
    q.getParameterMap.collect{ case(k,vs) if (vs.size > 0 && vs.head.size > 0) =>
    (k,vs.head)}.toMap
  def getAllParams( q:HttpServletRequest ):String = {
    val pas = q.getParameterMap
    pas.map{ case(k,va) => k +":"+ va.mkString(",") +"\t" }.mkString("\n")
  }
  def getBody( r:HttpServletRequest):String =
    scala.io.Source.fromInputStream(r.getInputStream)(
      scala.io.Codec("UTF-8")).mkString

  // Sample Usage (look at http://localhost:9090 )
  def main( args:Array[String]):Unit = {
    runTomcat( new TestServlet() )
    Log.log( "Test1" )
    Thread.sleep( 60000 )
  }

}

class TestServlet extends HttpServlet {
  override def service( q:HttpServletRequest, p:HttpServletResponse ):Unit = {
    p.setContentType("application/json")
    p.getWriter.write( Json.toJS(  Log.lastLogs()   ) )
  }
}

