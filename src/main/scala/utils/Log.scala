package rajkumar.org.utils

import collection.JavaConversions._

// General purpose Logger and Debug object
// Log.log( msg ) logs the msg while Log.out( msg ) prints to stdout
//   if the log-level is set high-enough via the environment variable LOG_LEVEL
//   LOG_LEVEL can be :: error | warning | info | debug | trace
//
// Log.log logs to the standard logger-configuration, syslogd and LogServlet
//  (which makes the last BUF_LOGS logs available)
//
// calling addSyslogHost(ip) additionally logs to syslog on remote machine
// default ip read from REMOTE_SYSLOG_HOST if set, else ds-jenkins


object Log {

  object Level extends Enumeration {
    type Level = Value
    val Quiet, Error, Warning, Info, Debug, Trace = Value
  }
  import Level._

  // - initialization ----------------------------------------
  // Do not modify logLevel. Set Env. variable instead
  val DEFAULT_REMOTE_HOST = "ec2-184-73-128-146.compute-1.amazonaws.com" // ds-jenkins
  var REMOTE_HOST = System.getenv.toMap.getOrElse( "REMOTE_SYSLOG_HOST",
    DEFAULT_REMOTE_HOST )
  var SYSLOG_HOST = "localhost"
  val Prefix      = "test.log"
  val BUF_LOGS    = 1000
  val logLevel    = toLevel( System.getenv.toMap.getOrElse("LOG_LEVEL","Warning"))
  val lr        = org.slf4j.LoggerFactory.getLogger( Prefix )
  addAppenders()

  // - Api --------------------------------------------------
  // is logging enabled ?
  def isLog( mLevel:Level = Trace ):Boolean = mLevel <= logLevel

  // write to stdout
  def out( msg:String, mLevel:Level = Trace ) =
    if ( mLevel <= logLevel )
      print( Prefix +"("+ mLevel.toString +") "+ fmsg(msg) )

  // log to std-logger, syslogd and LogServlet
  def log( msg:String, mLevel:Level = Trace ) =
    if ( mLevel <= logLevel ){
      val os = fmsg( msg )
      mLevel match {
        case Error      =>   lr.error(  os )
        case Warning    =>   lr.warn(   os )
        case Info       =>   lr.info(   os )
        case Debug      =>   lr.debug(  os )
        case _          =>   lr.info(  os )
      }
    }

  def addSyslogHost( ip:String=REMOTE_HOST ) = addSyslogAppender( ip )


  // - Privates ------------------------------------------
  private def fmsg( s:String):String = s"$SYSLOG_HOST : $s"
  // string to Enum Level
  private def toLevel( ls: String ): Level =
    ls.toLowerCase match {
      case "quiet"    => Quiet
      case "error"    => Error
      case "warning"  => Warning
      case "info"     => Info
      case "debug"    => Debug
      case "trace"    => Trace
      case "verbose"  => Trace
      case _          => Trace
    }

  // adds a Syslogd appender and a LogAppender
  private def addAppenders() = {
    val lb        = lr match {
      case l:ch.qos.logback.classic.Logger => l
    }
    val context = lb.getLoggerContext

    // Log Appender
    LogAppender.setContext( context )
    LogAppender.start()
    lb.addAppender( LogAppender )

    // Syslogd appender
    addSyslogAppender( SYSLOG_HOST )
  }

  private def addSyslogAppender(ip:String) = {
    val lb        = lr match {
      case l:ch.qos.logback.classic.Logger => l
    }
    val context = lb.getLoggerContext

    // Syslogd appender
    val sa = new ch.qos.logback.classic.net.SyslogAppender()
    sa.setContext( context )
    sa.setSyslogHost( ip )
    sa.setPort( 514 )
    sa.setFacility( "USER" )
    sa.start()
    lb.addAppender( sa )
  }


  def lastLogs():Array[String] = LogAppender.lastLogs

  // -- ---


  // -----
  def main( args:Array[String]):Unit = {
    println( isLog() )
    log("-Test Log-")
    addSyslogHost()
    log("-Test Log 2-")
  }

  // - Log Appender, appends last BUF_LOGS logs to our rolling cache
  import rajkumar.org.algos.FStack
  import ch.qos.logback.classic.spi.ILoggingEvent
  import ch.qos.logback.core.AppenderBase
  object LogAppender extends AppenderBase[ILoggingEvent] {

    // buffer the last N logs
    val buf = new FStack[String]( BUF_LOGS )

    def append( e:ILoggingEvent){
      val ls  = e.getLevel.toString
      val ts  = e.getTimeStamp
      val th  = e.getThreadName
      val lr  = e.getLoggerName
      val ms  = e.getFormattedMessage
      val os  = s"$ts $ls $th $lr \t $ms"
      buf.push( os )
    }
    def lastLogs():Array[String] = buf.elements()
  }
}

// Companion Servlet displays last N logs
import javax.servlet._
import javax.servlet.http._
class LogServlet extends HttpServlet {
  override def service( q:HttpServletRequest, p:HttpServletResponse ):Unit = {
    p.setContentType("application/json")
    val os = Json.toJS[Array[String]]( Log.lastLogs() )
    p.getWriter.write( os )
  }
}


