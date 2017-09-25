package rajkumar.org.utils

import javax.servlet._
import javax.servlet.http._
import java.util.concurrent.Executors

import scala.collection.mutable
import scala.util._
import scala.concurrent._
import scala.concurrent.duration._

// - Tool to collect and display statistics/data
//
// Usage: Info.inc( label, double-value )  increments value for label
//        Info.set( label, double-value )  sets a value for a label
//
//        Info.run( optional-func())       every period cumulates values
//                                         runs func()
//
// If Info.run( f ) is invoked, every  period
//  1) optionally sends data to Datadog/Graphite
//  2) clears out current-counters and adds their values to global-counter
//  3) optionally runs f() with the current values of all labels as parameter
//
// The Cumulative and Periodic updates are availabe as json.
//   optionally at datadog/graphite
//
// Note: Accumulator is thread-safe, (but synchronous; careful with parallelism)

object Info {

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool( 1000 ))


  type IMap = Map[String,Map[String,Double]]

  val period:FiniteDuration = 5 minutes
  var dg:Boolean            = true       // Datadog/Graphite logging
  val TUnit:TimeUnit        = MINUTES
  var sch:Scheduler         = new Scheduler( period ); sch.add( update )
  val Emptyf  = (m:IMap) => m
  var torun:( IMap ) => IMap  = Emptyf   // preproc f(); runs before accumulation

  // Api
  def inc( k: String, tab:String, cnt: Double = 1.0 ) = Future {
    if( cnt != 0.0 ) sinc( k, tab, cnt ) else Unit
  }


  // Starts the accumulator. Optional f() is run before accumulation
  def run( f:( IMap ) => IMap = Emptyf) = {
    sch.start()
    torun = f
  }

  // set current and clear out totals so that the totals don't accumulate
  def set( k: String, tab:String, value: Double ) = {
    setc(k,tab,value)
    if( totals.contains( k ))  totals(  k ).remove( tab )
  }
  // set current value (accumulated value remains)
  def setc( k: String, tab:String, value: Double ) = {
    val m = current.getOrElse( k, mutable.Map() )
    m( tab ) = value
    current( k ) = m
  }
  // clear out a previously-set or incremented value
  def unset( k: String, tab:String ) = {
    if( current.contains( k )) current( k ).remove( tab )
    if( totals.contains( k ))  totals(  k ).remove( tab )
  }

  def jsonInfo():String = {
    val status  = Status.jsonStatus
    val period = Tools.sJson( currentm() )
    val total  = Tools.mJson( totalsm() )
    s"""{ "status":$status, "current": $period, "total": $total } """
  }

  def currentm():IMap = toIM( current)

  def totalsm():IMap  = toIM( totals)

  def addPrefix( p:String ):Unit = DataLogger.addPrefix( p )


  // - Privates -----------------
  private val totals  = mutable.Map[String, mutable.Map[String,Double]]()
  private val current = mutable.Map[String, mutable.Map[String,Double]]()
  def sinc( k: String, t:String, cnt: Double ) = synchronized {
    val m = current.getOrElse( k, mutable.Map[String,Double]() )
    m( t ) = m.getOrElse( t, 0.0 ) + cnt
    current( k ) = m
  }

  // run the optional function; accumulate current to total; clear current
  private def update():Future[Boolean] = Future{
    val mcur = torun( toIM(current) )
    accumulate( mcur )
    true
  }

  private def accumulate( curm: IMap ):Unit = {
    for((k,cm) <- curm ){
      val tm = totals.getOrElse( k, mutable.Map[String,Double]() )
      for( (t,v) <- cm ) tm( t ) = tm.getOrElse( t, 0.0 ) + v
      totals( k ) = tm
    }
    if( dg ) DataLogger.log( current.map{ case(k,m) => m.map{ case(t,v) =>
      (k,v.toFloat,t,"")}}.flatten.toSeq )
    current.clear
    Status.pt = System.currentTimeMillis
  }

  private def toIM( m: mutable.Map[String, mutable.Map[String,Double]]) =
    m.map{ case(k,im) => (k,im.toMap)}.toMap
}

object Tools {
  // - Display Helpers --------
  def mJson( m: Info.IMap ):String = Json.toJS( m.toMap )
  def sJson( m: Info.IMap ):String = Json.toJS( m.toMap )

  def msHtml( m: Info.IMap ):Seq[String] =
    m.map{ case(t,dm) => toHtml( dm, t )}.toSeq

  def toHtml( m:Map[String,Double], c:String="", c1:String="", c2:String=""):
    String = {
    val cap = if( c.size > 0 ) "<caption>$t</caption>" else ""
    val hr  = if(c1.size >0 || c2.size >0) "<tr><th>$c1</th><th>$c2</th></tr>"
              else ""
    val ts = m.map{ case(k,v) => s"<tr><td>$k</td><td>$v</td></tr>" }.
      mkString("\n")
    s"<table> $cap $hr $ts </table>"
  }
}

// - Status information ------------------------------------
import java.lang.management._
object Status {

  val p:String            = Info.TUnit.toString.toLowerCase

  // - Api ----
  def etms():Long         = System.currentTimeMillis - st
  def petms():Long        = System.currentTimeMillis - pt
  def et():FiniteDuration = Duration( etms(), MILLISECONDS)
  def pet():FiniteDuration = Duration( petms(), MILLISECONDS)
  def jsonStatus() = "{" +
  s""" "uptime ($p)":"${uptime()}", """ +
  s""" "current ($p)":"${curtime()}", """ +
  f""" "system load":"${sload()}%4.2f", """ +
  s""" "process memory (MB)":"${totMB()}", """ +
  s""" "available memory (MB)":"${availMB()}" """ +
  "}"

  // Privates
  val st = System.currentTimeMillis
  var pt = System.currentTimeMillis

  def sload() = ManagementFactory.getOperatingSystemMXBean.getSystemLoadAverage
  def totMB():Int = (Runtime.getRuntime.totalMemory / 1000000 ).toInt
  def freeMB():Int = (Runtime.getRuntime.freeMemory / 1000000 ).toInt
  def maxMB():Int = (Runtime.getRuntime.maxMemory / 1000000 ).toInt
  def availMB():Int = maxMB() - totMB() + freeMB()
  def memory():String = s"Mem(MB): Total:${totMB()}, Free:${freeMB()}, Max:${maxMB()}, Avail:${availMB()}}"
  def uptime( unit: TimeUnit = Info.TUnit ):String = {
    val d = et.toUnit( unit )
    f"$d%6.2f"
  }
  def curtime( unit: TimeUnit = Info.TUnit ):String = {
    val d = pet.toUnit( unit )
    f"$d%6.2f"
  }

}

