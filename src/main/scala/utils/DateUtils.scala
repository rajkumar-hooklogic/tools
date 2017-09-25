package  rajkumar.org.utils 

import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar, Date}

import scala.concurrent.duration._
import scala.util._

// Some misc. date-helper functions
object DateUtils {

  val DAY_FORMAT      = "yyyy-MM-dd"
  val TIME_FORMAT     = "HH:mm:ss"
  val DAY_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
  def DateFormat      = new SimpleDateFormat( DAY_FORMAT )
  def TimeFormat      = new SimpleDateFormat( TIME_FORMAT )
  def DateTimeFormat  = new SimpleDateFormat( DAY_TIME_FORMAT )

  def validDate( d: String):Boolean = Try(DateFormat.parse(d)).isSuccess
  def validTime( t: String):Boolean = Try(TimeFormat.parse(t)).isSuccess
  def validDateTime( dt: String):Boolean = Try(DateTimeFormat.parse(dt)).isSuccess

  // for date = startdate, use positive numDays
  // for date = enddate,   use negative numDays
  // date is getting corrupted in multi-threaded access, so synchronized
  def dateList( date: String, numDays: Int ) : Seq[String] = synchronized {
    val ns = if (numDays > 0 ) (0 to numDays-1) else ( numDays+1 to 0 )
    val dates = ns.map( n => dateAdd( date, n)).filter( validDate(_) )
    if (dates.size != numDays.abs) println(s"Date error for $date ($numDays)")
    dates
  }
  // date in "yyyy-mm-dd" format, add n days (n can be negative )
  def dateAdd( d: String, n: Int ) : String = synchronized {
    if( validDate( d )){
      val c = Calendar.getInstance
      c.setTime( DateFormat parse d )
      c.add( Calendar.DAY_OF_MONTH, n )
      DateFormat.format( c.getTime )
    }
    else ""
  }

  def now:String   = TimeFormat.format(new Date)

  val today:String = DateFormat.format( new Date )

  val yesterday:String  = {
    val c = Calendar.getInstance
    c.add( Calendar.DAY_OF_MONTH, -1 )
    DateFormat.format( c.getTime )
  }
  val tomorrow:String  = {
    val c = Calendar.getInstance
    c.add( Calendar.DAY_OF_MONTH, 1 )
    DateFormat.format( c.getTime )
  }
  // negative days => past
  def futureDay( days:Int):String  = {
    val c = Calendar.getInstance
    c.add( Calendar.DAY_OF_MONTH, days )
    DateFormat.format( c.getTime )
  }
  def msToString( ms:Long ):String = TimeFormat.format( new Date( ms ))
  def stringToMs( t:String ):Long = (TimeFormat parse t).getTime
}

