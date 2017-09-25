package rajkumar.org.utils

import akka.actor._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util._
import scala.concurrent.Future
import scala.concurrent.duration._

// A General-purpose Scheduler that runs every period
//    runs all the functions f added through add(f) every period
// Note: Beware of closures

class Scheduler( period: FiniteDuration, delay:Boolean = true ) {

  // - Api --------
  def add( f: () => Future[Boolean]  ) = fs = fs ++ Seq( f )
  def start():Cancellable = {
    println("Scheduler :: will run updates every "+ period )
    Scheduler.schr.schedule( initialDelay = startDelay, period, runnable)(
      Scheduler.executor )
  }
  def stop(c:Cancellable) = c.cancel()

  // - Privates --------
  var fs:Seq[ () => Future[Boolean] ] = Seq[ () => Future[Boolean]]()
  val runnable  = new Runnable { def run() = fs.foreach( f => f() ) }
  val startDelay:FiniteDuration = if( delay ) period else (30 seconds )
}

// -----------------
object Scheduler {

  val as                = ActorSystem()
  val schr              = as.scheduler
  val executor          = as.dispatcher

  def kill():Unit = as.terminate()

  // - Testing --------------------------
  def time():Long = System.currentTimeMillis
  def main( args: Array[String]):Unit = {
    val s1 = new Scheduler( 1 minute )
    val s2 = new Scheduler( 30 seconds )
    s1.add( () => Future{ Thread.sleep(10000);println("F1:" + time()); true }  )
    s2.add( () => Future{ Thread.sleep(5000);println("F2:" + time() ); false }  )
    s1.start()
    s2.start()
  }
}

