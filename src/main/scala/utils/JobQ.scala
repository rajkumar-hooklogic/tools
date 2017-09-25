package rajkumar.org.utils

import org.apache.catalina.startup.Tomcat

import akka.actor._

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import java.time.LocalDateTime
import java.io.File
import javax.servlet._
import javax.servlet.http._
import java.util.concurrent.Executors

import scala.util._
import scala.concurrent._
import scala.concurrent.duration._


/** Periodically run f(arg:String):Boolean asynchronously until success
  *
  * add(f,arg) returns future, schedules f(arg) to run every Period to success
  * If f(arg) throws exception, callback is called with failure
  * Notes: Job can be in 4 states/queues: Working, Waiting, Error and Done
  *        Queues can be observed at localhost:9090
  *        Scheduler times-out jobs and stops after TimeOut duration (1 day)
  *
  * @example {{{
  * val fs = Seq( add( f, "a"), add( g,"b") )
  * Future.sequence( fs ) onComplete {
  *   case Success( results ) =>
  *     println(s"Failed: ${results.filterNot( x => x ).size}")
  *   case Failure( e   )     => println(s"Error: ${e.getMessage}" )
  * }
  * }}}
  */

// Should it be:  class JobQ( period: FiniteDuration ) extends HttpServlet {
object JobQ extends HttpServlet {

  // - Execution Context ------
  private val Parallelism:Int         = 8
  private val Period: FiniteDuration  = 10 minutes
  private val TimeOut:FiniteDuration  = 1 day
  private val InfoPort:Int            = 9090

  private implicit val ec = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool( Parallelism ))


  // - Api ---------------------------------
  /** starts job and adds to system */
  def add( g:(String) => Boolean, arg: String ):Future[Boolean] = {
    val info = Info( arg, LocalDateTime.now.toString, "", 0)
    val w = Job( g, arg, info)
    ondone( start(w), w)
    w.p.future
  }
  /** Has this job been added already? (job identified by arg) */
  def contains( arg: String ):Boolean = this.synchronized {
    val ms = for {
      (qname,q) <- qs
      w <- q
      if( w.arg == arg )
    } yield w
    ms.size >= 1
  }
  /** remove job from system (identified by arg) */
  def remove( j: Job ):Unit = this.synchronized {
    for((n,q) <- qs ) removeUnsafe( n, j )
  }

  /** Returns fail for working/waiting jobs; stops tomcat and scheduler */
  def stop():Unit = {
    for( w <- list("waitQ") ++ list("workingQ"))
      if( ! w.p.isCompleted ) w.p.success( false )
    tomcat.stop
    rerun.cancel
  }
  /** Returns map of job-statuses indexed by arg */
  def status():Map[String,String] = {
     val seq = for {
       (qname,q)  <- qs
       w          <- q
     } yield (w.arg, qname)
     seq.toMap
   }

  // - HttpServlet -------
  override def service( q:HttpServletRequest, r:HttpServletResponse):Unit = {
    r.setStatus( HttpServletResponse.SC_OK )
    r.setContentType("application/json;charset=utf-8")
    r.getWriter.println( qsjson() )
  }

  // - Privates -------------------------------------
  // Object variables
  private val rerun   = schedule(  Period )
  private val tomcat  = runTomcat( InfoPort )
  private val Start   = System.currentTimeMillis

  // - Case classes and data-structures -----
  private case class Info( id: String, start:String, var end:String,
    var tries:Int, var msg: String="" )
  private case class Job( f:(String) => Boolean, arg: String, info:Info,
    p: Promise[Boolean] = Promise[Boolean]())

  // Queues and queue-helpers
  private val qs = collection.mutable.Map[String,Seq[Job]](
    "waitQ"     -> Seq[Job](),
    "workingQ"  -> Seq[Job](),
    "errorQ"    -> Seq[Job](),
    "doneQ"     -> Seq[Job]() )

  private def add( qname: String, j: Job ) = this.synchronized {
    qs(qname) = qs(qname) :+ j
  }

  private def remove( qname: String, j: Job ) = this.synchronized {
    qs(qname) = qs(qname).filterNot( _.arg == j.arg )
  }

  private def empty( qname: String ) = this.synchronized { qs(qname) = Seq() }

  private def list( qname: String ) = this.synchronized { qs(qname)}

  //  This allows for other thread safe operations to use this method
  private def removeUnsafe( qname: String, j: Job ) = {
    qs(qname) = qs(qname).filterNot( _.arg == j.arg )
  }


  // - Helper routines ------
  // Internally, JobQ starts job asynchronously with a future and listens for
  // completions (with ondone below)
  private def start( w: Job ):Future[Boolean] = {
    w.info.tries = w.info.tries + 1
    add( "workingQ", w )
    future{ w.f( w.arg ) }
  }
  // Job completed successfully, move to doneQ and callback success
  private def success( w: Job ) = {
    w.info.end = LocalDateTime.now.toString
    remove("workingQ", w )
    add("doneQ", w )
    w.p.success( true )
  }
  // Job returned failure. Retry!
  private def retry( w: Job ) = {
    remove("workingQ", w )
    add("waitQ", w )
  }
  // Job threw Exception. Move to errorQ and callback failure
  private def failure( w: Job, e:Throwable ) = {
    e.printStackTrace()
    w.info.end = LocalDateTime.now.toString
    remove("workingQ", w )
    add("errorQ", w )
    w.p.failure( e )
  }
  // Sets up private-callbacks which handle internal job-status-changes
  private def ondone( f: Future[Boolean], w: Job ):Unit = {
    f onComplete {
      case Success( res ) =>  if( res ) success(  w )
                              else      retry(    w )
      case Failure( e )   =>            failure(  w, e )
    }
  }


  // - runs scheduler which restarts jobs in waitQ every Period
  // TimesOut after TimeOut time
  private def schedule( period:FiniteDuration ):Cancellable = {
    val as                = ActorSystem()
    val scheduler         = as.scheduler
    val executor = as.dispatcher

    val runnable = new Runnable { def run() = {
      for( j <- list("waitQ") )
        ondone( start( j ), j )
      empty("waitQ")
      val runTime = System.currentTimeMillis - Start
      if( runTime > TimeOut.toMillis ) stop() // Done for the day
    }}
    scheduler.schedule( initialDelay = Period, Period, runnable)(executor)
  }

  // - embedded Tomcat ----
  // - Make status info availabe at "port" via a Tomcat-embedded-server
  private def runTomcat(port:Int = 9090):Tomcat = {
    val t = new Tomcat(); t.setPort( port )
    val c = t.addContext( "",
      new java.io.File(System.getProperty("java.io.tmpdir")).getAbsolutePath )
    Tomcat.addServlet( c, "QueueService", JobQ )
    c.addServletMapping("/*","QueueService")
    t.start
    t
  }

  // - Utilities -------
  // Json Representations
  private implicit val formats = Serialization.formats(NoTypeHints)
  private def jjson( j: Job ):String = write(j.info )
  private def qjson( q: Seq[Job]):String =
    "["+ q.toSeq.map( jjson(_)).mkString(",") +"]"
  private def qsjson():String = this.synchronized {
    val mestrings = for {
      (n,q) <- qs
      mestr = "\""+ n +"\":" + qjson( q )
    } yield mestr
    "{"+ mestrings.mkString(",") +"}"
  }

  // -- Testing -----------------------------------------------------
  val WaitSeconds = 5
  // Test function g(x) : runs for a random time (max WaitSeconds)
  // returns true/false or throws Exception
  private val g = (x:String) => {
    val r = Random.nextInt( WaitSeconds )
    print(s"Starting $x($r)..."); Thread.sleep(r*1000); print(s"... $x : ")
    r match {
      case 0  => println("Success\n");  true
      case 1  => println("Error\n");    throw new Exception
      case _  => println("Failure\n");  false
    }
  }
  // Add a list of jobs to JobQ and blocks to completion
  private def qtest() = {
    val js = Seq("A","B","C","D","E","F","G","H")
    val fs = js.map( myadd( g, _ ))
    val f = Future.sequence( fs )
    f onComplete {
      case Success( res ) =>  println("Future[Seq] Success: " + res )
      case Failure( e )   =>  println(s"Future[Seq] Error ${e.getMessage}")
    }
    val a = Await.ready( f, 20 minutes ); println( "Await done")
    val af = a.asInstanceOf[ Future[ Seq[Boolean ]]] // Bug in 2.10, 2.11 OK
    System.exit( 0 )
  }
  // Wrapper to add job to JobQ and handle job-completion callbacks
  private def myadd( g:(String) => Boolean, id: String ):Future[Boolean] = {
    val f = add( g, id )
    f onComplete {
      case Success( res ) =>  if( res ) println(s"Job Done($id): Success")
                              else      println(s"Job Done($id): Fail")
      case Failure( e )   =>            println(s"Job Done($id): Error")
    }
    f
  }
  def main( args: Array[String]): Unit = {
    qtest()
  }
}

