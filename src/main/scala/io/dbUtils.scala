package rajkumar.org.io
import java.sql.{Connection, ResultSet, Statement, DriverManager}

import scala.util._
import scala.collection.JavaConversions._


class dbUtils ( uri: String, usr: String, pwd: String){

  // Tools for using jdbc to connect to various HL databases that support jdbc
  // Creds need to be supplied in the env-variable DB_CREDS as "username/password"

  // The Driver Manager loads appropriate drivers
  // The classOf provides hints for sqlserver, postgres-jdbc
  classOf[ com.microsoft.sqlserver.jdbc.SQLServerDriver ]
  classOf[ org.postgresql.Driver ]

  val conn:Connection = DriverManager.getConnection(uri, usr, pwd)
  val stmt            = conn.createStatement
  stmt.setFetchSize( dbUtils.FETCH_SIZE )

  //- Api --------------------------
  def query( q: String)   = stmt.executeQuery( q )
  def update( q: String)  =
    Try( stmt.executeUpdate(q)) match {
      case Success( s ) => s
      case Failure( e ) => dbUtils.procErr( e, q )
    }

  def close = conn.close

  // - Query Executors ----------
  // execute the query q and return each row Seq[string]
  def exec( q: String ) : Iterator[ Seq[String] ] = {
    val r = query( q )
    Iterator.continually( if( r.next ) dbUtils.proc(r) else Seq()).
    takeWhile(_.size > 0)
  }
  // return each row as a tab delimited string
  def execute( q: String ) : Iterator[ String ] = {
    val r = query( q )
    Iterator.continually( if( r.next ) dbUtils.procRes(r) else "").
    takeWhile(_.size > 0)
  }
  // return each row as a map
  def execm( q: String ) : Iterator[ Map[String,String]] = {
    val r = query( q )
    Iterator.continually( if( r.next ) dbUtils.procMap(r)
      else Map[String,String]() ).takeWhile(_.size > 0)
  }
}

// - Statics ---------------------------------
object dbUtils {

  val FETCH_SIZE  = 1000000
  val DEL         = "\t"

  // - static helpers -----
  // return row as string
  private def procRes( r: ResultSet ): String =
  (1 to r.getMetaData.getColumnCount).map( r.getString(_)).mkString( DEL )

  // return row as seq-string
  private def proc( r: ResultSet  ): Seq[String] =
  (1 to r.getMetaData.getColumnCount).map( r.getString( _ ))

  // process a row using the result's metadata and return as map
  private def procMap( r: ResultSet ): Map[String,String] = {
    val rmd = r.getMetaData
    val m = collection.mutable.Map[String,String]()
    for( ci <- 1 to rmd.getColumnCount ){
      val v = r.getObject( ci ) match {
        case s:String => s
        case o:Object => o.toString
        case _        => ""
      }
      val k = rmd.getColumnName( ci )
      if( v.size > 0 ) m( k ) = v
    }
    m.toMap
  }

  // - Error Processing ---------------------
  def procErr( e: Throwable, q: String ): Int = {
    val m = e.getMessage
    println( q.substring(0, 100.min( q.size )) )
    println( m )
    val re = """(?s).*Position: (\d+).*""".r
    m match {
      case re(pos) => println(pos + " position: " +
        q.substring( pos.toInt-20, pos.toInt+20   ))
      case _       => println("Position not found ")
    }
    0
  }

  // - Creds -----------------
  // get creds from env-variable "DB_CREDS" in the form "username/password"
  def getDBCreds(ev:String="DB_CREDS"):(String,String) = {
    val cs = System.getenv.toMap.getOrElse( ev, "")
    val i = cs.indexOf("/")
    val (u,p) = if( i > 0 ) ( cs.substring( 0, i), cs.substring(i+1)) else ("","")
    (u,p)
  }

  def getDb( uri:String, es:String="DB_CREDS" ) = {
    val (user,pass) = getDBCreds( es )
    new dbUtils( uri, user, pass )
  }

  // Some commonly used connectors.
  // username/password should be supplied in the environment variable DB_CREDS
  val DB4     = "jdbc:sqlserver://0.0.0.0;database=vendor;schema=db"
  val RSXLive = "jdbc:sqlserver://r-db.raj.com"
  val Redshift= "jdbc:postgresql://cluster.a.us-east-1.redshift.amazonaws.com:5439/prod"

  //- Testing -----------
  def db4test() =
  getDb( DB4 ).execm( "exec GetData").take( 5 ).foreach( println )

  def rsxlivetest() = {
    val q = "select segment from " +
    "leadgeneration.dbo.clientpathconfigurationfiles "+
    "with(nolock) where configType = 'GlobalAlgo'"
    getDb( RSXLive ).execm( q ).take( 1 ).foreach( println);
  }

  def redtest() = {
    val db = getDb( Redshift, "REDSHIFT_CREDS" )

    val q = "select count(*) from agg.algo_info"
    println( s"execing $q" ); db.execute( q ).foreach( println )
  }

  def main(args: Array[String]) = {
    val ttype = if( args.size > 0 ) args(0) else "redshift"
    ttype match {
      case "redshift" => redtest()
      case "db4"      => db4test()
      case "rsx"      => rsxlivetest()
      case _          => redtest()
    }
  }
}
