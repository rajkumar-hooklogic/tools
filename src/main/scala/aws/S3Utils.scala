package rajkumar.org.aws

import com.amazonaws.auth._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._

import java.io._
import java.net.URI

import scala.collection.JavaConversions._
import scala.reflect._
import scala.util._

object S3Utils {

  // Note: Credentials need to be provide in environment-variables
  // AWS_ACCESS_KEY_ID
  // AWS_SECRET_ACCESS_KEY
  // or credentials file

  val s3c     = new AmazonS3Client( AWS.credsp )

  // - Reads -----------------
  def get( b: String, p:String ) : InputStream =
    s3c.getObject( b, p ).getObjectContent

  // Get lines in s3 uri. Is Seq (cannot be Iterator)
  def getLines( b: String, p:String ):Seq[String] = {
    val o = s3c.getObject( b,p )
    val ois = o.getObjectContent
    val is  = if( p.endsWith(".gz")) new java.util.zip.GZIPInputStream( ois )
    else ois
    val ls = scala.io.Source.fromInputStream( is, "utf-8" ).getLines.toSeq
    val s = ls.size
    o.close
    ls
  }

  def exists( b:String, p:String ):Boolean = list( b, p ).size >= 1

  // Note: First call only returns 1000. We need to check for additional prefixes
  def list( b: String, p:String):Seq[String] = {
    val ol = s3c.listObjects( new ListObjectsRequest().
      withBucketName( b ).withPrefix( p ))
    okeys( ol ) ++ akeys( ol )
  }

  // - Writes ---------
  // Warning: length is num_bytes *not* string.length
  def put( b: String, p: String, is: InputStream, length:Long ):Unit = {
    val md = new ObjectMetadata()
    md.setContentLength( length )
    s3c.putObject(new PutObjectRequest(b, p, is, md ))
  }

  def put( b:String, p:String, bytes: Array[Byte]):Unit =
    put( b,p, new ByteArrayInputStream( bytes ), bytes.length )

  // - Privates ----
  private def akeys( o: ObjectListing ):Seq[String] = {
    var ol = o
    Iterator.continually({ol = s3c.listNextBatchOfObjects( ol ); okeys(ol)})
      .takeWhile( _.size > 0 ).flatten.toSeq
  }
  private def okeys( ol: ObjectListing ):Seq[String] =
    ol.getObjectSummaries.map( _.getKey )


  def test() = {
    case class ST( searchterms_raw: String, searches: Int, cid:Int, culture:String )
    val est = ST( "", 0, 0, "")
    val m = collection.mutable.Map[Int,Int]()
    val b = "bucket"
    val p = "user/hive/warehouse/some.db/foobar/dt=2017-06-27"
    for( pre <- list( b, p ))
      for( l <- getLines( b, pre )){
        val e = rajkumar.org.utils.Json.fromJS[ ST ]( l ).getOrElse( est )
        m( e.cid ) = m.getOrElse( e.cid, 0 ) + 1
      }
    println( m.toSeq.sortBy( _._1 ))
  }
  def test2() = {
    case class ST( searchterms_raw: String, skuid: Int, cid:Int, culture:String )
    val est = ST( "", 0, 0, "")
    val m = collection.mutable.Map[Int,Int]()
    val b = "bucket"
    val p = "user/hive/warehouse/some.db/json/dt=2017-06-27"
    for( pre <- list( b, p ))
      for( l <- getLines( b, pre )){
        val e = rajkumar.org.utils.Json.fromJS[ ST ]( l ).getOrElse( est )
        m( e.cid ) = m.getOrElse( e.cid, 0 ) + 1
      }
    println( m.toSeq.sortBy( _._1 ))
  }
  def main( args: Array[String]):Unit = test2()

}
