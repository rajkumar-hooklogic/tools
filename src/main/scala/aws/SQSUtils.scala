package rajkumar.org.aws

import com.amazonaws._
import com.amazonaws.auth._
import com.amazonaws.regions.{Region,Regions}

import com.amazonaws.services.sqs.model._
import com.amazonaws.services.sqs.AmazonSQSClient

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read

import scala.util._
import collection.JavaConversions._

case class Bucket ( name:String )
case class Object ( key:String,     size:Int )
case class S3     ( bucket:Bucket,  `object`:Object )
case class Rec    ( s3:S3 )
case class Recs   ( Records:Seq[Rec] )
case class MBody  ( Message:String )

object SQSUtils {

  implicit val formats = Serialization.formats( NoTypeHints )

  // - SQS api ----------
  val sqs = new AmazonSQSClient( AWS.credsp )
  sqs.setRegion( Region.getRegion(Regions.US_EAST_1) )

  def sqsdelete( q:String, h: String ):Unit =
    Try( sqs.deleteMessage( new DeleteMessageRequest( q, h )) )
  // seq of message-body, handle
  def sqsmessages(q:String):Seq[(String,String)] =
    Try( sqs.receiveMessage( q ).getMessages.map( m =>
      (m.getBody, m.getReceiptHandle ))).getOrElse( Seq())

  // delete message and return body
  def sqsprocess(q:String):Seq[String] = {
    for {
      (b,h) <- sqsmessages( q )
      if( h.size > 0 )
      _ = sqsdelete( q,h )
    } yield b
  }

  val BPQ   = "https://sqs.us-east-1.amazonaws.com/Stats"
  val EastQ = "https://sqs.us-east-1.amazonaws.com//TestQueue"
  val WestQ = "https://sqs.us-west-2.amazonaws.com//Userc_West"
  val Queue = if( rajkumar.org.aws.AWS.region.contains("west")) WestQ else EastQ
  val TBRQ  = "https://sqs.us-east-1.amazonaws.com/FileReady"


  // Parse SNS/SQS message and get s3 bucket and prefixes
  def parseSnsFileReady( s:String ):Seq[(String,String)] = {
    val rstr  = read[MBody]( s ).Message
    val rs    = read[Recs]( rstr ).Records
    rs.map( r => (r.s3.bucket.name, r.s3.`object`.key) )
  }

  // Parse SQS message and get s3 bucket and prefixes
  def parseS3FileReady( s:String ):Seq[(String,String)] = {
    val rs    = read[Recs]( s ).Records
    rs.map( r => (r.s3.bucket.name, r.s3.`object`.key) )
  }

  def snsFileReady( q:String ):Seq[(String,String)] =
    sqsprocess( q ).map( m => parseSnsFileReady( m )).flatten

  def s3FileReady( q:String ):Seq[(String,String)] =
    sqsprocess( q ).map( m => parseS3FileReady( m )).flatten

  // - Testing  ------------------------------
  import rajkumar.org.utils.Info
  def s3test() = s3FileReady( TBRQ ).foreach{ case(b,p)  => println(s"$b:$p") }
  def sqstest() = {
    val N = 10000
    var cnt = 0
    println(s"Using queue: $Queue")
    for( i <- 1 to N ) {
      snsFileReady(  Queue ).foreach{ case(b,p)  =>
        Info.inc("test","sqsutils",1.0)
        cnt = cnt + 1  }
    }
    println(s"$N SQS reads: File count: $cnt" )
    println( Info.currentm() )
    println( Info.totalsm() )
  }
  def main( args:Array[String]):Unit = sqstest()

}
