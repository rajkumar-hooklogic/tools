package rajkumar.org.aws

import rajkumar.org.io._

import com.amazonaws.auth._
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.utils._
import com.amazonaws.services.dynamodbv2.document.spec._
import com.amazonaws.services.dynamodbv2.model._

import scala.util._
import scala.collection.JavaConversions._

// Notes: Credentials reqquired via environment-variables or credentials file
//        Key can be primary-key or primary-key + order-key

object Dynamo extends Store {

  val dc    = new AmazonDynamoDBClient( AWS.credsp )
  val adc   = new AmazonDynamoDBAsyncClient( AWS.credsp )
  val ddb   = new DynamoDB( dc )
  val addb  = new DynamoDB( adc )

  // - Primary Key only -
  // From table
  def get( tn:String, pk:String, pv:Any ):String =
    get( ddb.getTable( tn ), pk, pv )
  def get( t:Table, pk:String, pv:Any ):String =
    t.getItem( new KeyAttribute( pk, pv )).toString


  // get all attributes as Json
  def query( tn:String, pk:String, pv:Any ):Iterator[String] =
    iquery( tn, pk, pv).map( i => i.toJSON )

  // get string attribute sak only
  def query( tn:String, pk:String, pv:Any, sak:String ):Iterator[String] =
    iquery( tn, pk, pv).map( i => i.getString( sak ) )

  // From index
  def iquery( tn:String, in:String, pk:String, pv:Any ):
    Iterator[Map[String,String]] = {
    val i   = ddb.getTable( tn ).getIndex( in )
    val ka  = new KeyAttribute( pk, pv )
    val qm = i.query( ka ).iterator().toIterator.map( _.asMap )
    qm.map( m => m.toMap.map{ case(k,a) => (k,a.toString) })
  }

  // scan attribute sak only
  val eav = new AttributeValue()
  def scan( tn:String, sak:String ):Iterator[ String ] = {
    val sr = new ScanRequest().withTableName( tn ).
      withAttributesToGet( sak )
    val res = Dynamo.dc.scan( sr )
    val im = res.getItems().iterator().toIterator.map( _.toMap )
    im.map( m => m.getOrElse( sak, eav ).getS )
  }
  def scan( tn:String, as:Seq[String]):Iterator[ Map[String,String] ] = {
    val res = Dynamo.dc.scan( tn, as )
    val sam = res.getItems().iterator().toIterator.map( _.toMap )
    sam.map( m => m.map{ case(k,a) => (k,a.getS) } )
  }


  // - Primary Key + Secondary Key -
  def get( tn:String, pk:String, pv:Any, sk:String, sv:Any ):String =
    get( ddb.getTable( tn ), pk, pv, sk, sv )
  def get( t:Table, pk:String, pv:Any, sk:String, sv:Any ):String = {
    val res = t.getItem( new KeyAttribute(pk,pv), new KeyAttribute(sk,sv) )
    if( res != null ) res.toString else ""
  }
  def getm(tn:String, pk:String, pv:Any,sk:String, sv:Any):Map[String,String] =
    iteMap( ddb.getTable( tn ).getItem( new KeyAttribute(pk,pv),
      new KeyAttribute(sk,sv) ) )


  // Queries need to specigy primary key, and can specify conditions on the
  // Range-Key (or indices) by creating a RangeKeyCondition
  // val rkk = new RangeKeyCondition( sk )
  // rkk.between(a,b) || beginsWith(a) || eq(a) || gt(a) || lt || ge || le
  def rkquery( tn:String, pk:String, pv:Any, rkc:RangeKeyCondition ):
    Iterator[Item] =
    ddb.getTable( tn ).query( new KeyAttribute( pk, pv ), rkc).iterator

  // - Privates ----
  private def iquery( tn:String, pk:String, pv:Any ):Iterator[Item] =
    ddb.getTable( tn ).query( new KeyAttribute( pk, pv )).iterator

  def scan( tn:String ):Iterator[ Map[String,AttributeValue]] = {
    val sr = new ScanRequest().withTableName( tn )
    val res = Dynamo.dc.scan( sr )
    res.getItems().iterator().toIterator.map( _.toMap )
  }
  // Convert Item to Map
  private def iteMap( i:Item ):Map[String, String] = {
    if( i != null ) i.asMap.toMap.map{ case( k,a) => ( k, a.toString )}
    else Map[String,String]()
  }


  // - trait Store implementation ----------------
  def connect( t:String):Unit = {}
  def disconnect():Unit       = {}
  def isConnected():Boolean   = { Try(dc.listTables()).isSuccess }

  def getKSStore( n:String = "Test" ):KSStore = new SDynamo( n )
  def getKOStore( n:String = "Test" ):KOStore = new SDynamo( n )

  // - Testing ----------------------------------------------------------
  def ctest() = {
    val (t,pk,sk)   = ("Campaign", "id", "Key" )
    println( get(t, pk, 15886, sk, "119-31-00002708446271" ) )
    scan( t ).take( 10 ).foreach( println )
    val rkk = new RangeKeyCondition( sk ).beginsWith( "119" )
    rkquery(  t, pk, 15886, rkk ).take( 10 ).foreach( println )
  }
  def stest() = {
    val cols = Seq("Key", "id", "cid", "thumbnail")
    scan( "Skus", cols  ).take( 10 ).foreach( println )
  }
  def itest(skuid:String = "6286690") =
    iquery( "keys", "id-gc", "id", skuid ).take( 10 ).foreach( println )


    def main( args: Array[String]):Unit =
      if( args.size > 0 ) itest( args(0) ) else itest()
}
