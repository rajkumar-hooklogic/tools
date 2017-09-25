package rajkumar.org.aws

import rajkumar.org.io._
import rajkumar.org.utils.Json

import com.amazonaws.auth._
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.utils._
import com.amazonaws.services.dynamodbv2.document.spec._
import com.amazonaws.services.dynamodbv2.model._

import scala.util._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.JavaConversions._

// String Key-Value store implemented on Dynamo
//    Uses just two string-valued attributes/columns to store key and val
//    Notes: Credentials required via environment-variables or credentials file

class SDynamo( tname:String ) extends KSStore with KOStore {

  val KName = "phkey"
  val VName = "stringattr"
  val t     = Dynamo.ddb.getTable( tname )

  // - Implementation of KSStore (prepend not efficient) ----------------
  // - Sync --

  def isReady():Boolean = true

  def put( k:String, v:String ):Boolean =
    t.putItem( mkItem( k,v) ).getPutItemResult.hashCode > 0

  def put( kvs: Map[String, String] ):Int = {
    val is = mkItems( kvs )
    val twis = new TableWriteItems( tname ).withItemsToPut( is )
    Dynamo.ddb.batchWriteItem( twis )
    kvs.size
  }

  def get( k:String ):String = {
    val i = t.getItem( new KeyAttribute( KName, k ))
    if( i != null ) i.getString( VName ) else ""
  }

  override def get( ks:Seq[String] ):Map[String,String] = {
    val q = new TableKeysAndAttributes( tname ).
      addHashOnlyPrimaryKeys( KName, ks:_* ).
      withAttributeNames( KName, VName )
    val is = Dynamo.ddb.batchGetItem( q ).getTableItems.collect {
      case(s,is) if (s == tname)  => is.toSeq }.flatten
    is.map( i => kv(i) ).toMap
  }

  def prepend( k:String, v:String):Boolean = {
    val e:String = get( k )
    put( k, v + e)
  }

  def exists( k:String):Boolean =
    t.getItem( new KeyAttribute( KName, k )) != null

  def delete( k:String ):Boolean =
    t.deleteItem( mkKey( k )).getDeleteItemResult.hashCode > 0

  def keys():Iterator[String] = Dynamo.scan( tname, KName )

  // - Async -----
  // Wrap sync-calls in Futures for now. Should use async api

  def aget( k:String ):Future[String] = Future{ get(k) }

  def aput( k:String, v:String):Future[Boolean] = Future{ put(k,v) }

  def aprepend( k:String, v:String ):Future[Boolean] = Future{ prepend(k,v) }

  def adelete( k:String ):Future[Boolean] = Future { delete(k) }

  def aexists( k:String ):Future[Boolean] = Future { exists( k ) }

  def akeys():Future[Iterator[String]] = Future { keys() }



  // - Implementation of KOStore --------------------------------------
  def get[A:Manifest](  k:String ):Option[A] = {
    val i = t.getItem( new KeyAttribute( KName, k ))
    val js = if( i != null ) i.getString( VName ) else ""
    Json.fromJS[A]( js )
  }
  def put[  A <: AnyRef:Manifest](  k:String, v:A ):Boolean = {
    val js = Json.toJS[A]( v )
    t.putItem( mkItem( k,js) ).getPutItemResult.hashCode > 0
  }

  // - Privates ------------------------------------------------
  private def mkItems( kvs: Map[String,String] ):Seq[Item] =
    kvs.map{ case(k,v) => mkItem( k, v) }.toSeq
  private def mkItem( k:String, v:String ):Item =
  (new Item()).withString( KName, k).withString( VName,v )
  private def mkItem( k:String ):Item =
  (new Item()).withString( KName, k)
  private def kv( i:Item ):(String,String) =
  (i.getString( KName ), i.getString( VName ))
  private def mkKey( k:String ):PrimaryKey = new PrimaryKey( KName, k )

}

object SDynamo {

  // - Testing ----------------------------------------------------------
  def test() = {
    val tname = "Test"
    val kvs = new SDynamo( tname )
    kvs.put( "key1", "value1")
    kvs.put( "key2", "value2")
    kvs.put( "key3", "value3")

    val res:String = kvs.get( "key1" )
    println(  res )
    println( kvs.get( Seq( "key1", "key2") ))

    kvs.prepend("key2", "foo bar " )
    kvs.keys.foreach( println )
  }

  def main( args: Array[String]):Unit = test()
}
