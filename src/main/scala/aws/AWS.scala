package rajkumar.org.aws

import com.amazonaws.regions._
import com.amazonaws.auth._
import com.amazonaws._
import com.amazonaws.regions.Regions

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import javax.xml.bind.DatatypeConverter

import scala.util.Properties
import collection.JavaConversions._

object AWS {

  val region = if(Regions.getCurrentRegion != null)
    Regions.getCurrentRegion.getName
  else ""

  // Credentials provider (from environment)
  val credsp = new DefaultAWSCredentialsProviderChain()

  // SMTP hosts
  val SmtpHostEast    = "email-smtp.us-east-1.amazonaws.com"
  val SmtpHostWest    = "email-smtp.us-west-2.amazonaws.com"
  val SmtpHostEurope  = "email-smtp.eu-west-1.amazonaws.com"


  // ----------------------------------------------------------------
  // -- Utility Functions --
  // get the aws id and key from the environment variables ("" if not found)
  def getIdKeyFromEnv():(String,String) = {
    val id  = Properties.envOrElse( "AWS_ACCESS_KEY_ID",  "")
    val key = Properties.envOrElse( "AWS_SECRET_ACCESS_KEY", "")
    (id,key)
  }

  // Create an aws-password from AWS_SECRET_ACCESS_KEY and message
  def passwordFromKey( key:String, message:String ):String = {
    val Version:Byte =  0x02

    // Create an HMAC-SHA256 secretkey from the raw bytes of the key
    // Get an HMAC-SHA256 Mac instance initialized with the AWS secret access key
    val secretKey = new SecretKeySpec( key.getBytes(), "HmacSHA256")
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(secretKey)

    // Compute the HMAC signature on the input-message bytes
    val rawSignature = mac.doFinal( message.getBytes() )

    // Prepend the version number to the signature
    val rawSigWithVersion = new Array[Byte](rawSignature.length + 1)
    val versionArray = Array( Version )
    System.arraycopy(versionArray, 0, rawSigWithVersion, 0, 1)
    System.arraycopy(rawSignature, 0, rawSigWithVersion, 1, rawSignature.length)

    // convert the HMAC signature to base 64
    DatatypeConverter.printBase64Binary( rawSigWithVersion )
  }

}
