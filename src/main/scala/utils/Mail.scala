package rajkumar.org.utils

import java.util.Properties
import javax.mail._
import javax.mail.internet._

// Email helpers
object Mail {

  val Port = 25

  // send mail using an smtp-host (with name/password creds)
  // Note: If using an AWS smtp-server, the from email needs to be registered
  def sendMail( to:String, from:String, subject:String, body:String,
    smtpHost:String, name:String, password:String ):Unit = {

    val props = new Properties()
    props.put("mail.transport.protocol",      "smtps")
    props.put("mail.smtp.port",               Port.toString )
    props.put("mail.smtp.auth",               "true")
    props.put("mail.smtp.starttls.enable",    "true")
    props.put("mail.smtp.starttls.required",  "true")

    val session = Session.getDefaultInstance( props )
    val msg = new MimeMessage( session )
    msg.setFrom(      new InternetAddress( from ))
    msg.setRecipient( Message.RecipientType.TO, new InternetAddress( to ))
    msg.setSubject(   subject )
    msg.setContent(   body, "text/plain" )

    val transport = session.getTransport()
    transport.connect( smtpHost, name, password)

    transport.sendMessage( msg, msg.getAllRecipients )
  }

  // --------
  def awsMailTest() = {

   val from =     "rak.kumar@cs.columbie.edu"
   val to   =     "raj.kumar@gmail.com"
   val body =     "Test sent through Amazon SES SMTP"
   val subject =  "AWS Mail Test"

   val (id,key) = rajkumar.org.aws.AWS.getIdKeyFromEnv()
   val pass     = rajkumar.org.aws.AWS.passwordFromKey( key, "SendRawEmail" )

   sendMail( to, from, subject, body, rajkumar.org.aws.AWS.SmtpHostEast, id, pass )
  }

  def main( args:Array[String] ):Unit = awsMailTest()

}

