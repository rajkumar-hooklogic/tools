package rajkumar.org.utils

object Strings {

  // trim character c off string s
  def trim( s:String, c:Char ):String =
    s.dropWhile( _ == c ).reverse.dropWhile( _ == c ).reverse

  // get the value of field f from a key:value string
  // where the kv-pairs are delimited by fdel and key from value by kvdel
  // "...,"foo":"bar",hue:moo,...", field(e,"foo") returns "bar"
  val FDel  = ","
  val KVDel = ":"
  val CDel  = "}"
  def field( e: String, f:String, fdel:String=FDel, kvdel:String=KVDel ):String = {
    val findex  = e.indexOf( f )
    if( findex >= 0 ) {
      val sindex = e.indexOf(KVDel, findex) + 1

      val aindex = e.indexOf(FDel,  sindex )
      val dindex = if( aindex > sindex ) aindex else e.size
      val bindex = e.indexOf(CDel,  sindex )
      val cindex = if( bindex > sindex ) bindex else e.size
      val eindex = dindex.min( cindex )
      e.substring( sindex, eindex )
    }
    else ""
  }
  // trim "s off result
  def qfield( e: String, f:String ):String = trim( field( e,f ), '"' )

  // generate guid
  def guid() = java.util.UUID.randomUUID().toString
}
