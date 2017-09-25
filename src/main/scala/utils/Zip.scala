package rajkumar.org.utils

import java.io._
import java.util.zip._
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils

// Tools to zip/unzip data (can be gzip compatible)

object Zip {

  val Level = 5        // can be 0-9

  def compress( str: String): Array[Byte] = {
    val bytes = str.getBytes("UTF-8")
    val compressor = new Deflater( Level, true )
    compressor.setInput( bytes )
    compressor.finish
    val output = new Array[Byte]( str.size * 2 )
    val l =  compressor.deflate( output )
    output.take( l )
  }

  // gzip compatible inflation (norap Inflator) requires a dummy byte at end
  def decompress( ba: Array[Byte] ): String = {
    val xba = ba ++ Array[Byte](0)
    val decompressor = new Inflater( true )
    decompressor.setInput( xba, 0, xba.size )
    val res = new Array[Byte]( 1000 )
    val l = decompressor.inflate( res )
    decompressor.end
    new String( res, 0, l, "UTF-8")
  }

  // - gz codecs --
  def gzCompress( s:String):Array[Byte] = {
    val o = new java.io.ByteArrayOutputStream()
    val gos = new GZIPOutputStream( o )
    gos.write( s.getBytes("UTF-8")); gos.close
    o.toByteArray;
  }
  def gzUncompress( bs:Array[Byte]):String = {
    val gis = new GZIPInputStream( new java.io.ByteArrayInputStream(bs))
    scala.io.Source.fromInputStream( gis ).getLines.mkString("")
  }
  def isGz( bs:Array[Byte]):Boolean =
  bs(0) == GZIPInputStream.GZIP_MAGIC.toByte &&       // 0x1f
  bs(1) == (GZIPInputStream.GZIP_MAGIC >> 8).toByte   // 0x8b

  // - Testing ------
  def test() = {
    val tes = "foo bar goo hoo"
    println( decompress( compress( tes )) )

    val ts = """OYO6Q9Q6%2FkISJuVpiD3PIh7ol2JLwcdup4cjh4cGMJ6Ec%2BmAI86ZHQcAICQ%2BJ%2Fs3LQIKPL9YrARjCulebrt2kDcp472l1vE5khYhMJgHEoRG8jXxx3bQ1%2FfqwUczLDzV8myxpEPJabkGfpAZgBrkQb3ZrJCPx%2Fm4opiuQ3cSneQOzxBjtCXRfbvjSSXrJCOOQVAlC%2FQXeslRBUjawFockQDEoQkiBbM7l%2BzPD4mcPUlf0hTqMiIJBrRE%2Bp7c3mue7KLQvzitynQRofOX4H8g48qVHCWhlqqJpCsFhnGkS48rcer4zDTa8n%2B2U6aeU76zjWz%2BVVIYvFaNz4vkHGs13uAexARSPGMtQL9mQNgZaC2oN2gCS7DMfwymX5H5jrXE3Mgwmw3DF3zW1mIPHqKASVza79icAeUviVnYUtPs9R%2BkYRnUVZbAMfeUieNQfQSQ3l%2FyZL3jl48gsrf5vdkShhYXVSCmHrmRxTT%2BL0lbz9Cg%2B%2Bfhs5slQiC3vIZBlmFA2m0pqvuCxAdXCMeFjt217b7WD9Nhx3pzBtP19e8BuMo%3D"""

    val dts   = Codec.urldecode( ts )
    val dects = Codec.decrypt( dts )
    val dec   = Codec.decode( dects )
    val unc   = decompress( dec )
    println( unc )

  }
  def main( args: Array[String]):Unit = test()
}

