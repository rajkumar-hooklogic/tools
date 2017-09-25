package rajkumar.org.text

// tools for text/document analysis
object textUtils {

  def alphaNumerics( doc:String):String =
    doc.toLowerCase.replaceAll("'","").replaceAll("[^a-zA-Z_0-9-]"," ")

  // return tokens from a string/doc on splitting on \s
  def tokens(doc:String):Seq[String] = doc.split("\\s+").toSeq

  // remove stopwords
  def unstop( tokens:Seq[String]):Seq[String] =
    tokens.filterNot( Constants.Stopwords.contains( _ ))

  def stem(tokens:Seq[String]):Seq[String] = tokens.map( PorterStemmer.stem( _ ))

  // clean, tokenize and stem document
  def tokenize( doc:String ):Seq[String] =
    stem( unstop( tokens( alphaNumerics( doc ))))

}
