package rajkumar.org.text

import scala.collection.immutable.BitSet

// Tools that work with BitSet representations of text
object bitTools {

  // represent a document as a BitSet
  // tokenizes and hashes tokens. The hask becomes the index in the bitset
  def docBits( doc:String ):BitSet =
    BitSet() ++ textUtils.tokenize( doc ).distinct.map( hash( _ ))

  // - Jaquard distance between two BitSets (intersection / union) -------
  def distance( av:BitSet, bv:BitSet):Float =
    ((av & bv).size * 1.0 / (av ++ bv ).size * 1.0).toFloat

  def distance( doca:String, docb:String):Float =
    distance( docBits( doca), docBits( docb ))

  // - Privates ---
  // Hash function (Uses absolute and max-value)
  val BitSetSize = 65536;
  private def hash( word:String ):Int =
    Math.abs( word.toLowerCase.hashCode % BitSetSize )
}
