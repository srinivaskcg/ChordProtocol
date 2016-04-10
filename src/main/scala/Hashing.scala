import java.security.MessageDigest
import java.lang.Long

/**
 * @author SRINIVAS
 */

object Hashing {
  
  var HASH_TYPE = "SHA-1"
  
  def getHash(id: String, chordSpace: Int): Int = {

    if (id != null) {
      var key = MessageDigest.getInstance(HASH_TYPE).digest(id.getBytes("UTF-8")).map("%02X" format _).mkString.trim()
      if (key.length() > 15) {
        key = key.substring(key.length() - 15);
      }
      (Long.parseLong(key, 16) % chordSpace).toInt
    } else
      0
  }
}