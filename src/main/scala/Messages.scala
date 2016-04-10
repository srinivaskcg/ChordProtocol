
/**
 * @author SRINIVAS
 */

object Messages {
  
  sealed trait ChordMessage
  case class join(randNode: Int) extends ChordMessage
  case class getSuccessor(isNewNode: Boolean) extends ChordMessage
  case class setPredecessor(name: Int, isNewNode: Boolean) extends ChordMessage
  case class setSuccessor(name: Int, isNewNode: Boolean) extends ChordMessage
  case class findPredecessor(key: Int, origin: Int, reqType: String, data: String) extends ChordMessage
  case class findSuccessor(key: Int, origin: Int, reqType: String, i: Int) extends ChordMessage
  case class fingerData(hashKey: Int, node: Int, reqType: String, i: Int) extends ChordMessage
  case class updateOthers() extends ChordMessage
  case class searchKey(numRequests: Int) extends ChordMessage
  case class findKey(keyHash: Int) extends ChordMessage
}