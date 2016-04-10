import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.concurrent.impl.Future
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.collection.convert.decorateAsScala._
import scala.collection._

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Props
import akka.event.Logging

import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.ConfigFactory
import com.sun.xml.internal.fastinfoset.tools.PrintTable

/**
 * @author SRINIVAS
 */

class ChordNode(val hashName: Int, val abstractName: String, val requests: Int) extends Actor {

  var finger = new HashMap[Int, Int]
  var successor: Int = -1;
  var predecessor: Int = -1;
  var isJoined: Boolean = false;
  var timeStamp = System.currentTimeMillis()
  var searchForPredecs = new ArrayBuffer[Int]
  
  val request = "fingerRequest"
  val setRequest = "setRequest"
  val fingerUpdate = "fingerUpdate"
  val m: Int = (math.ceil((math.log10(Integer.MAX_VALUE) / math.log10(2)))).toInt - 1
  val chordSpace: Int = Math.pow(2, m).toInt
  val prefix = "akka://" + "system" + "/user/"

  def getFingerId(index: Int): Int = {
    (hashName + Math.pow(2, index).toInt) % ChordSimulator.chordSpace
  }

  def receive = {
    
    case Messages.searchKey(numRequests: Int) => {
      val scheduler = context.system.scheduler

      for (i <- 1 to numRequests) {
        var keyName = ChordSimulator.namingPrefix + hashName + i;
        var KeyHash = Hashing.getHash(keyName, chordSpace)
        scheduler.scheduleOnce(Duration(i, TimeUnit.SECONDS), self, Messages.findKey(KeyHash))
      }
    }

    case Messages.findKey(keyHash: Int) => {
      ChordSimulator.TotalHops = ChordSimulator.TotalHops + 1;
      var keyHashName = keyHash
      var found: Boolean = false;
      var maxPredecessor: Int = -1;

      var isForwarded: Boolean = false
      if (checkSuccessor(keyHashName)) {
        foundKey(keyHashName, hashName)
        found = true;
      }

      if (!found && checkPredecessor(keyHashName)) {
        var tempActor = context.actorSelection(prefix + successor)
        tempActor ! Messages.findKey(keyHashName)
        isForwarded = true
      }

      if (!found && !isForwarded) {
        var tempActor = context.actorSelection(prefix + finger(closestPrecedingFinger(keyHashName)))
        tempActor ! Messages.findKey(keyHashName)
      }
    }

    case Messages.join(randNode: Int) => {

      if (randNode == -1) {
        successor = hashName;
        predecessor = hashName;
        fillFingerTable(-1);
        isJoined = true              
        ChordSimulator.numNodesJoined = ChordSimulator.numNodesJoined + 1;        
      } else {
        var tempActor = context.actorSelection(prefix + randNode)
        tempActor ! Messages.findPredecessor(hashName, hashName, setRequest, null)
      }
    }

    case Messages.fingerData(hashKey: Int, node: Int, reqType: String, i: Int) => {
      if (reqType.equals(request)) {
        if (i <= m) {
          var currentIndex = i + 1
          finger(i) = node
          var nextIndex: Int = getFingerId(currentIndex)
          if (currentIndex < m) {
            var successorActor = context.actorSelection(prefix + successor)
            successorActor ! Messages.findSuccessor(nextIndex, hashName, request, currentIndex)
          } else {
            self ! Messages.updateOthers()
          }
        } else {
        }
      }
    }

    case Messages.updateOthers() => {
      var preActor = context.actorSelection(prefix + predecessor)
      var updateableNodes = new HashMap[Int, Int]
      for (i <- 0 until m) {
        var temp = -1
        var pow = Math.pow(2, i).toInt;
        if (pow < hashName) {
          temp = (hashName - pow) % chordSpace
        } else {
          temp = chordSpace - Math.abs(hashName - pow)
        }
        updateableNodes.put(i, temp);
      }

      for (i <- 0 until m) {
        var t = updateableNodes(i)
        if (!checkPredecessor(t)) {
          preActor ! Messages.findPredecessor(updateableNodes(i), hashName, fingerUpdate, "" + i)
        }
      }
      isJoined = true;
      ChordSimulator.numNodesJoined = ChordSimulator.numNodesJoined + 1;
      var temp = ChordSimulator.checker.get()  
      ChordSimulator.checker.set(!temp)
    }

    case Messages.getSuccessor(isNewNode: Boolean) => {
      if (isNewNode) {
        sender ! Messages.setSuccessor(successor, false)
      }
    }

    case Messages.setSuccessor(name: Int, isNewNode: Boolean) => {
      if (isNewNode && isJoined) {
        var myOldSuccesor = context.actorSelection(prefix + successor)
        successor = name
        myOldSuccesor ! Messages.setPredecessor(name, isJoined)
      } else if (!isNewNode && !isJoined) {
        successor = name
        var myNewPredecessor = context.actorSelection(prefix + predecessor)
        myNewPredecessor ! Messages.setSuccessor(hashName, !isJoined)
        var f = Future {
          Thread.sleep(10)
        }
        f.onComplete {
          case x =>
            finger(0) = successor
            self.tell(Messages.fingerData(-1, finger(0), request, 0), self)
        }
      }
    }

    case Messages.setPredecessor(name: Int, isNewNode: Boolean) => {
      if (isJoined && isNewNode) {
        predecessor = name;
      } else if (!isJoined && ! isNewNode) {
        predecessor = name
        var act = context.actorSelection(prefix + predecessor)
        act ! Messages.getSuccessor(!isJoined)
      }
    }

    case f: Messages.findPredecessor => {
      var start = hashName
      var end = successor
      var condition1 = (start == end)
      var condition2 = (end > start && (f.key >= start && f.key < end))
      var condition3 = (end < start && ((f.key >= start && f.key < chordSpace) || (f.key >= 0 && f.key < end)))

      if (f.reqType.equals(setRequest)) {
        if (condition1 || condition2 || condition3) {
          var act = context.actorSelection(prefix + f.origin)
          act ! Messages.setPredecessor(start, false)
        } else {
          var nearestNbr = closestPrecedingFinger(f.key);
          if (finger(nearestNbr) == hashName) {
            var act = context.actorSelection(prefix + f.origin)
            act ! Messages.setPredecessor(start, false)
          } else {
            var act = context.actorSelection(prefix + finger(nearestNbr))
            act ! f
          }
        }
      } else if (f.reqType.equals(fingerUpdate)) {
        if (condition1 || condition2 || condition3) {
          var c1 = (finger(f.data.toInt) > f.origin)
          var c2 = ((finger(f.data.toInt) < f.origin) && (finger(f.data.toInt) <= hashName))
          if (c1 || c2) {
            finger(f.data.toInt) = f.origin
          }
        } else {
          var act = null
          if (checkSuccessor(f.key)) {
            var act = context.actorSelection(prefix + predecessor)
            act ! f
          } else {
            var closetNeigh = closestPrecedingFinger(f.key);
            var act = context.actorSelection(prefix + finger(closetNeigh))
            act ! f
          }
        }
      }
    }

    case Messages.findSuccessor(key: Int, origin: Int, reqType: String, i: Int) => {
      if (reqType.equals(request)) {

        var start = hashName
        var end = successor

        var condition1 = (start == end)
        var condition2 = (end > start && (key >= start && key < end))
        var condition3 = (end < start && ((key >= start && key < chordSpace) || (key >= 0 && key < end)))

        var condition4 = false
        if (predecessor < hashName)
          condition4 = (predecessor < key && key < hashName)
        else {
          var c1 = (predecessor < key && key < chordSpace)
          var c2 = (0 < key && key < hashName)
          condition4 = (c1 && !c2) || (!c1 && c2)
        }

        if (condition1 || condition2 || condition3) {
          var tmpActor = context.actorSelection(prefix + origin)
          tmpActor ! Messages.fingerData(key, end, reqType, i)
        } else if (condition4) {
          var tmpActor = context.actorSelection(prefix + origin)
          tmpActor ! Messages.fingerData(key, hashName, reqType, i)
        } else {
          var nearestNeighbour = closestPrecedingFinger(key);
          if (finger(nearestNeighbour) == hashName) {
            var tmpActor = context.actorSelection(prefix + origin)
            tmpActor ! Messages.fingerData(key, finger(nearestNeighbour), reqType, i)
          } else {
            var tmpActor = context.actorSelection(prefix + finger(nearestNeighbour))
            tmpActor ! Messages.findSuccessor(key, origin, reqType, i)
          }
        }
      }
    }

    case x: String => {
      if (x.equals("print")) {
        println("NodeId: " + hashName + " ## Predecessor: " + predecessor + " ## Successor: " + successor)
        println("--------------------- Finger Table -------------------------")
        for(cntr <- 0 to finger.size-1){
           println("Finger: " + cntr + " -- Successor:" + finger(cntr))
        }
        println("------------------------------------------------------------")
      } else if (x.equals("printTable")) {
        printTable()
      }
    }
  }

  def foundKey(fileName: Int, foundAt: Int): Unit = {
    ChordSimulator.incrementCounter()
    ChordSimulator.searchedKeys(fileName) = foundAt
  }

  def printTable(): Unit = {
    var x = " "
    for (i <- 0 to finger.size - 1) {
      x = x + i + "-" + getFingerId(i) + "-" + finger(i) + " , "
    }
  }

  def fillFingerTable(randNode: Int): Unit = {
    if (randNode == -1) {
      for (i <- 0 until m) {
        finger(i) = hashName
      }
    }
  }

  def checkSuccessor(key: Int): Boolean = {

    var checkSucc = false
    if (predecessor < hashName) {
      checkSucc = (predecessor <= key) && (key < hashName)
    } else {
      var condition1 = (predecessor <= key) && (key < chordSpace)
      var condition2 = (0 < key) && (key < hashName)
      checkSucc = (condition1 && !condition2) || (!condition1 && condition2)
    }
    checkSucc
  }

  def checkPredecessor(key: Int): Boolean = {

    var checkPred = false
    if (successor > hashName) {
      checkPred = (key <= successor) && (key > hashName)
    } else {
      var condition1 = (hashName < key) && (key < chordSpace)
      var condition2 = (0 < key) && (key < successor)
      checkPred = (condition1 && !condition2) || (!condition1 && condition2)
    }
    checkPred
  }

  def closestPrecedingFinger(key: Int): Int = {
    var keyFound = Integer.MIN_VALUE
    var distantNbr = Integer.MIN_VALUE
    var current = Integer.MIN_VALUE;

    var lowerNbr = Integer.MAX_VALUE
    var higherNbr = Integer.MAX_VALUE
    var positiveValFound = false

    for (i <- 0 until finger.size) {

      var diff = key - finger(i)
      if (0 < diff && diff < higherNbr) {
        keyFound = i;
        higherNbr = diff
        positiveValFound = true
      } else if (diff < 0 && diff < lowerNbr && !positiveValFound) {
        keyFound = i;
        lowerNbr = diff
      }
    }
    printTable()
    keyFound
  }
}
