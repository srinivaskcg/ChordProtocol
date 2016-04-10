
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.concurrent.impl.Future
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.collection.convert.decorateAsScala._
import scala.collection._
import scala.util.Random

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

object ChordSimulator {
  
  val fingerSpace: Int = (math.ceil((math.log10(Integer.MAX_VALUE) / math.log10(2)))).toInt - 1
  val chordSpace: Int = Math.pow(2, fingerSpace).toInt
  val namingPrefix = "akka://" + "system" + "/user/"

  var numNodesJoined: Int = 0;
  var searchedKeys: concurrent.Map[Int, Int] = new ConcurrentHashMap().asScala
  var counter: AtomicInteger = new AtomicInteger()
  var checker: AtomicBoolean = new AtomicBoolean(true)
  var TotalHops = 0;

  def main(args: Array[String]): Unit = {

    var numNodes = 0;
    var numRequests = 0;
    var avgHopsinSystem = 0;
    
    if(args.length != 2){
      println(" !!! Invalid Input !!!\n "+
              "Input should be : project3.scala numNodes numRequests "+
              "(Number of Nodes and Number of Request should be > 0")
      System.exit(1)
    }else{
      numNodes = args(0).toInt
      numRequests = args(1).toInt
      println("Started building Network with " + numNodes + " nodes\n")
    }
    
    val system = ActorSystem("system")

    var chordNetwork = new ArrayBuffer[Int]()
    var startNode: Int = -1;
    var node1: ActorRef = null;
        
    for (i <- 1 to numNodes) {
      if (startNode == -1) {
        startNode = Hashing.getHash(i.toString(), chordSpace)
        node1 = system.actorOf(Props(new ChordNode(startNode, i.toString(), numRequests)), startNode.toString())
        chordNetwork += startNode
        node1 ! new Messages.join(-1)
        Thread.sleep(1000)
      } else {
        var x = checker.get()
        var hashName = Hashing.getHash(i.toString(), chordSpace)
        var node = system.actorOf(Props(new ChordNode(hashName, i.toString(), numRequests)), hashName.toString())
        chordNetwork += hashName
        //var randNode = Random.shuffle(chordNetwork.toList).head
        node ! new Messages.join(startNode)
        while (x == checker.get && numNodesJoined < numNodes) {
          Thread.sleep(1)
        }        
      }
    }
    Thread.sleep(100)
    
    for (i <- 1 to numNodes) {
      Thread.sleep(10)
      var hashName = Hashing.getHash(i.toString(), chordSpace)
      var node = system.actorSelection(namingPrefix + hashName)
      node ! "print"
      Thread.sleep(10)
    }
    
    var buffer: Int = 50
    while (ChordSimulator.numNodesJoined < numNodes - buffer) {
      Thread.sleep(100)
    }
    Thread.sleep(10000)

    for (i <- 1 to numNodes) {
      println("Lookup for Node : " + i)
      var hashName = Hashing.getHash(i.toString(), chordSpace)
      var node = system.actorSelection(namingPrefix + hashName)
      node ! new Messages.searchKey(numRequests);
    }
    var bufferPeriod: Int = (0.005 * numNodes * numRequests).toInt
    while ((counter.get() < (numNodes * numRequests) - bufferPeriod)) {
      Thread.sleep(1000)
    }

    Thread.sleep(10000)
    println("###############################################")
    println("Number of Nodes: " + numNodes)
    println("Number of Requests: " + numRequests)
    println("Keys Searched: " + counter.get())
    println("Number of hops: " + TotalHops)
    println("Avg. number of hops:" + TotalHops.toDouble / (numNodes * numRequests))
    println("Terminated")
    System.exit(0)
  }
  
  def incrementCounter(): Unit = {
    counter.incrementAndGet()
  }
}