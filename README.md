# ChordProtocol

Implementation of Chord Protocol using Scala and Akka

The Chord protocol has been implemented such that the program can be started by giving an input specifying the number of nodes that are to be configured in the network and the number of requests that each node needs to make. The program achieves termination when all the nodes configured in the network perform the number of requests specified. Every node is implemented such that it transmits 1 request/second.

Input: project3.scala numberOfNodes numberOfRequests

Output: Average number of hops traversed to deliver the message.

How to run:
• Copy the folder Chord onto the System.
• Access the folder Chord using the command prompt.
• In the command prompt run the sbt command. Run the command "run numNodes numRequests" where numNodes is the number of nodes configured in the network and numRequests is the number of requests each node has to make.

Output:
Number of Nodes Number of Requests Total number of Hops Average number of Hops
10 1 34 3.400
10 3 99 3.333
10 10 313 3.130
100 3 1565 5.217
500 3 9672 6.448
1000 3 20698 6.899
2500 3 56664 7.552
Largest network simulated: 2500 nodes
Observations:
 With increase in number of total nodes introduced in network, Average number of Hops were increased.
 With increase in number of requests made by each node for the same network, Average number of Hops were decreased.
