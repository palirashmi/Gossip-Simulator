/*
 * imports
 */
import akka.actor.{ActorSystem, ActorLogging, Actor, Props, ActorRef}
import scala.collection.mutable.Set
import scala.collection.mutable.Map
import scala.util.Random;
import scala.concurrent.duration._
import akka.util.Timeout
import akka.pattern.ask
import scala.language.postfixOps
import akka.dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.math._
import util.control.Breaks._
import scala.collection.mutable.ArrayBuffer


/*
 * message templates
 */
case class add(node: ActorRef)
case class remove(node: ActorRef)
case class gossip(msg:String)
case class checkMessage(msg:String)
case class convergence(convergedNodeCount:Int)
case class messageReceived(flag:Boolean)
case class endAlgorithm(convergedNodeCount:Int, currentTime : Long)
case class pushsum(s:Double, w:Double)
case object forwardPushsum//(s:Double, w:Double)
case object checkRatioCount
case class  addNodesFor3DTopology(neighbours : ArrayBuffer[ActorRef])

object project2_oneFile extends App {

  /*
   * Obtain user input
   */
  val numNodes=Integer.parseInt(args(0))
  val topology= args(1)
  val algorithm=args(2)
  /*
   * Create actor system and numNodes actors
   */
  val gossipSimulator = ActorSystem("gossipSimulator")
  /*
   * Determine total no of nodes based on topology
   */
  var totalNumberOfNodes:Int=0;
  var orderOf3DTopology=math.cbrt(numNodes).toInt
  if(topology.equalsIgnoreCase("3D") || topology.equalsIgnoreCase("imp3D")){
  totalNumberOfNodes=math.pow(orderOf3DTopology,3).toInt;
  }else{
  totalNumberOfNodes=numNodes
  }
  
  val nodes = new Array[ActorRef](totalNumberOfNodes)
  /*
   * Initialize nodes
   */
  initializeNodes
  /*
   * Setup network topology
   */
  setupTopology
  /*
   * generate Listener actor which collect results from other actors and output the convergence ratio
   */
  
  val Listener=gossipSimulator.actorOf(Props[Listener], "Listener")
  gossipSimulator.scheduler.scheduleOnce(50 milliseconds) 
  {
     Listener ! convergence(0)
  } 
  /*
   * Generate and pass the gossip/push sum  to first node of the network
   */
  /*
   * start of the algorithm
   */
  println("Topology built successfully")
  var startTime=System.currentTimeMillis
  println("start Time="+startTime)
  var msg=""
  runAlgorithm
   
  
  /*
   * Network Nodes class
   */
  class NetworkNodes(val nodeNumber : Double) extends Actor  {
  
  var connectedNodes : Set[ActorRef] = Set()
  var messages:Map[String,Int] = Map()
  
  /*
   * variables for push sum algorithm
   */
  var s=nodeNumber+1
  var w=1.0
  var ratioCount=0
  var lastratio=s/w
  
  def receive = {
    /*
     * case to populate the network
     */
    case add(node) => 
        connectedNodes+=node;
        
    case remove(node)=>
      connectedNodes-=node;
      
      case addNodesFor3DTopology(neighbors)=>
      /*
       * loop over array buffer and add to set
       */
        for(neighbor <- neighbors)
          connectedNodes+=neighbor
          
    /*
     * cases for gossip algorithm
     */
    case gossip(msg)=>
       var cancellable=gossipSimulator.scheduler.scheduleOnce(50 milliseconds,self,"spreadGossip")
      if(!messages.contains(msg)){
         messages(msg)=1
         cancellable=gossipSimulator.scheduler.schedule(50 milliseconds,50 milliseconds,self,"spreadGossip")
         self ! "spreadGossip"
       }else{
         if(messages(msg)<10)
         {
          messages(msg)=(messages(msg)+1)
          self ! "spreadGossip"
          
        }else {
          //logic to remove node from other's list and add other netoworks to this
          for (tempNode <- connectedNodes) {
           tempNode ! remove(context.self);
          }
          cancellable.cancel()
        } 
        }
    case "spreadGossip" =>
        if(connectedNodes.size>0){
        var nextNode=connectedNodes.toList(Random.nextInt(connectedNodes.size))
        nextNode ! gossip(msg)
        }
    
    case checkMessage(msg)=>
      sender ! (messages.contains(msg))
    /*
     * cases for push-sum
     */
      
      case pushsum(s_received, w_received)=>

      //gossipSimulator.scheduler.scheduleOnce(50 milliseconds,self,forwardPushsum)
      var tempS=s+s_received;
      var tempW=w+w_received
      //s=tempS/2
      //w=tempW/2
      var ratio=s/w;
      
      var difference = ratio-lastratio 
      //println("ratios"+ ratio +"  "+lastratio)
     
      if((Math.abs(difference) < 0.0000000001) && ratioCount<=3)
      {
        if(ratioCount<3)
        {
          ratioCount+=1
          if(connectedNodes.size>0){
            var nextNode=connectedNodes.toList(Random.nextInt(connectedNodes.size))
            nextNode ! pushsum(s,w)
          }
          lastratio=s/w
          self ! forwardPushsum
          
        }else{
          for (tempNode <- connectedNodes) {
           tempNode ! remove(context.self);
          }
          lastratio=s/w
          ratioCount+=1
          //println("Converged!"+ratioCount)
          //cancellable.cancel()
        }
        
      }else if (ratioCount < 3){
        ratioCount=0;
        if(connectedNodes.size>0){
          var nextNode=connectedNodes.toList(Random.nextInt(connectedNodes.size))
          nextNode ! pushsum(s,w)
        }
        lastratio=s/w
       self ! forwardPushsum
      }
      
    case `forwardPushsum` =>
      if(connectedNodes.size>0 && ratioCount<3){
        var nextNode=connectedNodes.toList(Random.nextInt(connectedNodes.size))
        lastratio=s/w
        s = s/2
        w = w/2
        nextNode ! pushsum(s,w)
        //gossipSimulator.scheduler.scheduleOnce(500 milliseconds,self,forwardPushsum)
      }
      
   case `checkRatioCount` =>
      //println("inside here"+ratioCount)
      sender ! ratioCount
     
    
    case default =>
      println("wrong input")
  }
}
  /*
   * Listener class
   */
  class Listener extends Actor{
  var count=0;
  var convergedNodeCurrent=0;
  //var overloadedNodes : Set[ActorRef] = Set()
 
  implicit val timeout = Timeout(2 seconds)
  def findNoOfConvergedNodeGossip() : Int = {
    convergedNodeCurrent=0;
    for(i <- 0 until nodes.length)
       {
        
        var future = nodes(i) ? checkMessage(msg)
         
        //retreive value from future
        var result = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        if(result) 
          convergedNodeCurrent+=1
       }
    return convergedNodeCurrent
  }
  /*
   * convergence node logic 
   */
  def findNoOfConvergedNodePushSum : Int = {
    convergedNodeCurrent=0;
    for(i <- 0 until nodes.length)
       {
       // println("calling futire for "+i+"  "+nodes(i))
        var future = nodes(i) ? checkRatioCount
        
         
        var result = Await.result(future, timeout.duration).asInstanceOf[Int]
        //println("got response for "+ result )
        if(result>=3) 
          convergedNodeCurrent+=1
       }
    return convergedNodeCurrent
  }
  def receive={
    
    case convergence(convergedNodeBefore)=>
    /*
     * get no of nodes currently converged
     */
    if(algorithm.equalsIgnoreCase("gossip"))
          convergedNodeCurrent=findNoOfConvergedNodeGossip
    else
          convergedNodeCurrent=findNoOfConvergedNodePushSum
    if(convergedNodeCurrent>convergedNodeBefore)
    {
      gossipSimulator.scheduler.scheduleOnce(500 milliseconds) 
      {
        Listener ! convergence(convergedNodeCurrent)
      }
    }else
    {
      gossipSimulator.scheduler.scheduleOnce(500 milliseconds) 
      {
        Listener ! endAlgorithm(convergedNodeCurrent,System.currentTimeMillis)
      }
    }
      
    case endAlgorithm(convergedNodeBefore, time)=>
      if(algorithm.equalsIgnoreCase("gossip"))
    convergedNodeCurrent=findNoOfConvergedNodeGossip
      else
        convergedNodeCurrent=findNoOfConvergedNodePushSum
    var endTime=System.currentTimeMillis
    var timeTaken=endTime-startTime
    if(convergedNodeCurrent==convergedNodeBefore){
      println("current time "+ endTime)
      println("time taken to converge is : "+timeTaken )
      println(" converged nodes "+convergedNodeCurrent )
    }else{
      timeTaken=time-startTime
      println("time taken to converge is : "+timeTaken) 
      println(   "No of Converged Nodes : "+convergedNodeCurrent)
    }
     shutdown
     case default =>
    println("wrong message")
  }
  }
  /*
   * Function definitions
   */
  
  def initializeNodes={
    
    println("no of nodes"+ totalNumberOfNodes)
    for (i <- 0 until totalNumberOfNodes)
    {
      var name="topologyNode"+i
      nodes(i) = gossipSimulator.actorOf(Props(new NetworkNodes(i)), name)
    }
  }
  def setupTopology={
    if(topology.equalsIgnoreCase("line")){
     setupLinearTopology
     }else if(topology.equalsIgnoreCase("3D") || topology.equalsIgnoreCase("imp3D")){
      setup3Dtopology
      }else if(topology.equalsIgnoreCase("full")){
      setupFullTopology;
      }else{
      println("wrong topology")
      }
  }
  def setupLinearTopology{
    for(i <-0 until nodes.length-1){
      nodes(i) ! add(nodes(i+1))
      nodes(i+1)! add(nodes(i))
     }
  }
  def setupFullTopology{
    for(i <-0 until nodes.length){
      for(j<-0 until nodes.length)
      { 
        if(i!=j){
        nodes(i) ! add(nodes(j))
      }
     }
    }
  }
  /*
   * code i referenced from a friend and integrated in my code
   */
  def setup3Dtopology{
        var k=orderOf3DTopology
        for(i<-0 until nodes.length){
          var neighbours= new ArrayBuffer[ActorRef]();
          
          if(((i/k)%k)==0){//top surface
            //System.out.println("top for"+i);
            //bottom neighbors
              neighbours+=nodes(i+k);
            //left neighbours
            if(i%k!=0)
              neighbours+=nodes(i-1);
            //right 
            if(i%k!=(k-1))
               neighbours+=nodes(i+1);
            //front
            if((i-math.pow(k, 2))>=0)
              neighbours+=nodes(i-(math.pow(k, 2).toInt));
            //back
            if((i+math.pow(k,2))<math.pow(k, 3))
              neighbours+=nodes(i+(math.pow(k, 2).toInt))
              
          }
          else if(((i/k)%k)==(k-1)){//bottom surface
             //System.out.println("bottom for"+i);
              //up neighbors
              neighbours+=nodes(i-k);
            //left neighbours
            if(i%k!=0)
              neighbours+=nodes(i-1);
            //right 
            if(i%k!=(k-1))
               neighbours+=nodes(i+1);
            //front
            if((i-math.pow(k, 2))>=0)
              neighbours+=nodes(i-(math.pow(k, 2).toInt));
            //back
            if((i+math.pow(k,2))<math.pow(k, 3))
              neighbours+=nodes(i+(math.pow(k, 2).toInt))
          }
          else if(i%k==0){//left surface
            //System.out.println("left for"+i);
            //up neighbors
            if(((i/k)%k)!=(k-1))
              neighbours+=nodes(i-k)
            //bottom neighbors
            if(((i/k)%k)!=0)
              neighbours+=nodes(i+k);
            //right 
            if(i%k!=(k-1))
               neighbours+=nodes(i+1);
            //front
            if((i-math.pow(k, 2))>=0)
              neighbours+=nodes(i-(math.pow(k, 2).toInt));
            //back
            if((i+math.pow(k,2))<math.pow(k, 3))
              neighbours+=nodes(i+(math.pow(k, 2).toInt))
            
          }
          else if(i%k==(k-1)){//right surface
            //System.out.println("right for"+i);
              //bottom neighbors
            if(((i/k)%k)!=(k-1))
              neighbours+=nodes(i-k)
              //top
            if(((i/k)%k)!=0)
              neighbours+=nodes(i+k);
            //left neighbours
            if(i%k!=0)
              neighbours+=nodes(i-1);
            //front
            if((i-math.pow(k, 2))>=0)
              neighbours+=nodes(i-(math.pow(k, 2).toInt));
            //back
            if((i+math.pow(k,2))<math.pow(k, 3))
              neighbours+=nodes(i+(math.pow(k, 2).toInt))
            
          }
          else {
            //System.out.println("default for"+i);
            //bottom neighbour
            neighbours+=nodes(i+k);
            //top neighbour
            neighbours+=nodes(i-k);
            //right neighbour
            neighbours+=nodes(i+1);
            //left neighbour
            neighbours+=nodes(i-1);
            //back neighbour
            if(i+((math.pow(k,2).toInt))<math.pow(k,3))
              neighbours+=nodes(i+((math.pow(k,2).toInt)));
            //front neighbour
            if(i-((math.pow(k,2).toInt))>=0)
              neighbours+=nodes(i-((math.pow(k,2).toInt)));
            
          }
          /*
           * add a random node for imp3D topology
           */
          if(topology.equalsIgnoreCase("imp3D"))
            {
              var randomNode=nodes(Random.nextInt(nodes.size))
              while(neighbours.contains(randomNode)){
              randomNode=nodes(Random.nextInt(nodes.size));
            }
              neighbours+=randomNode
            }
            
          nodes(i) ! addNodesFor3DTopology(neighbours);
         } 
       }
  
  
  def runAlgorithm={
    if(algorithm.equalsIgnoreCase("gossip")){
      msg=generateGossip;
      nodes(0) ! gossip(msg)
    }else if(algorithm.equalsIgnoreCase("push-sum")){
      nodes(0)!pushsum(0.0,0.0)
    }else{
      println("wrong algorithm")
      
    }
  }
  def generateGossip : String ={
      return Random.alphanumeric.take(5).mkString;
    }
  def shutdown={
  gossipSimulator.shutdown  
  }
  
}