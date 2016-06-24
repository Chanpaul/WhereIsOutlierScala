import whereIsOutLier._
import breeze.linalg._
package mtree {

class mtree[T >: Null <: AnyRef] {

  type Node = mtree.Node[T]

  private[mtree] var root = null: Node

  /** The root node in the mtree. */
  def rootNode = root

  private[mtree] var n = 0

  /** Number of nodes in the mtree. */
  def size = n

  /**
   * Removes all elements from this root.
   */
  def clear() {
    root = null
    n = 0
  }

  /**
   * Deletes a node from the heap given the reference to the node.
   * The trees in the heap will be consolidated, if necessary.
   *
   * @param  x  node to remove from heap.
   */
  def delete(x: Node) {
    val y = x.parent
    if (y != null) {
      y.cut(x, min)
      y.cascadingCut(min)
    }
    min = x
    removeMin()
  }

  def isEmpty = (root == null)

  /**
   * Inserts a new data element into the heap. No heap consolidation
   * is performed at this time, the new node is simply inserted into
   * the root list of this heap.
   *
   * @param  x    data object to insert into heap.
   * @param  key  key value associated with data object.
   * @return newly created heap node.
   */
  def insert(x: T, key: Double) = {
    val node = new Node(x, key)
    if (min != null) {
      node.right = min
      node.left = min.left
      min.left = node
      node.left.right = node
      if (key < min.key) {
        min = node
      }
    } else {
      min = node
    }
    n += 1
    node
  }

  /**
   * Removes the smallest element from the heap. This will cause
   * the trees in the heap to be consolidated, if necessary.
   *
   * @return  data object with the smallest key.
   */
  def removeMin() = {
    val z = min
    if (z == null) {
      null: T
    } else {
      if (z.child != null) {
        z.child.parent = null
        var x = z.child.right
        while (x != z.child) {
          x.parent = null
          x = x.right
        }
        val minleft = min.left
        val zchildleft = z.child.left
        min.left = zchildleft
        zchildleft.right = min
        z.child.left = minleft
        minleft.right = z.child
      }
      z.left.right = z.right
      z.right.left = z.left
      if (z == z.right) {
        min = null
      } else {
        min = z.right
        consolidate()
      }
      n -= 1
      z.data
    }
  }

  private[this] def consolidate() {
    val A = new Array[Node](45)

    var start = min
    var w = min
    do {
      var x = w
      var nextW = w.right
      var d = x.degree
      while (A(d) != null) {
        var y = A(d)
        if (x.key > y.key) {
          val tmp = y
          y = x
          x = tmp
        }
        if (y == start) {
          start = start.right
        }
        if (y == nextW) {
          nextW = nextW.right
        }
        y.link(x)
        A(d) = null
        d += 1
      }
      A(d) = x
      w = nextW
    } while (w != start)

    min = start
    for (a <- A; if a != null) {
      if (a.key < min.key) min = a
    }
  }

}

object mtree {

  /** Implements a node of the mtree. */
  class Node[T](val data: T) {   

    private[mtree] var parent = null: Node[T]
    private[mtree] var anchor = null: DenseVector[Double]
    
    private[mtree] var children = null: Array[Node[T]]
    
    private[mtree] var degree = 0
    private[mtree] var mark = false

    private[mtree] def split(min: Node[T]) {
      val z = parent
      if (z != null) {
        if (mark) {
          z.cut(this, min)
          z.cascadingCut(min)
        } else {
          mark = true
        }
      }
    }

    private[heap] def cut(x: Node[T], min: Node[T]) {
      x.left.right = x.right
      x.right.left = x.left
      degree -= 1
      if (degree == 0) {
        child = null
      } else if (child == x) {
        child = x.right
      }
      x.right = min
      x.left = min.left
      min.left = x
      x.left.right = x
      x.parent = null
      x.mark = false
    }

    private[heap] def link(prt: Node[T]) {
      left.right = right
      right.left = left
      parent = prt
      if (prt.child == null) {
        prt.child = this
        right = this
        left = this
      } else {
        left = prt.child
        right = prt.child.right
        prt.child.right = this
        right.left = this
      }
      prt.degree += 1
      mark = false
    }
  }  
  
}

}


/*
case class Point(content:Array[Double],ndId:String);
case class Entry(obj:String,nextNdId:String,distance2ParentObj:Double,radius:Double);
case class MtNd(id:String,parentObj:String,entries:Array[Entry],dist2ParentNd:Tuple2[String,Double],typ:String);

case class  QueryResult(ndId:String,objId:String);
class mtree extends util {
  var mtNdMap=scala.collection.mutable.Map[String,MtNd]();
  var ptMap=scala.collection.mutable.Map[String,Point]();  
  var ndCount=1;
  var ndCapacity=10;   //100
  implicit var rootNd=MtNd(ndCount.toString,"",Array[Entry](),("None",0.0),"leaf");
  implicit var colName=Array[String]();
	implicit var colAttr=Array[(String,String,String,String)]();
	
	def initialization(capacity:Int,pcolName:Array[String],
      pcolAttr:Array[(String,String,String,String)]){
	  colName=pcolName;
    colAttr=pcolAttr;    
    ndCapacity=capacity;    
	}
  
  def create(id:String,content:Array[Double]){
    var ndid=ndCount.toString;
    rootNd=MtNd(ndid,id,Array(Entry(id,"None",0.0,0.0)),("None",0.0),"leaf");
    mtNdMap=mtNdMap+(ndid->rootNd);
    ptMap=ptMap+(id->Point(content,ndid));    
  }
  def insert(objId:String,curContent:Array[Double]){
	  privateInsert(objId,curContent)(rootNd);
  }
  def privateInsert(objId:String,curContent:Array[Double])(implicit nd:MtNd){
    if (!ptMap.contains(objId)){
    	ptMap=ptMap+(objId->Point(curContent,"None"));  
    }    
    if (nd.typ=="leaf"){
      var dist2ParentObj=eucDistance(ptMap(nd.parentObj).content,curContent);
      var entryItem=Entry(objId,"None",dist2ParentObj,0.0);
    	if(nd.entries.size<ndCapacity){
    		mtNdMap=mtNdMap-nd.id;
    		mtNdMap=mtNdMap+(nd.id->MtNd(nd.id,nd.parentObj,nd.entries:+entryItem,nd.dist2ParentNd,nd.typ));    		
    		//Entry(nd.parentObj,nd.id,nd.dist2ParentNd._2,nd.entries.map(_.distance2ParentObj).max);
    		//ptMap=ptMap-objId+(objId->Point(curContent,nd.id));
    	} else {
    		split(nd,entryItem);
    	}  
    } else {    
      var objNdId=""; 
      var Nin=Map[String,Double]();   
      var subNin=Map[String,Double]();
      
      for (entry<-nd.entries){
        var tempDist=eucDistance(ptMap(entry.obj).content,curContent);  
    	  
    	  Nin=Nin+(entry.nextNdId->tempDist);
    	  if (tempDist<=entry.radius){
    	    subNin=subNin+(entry.nextNdId->tempDist);
    	  }
      }     
      if (subNin.isEmpty){  
        objNdId=Nin.minBy(x=>(x._2-nd.entries.filter(_.nextNdId==x._1).head.radius))._1;    	  
    	  mtNdMap=mtNdMap-nd.id;
    	  var tEntryItem=nd.entries.filter(_.nextNdId==objNdId).head;
    	  var tentry=nd.entries.filter(_.nextNdId!=objNdId):+Entry(tEntryItem.obj,tEntryItem.nextNdId,tEntryItem.distance2ParentObj,Nin(objNdId));
    	  mtNdMap=mtNdMap+(nd.id->MtNd(nd.id,nd.parentObj,tentry,nd.dist2ParentNd,nd.typ));
      } else {
        objNdId=subNin.minBy(_._2)._1;
      }
      privateInsert(objId,curContent)(mtNdMap(objNdId));      
    }
    rootNd=mtNdMap(rootNd.id);
  }
  def split(nd:MtNd,entry:Entry){        
    if (nd.dist2ParentNd._1=="None"){     //root node
      var tempDistSet=nd.entries.map(_.distance2ParentObj);
      var minDist=tempDistSet.min;
      var maxDist=tempDistSet.max;
      
      var sonObjs=nd.entries.filter(x=>x.distance2ParentObj==maxDist||x.distance2ParentObj==minDist).map(_.obj);
      
      var separatedEntry=entryDistribution(sonObjs(0),sonObjs(1),nd.entries:+entry); 
      var newEntry=Array[Entry]();
      for (x <-sonObjs){
        var dist2parentObj=nd.entries.filter(_.obj==x).head.distance2ParentObj;
    	  var tempEntries=separatedEntry(x);
    	  var newRadius=getRadius(tempEntries,nd.typ);
    	  ndCount=ndCount+1;
    	  var newNd=ndCount.toString;
    	  mtNdMap=mtNdMap+(newNd->MtNd(newNd,x,tempEntries,(nd.id,dist2parentObj),nd.typ));    	  
    	  newEntry=newEntry:+Entry(x,newNd,dist2parentObj,newRadius);    	  
      }; 
      mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntry,("None",0.0),"route"));   //register new route objectives to the parent node;
            
    } else {
    	var parentNdId=nd.dist2ParentNd._1;     
    	var parentNd=mtNdMap(parentNdId);
    	var tempDistSet=nd.entries.map(_.distance2ParentObj);    	  
    	var maxDist=tempDistSet.max;
    	var candidate=nd.entries.filter(_.distance2ParentObj==maxDist).head.obj; 
    	var newEntry=Array[Entry]();
    	
    	var separatedEntry=entryDistribution(candidate,nd.parentObj,nd.entries:+entry);    	
    	for (x<-Array(candidate,nd.parentObj)){    	  
    		var dist2ParentNd= x match{
    		  case nd.parentObj => nd.dist2ParentNd._2
    		  case default => eucDistance(ptMap(mtNdMap(parentNdId).parentObj).content,ptMap(x).content)
    		};    		
    	  var tempEntries=separatedEntry(x);
    	  var newRadius=getRadius(tempEntries,nd.typ);
    	  if (x==nd.parentObj){
    	    mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,x,tempEntries,nd.dist2ParentNd,nd.typ));
    	    var newParentEntries=parentNd.entries.filter(_.obj!=x):+Entry(x,nd.id,newRadius,nd.dist2ParentNd._2);
    	    mtNdMap=mtNdMap-parentNdId+(parentNdId->MtNd(parentNdId,parentNd.parentObj,newParentEntries,parentNd.dist2ParentNd,parentNd.typ));
    	  } else {
    		  ndCount=ndCount+1;
    		  var newNd=ndCount.toString;
    		  mtNdMap=mtNdMap+(newNd->MtNd(newNd,x,tempEntries,(parentNdId,dist2ParentNd),nd.typ));
    		  newEntry=newEntry:+Entry(x,newNd,dist2ParentNd,newRadius); 
    	  }    		
    	}    	
    	if (parentNd.entries.length==ndCapacity){
    		split(parentNd,newEntry.head);
    	} else {    		
    		mtNdMap=mtNdMap-parentNdId+
    				(parentNdId->MtNd(parentNdId,parentNd.parentObj,parentNd.entries++newEntry,parentNd.dist2ParentNd,parentNd.typ));
    	}
      
    }
    
  }
  
  def getRadius(entries:Array[Entry],typ:String):Double={
    var dist=0.0
    if (entries.length>0){
    	dist= typ match {
    	case "leaf" => entries.map(_.distance2ParentObj).max
    	case default => entries.map(x=>x.distance2ParentObj+x.radius).max
    	}  
    }    
        
    return dist;
  }
  def entryDistribution(o1:String,o2:String,entries:Array[Entry]):scala.collection.mutable.Map[String,Array[Entry]]={
    
    var tempSepMap=scala.collection.mutable.Map(o1->Array[Entry](),o2->Array[Entry]());
    for (id<-entries){    
      
    	var dist1=eucDistance(ptMap(o1).content,ptMap(id.obj).content);
    	var dist2=eucDistance(ptMap(o2).content,ptMap(id.obj).content);          
    	if (dist1>=dist2){    	  
    		tempSepMap(o2)=tempSepMap(o2):+Entry(id.obj,id.nextNdId,dist2,id.radius);
    	} else {
    		tempSepMap(o1)=tempSepMap(o1):+Entry(id.obj,id.nextNdId,dist1,id.radius);
    	}

    }
    return tempSepMap;
  }
  def rangeQuery(id:String,radius:Double)(implicit nd:MtNd):Array[QueryResult]={
    var neig=Array[QueryResult]();     
    var dist=eucDistance(ptMap(nd.parentObj).content,ptMap(id).content);
    if (nd.typ!="leaf"){      
      for (x<-nd.entries if math.abs(dist-x.distance2ParentObj)<=(radius+x.radius)){        	
        var dist1=eucDistance(ptMap(x.obj).content,ptMap(id).content);
        if (dist1<(radius+x.radius)) {
          neig=neig++rangeQuery(id,radius)(mtNdMap(x.nextNdId));
        }
      }
    } else {
      for (x<-nd.entries if math.abs(dist-x.distance2ParentObj)<=radius){
        var dist1=eucDistance(ptMap(x.obj).content,ptMap(id).content);
        if (dist1<=radius) {
          neig=neig:+QueryResult(nd.id,x.obj);
        }
      }
    }    
    return neig;
  }
  def query(id:String,content:Array[Double],radius:Double):Array[String]={
    var qr=rangeQuery(id,radius)(rootNd);    
    return(qr.map(_.objId));
  }
  
  def delUpdate(id:String,content:Array[Double],nd:MtNd,candidateEntry:Entry){
    if (nd.typ=="leaf"){
      var parentNdId=nd.dist2ParentNd._1;
      var newEntries=nd.entries.filter(_.obj!=id);      
      mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));
      var radius=getRadius(newEntries,nd.typ);
      if(parentNdId!="None")
    	  delUpdate(id,content,mtNdMap(parentNdId),Entry(nd.parentObj,nd.id,nd.dist2ParentNd._2,radius));
      
    } else {
    	if (nd.dist2ParentNd._1=="None"){
    		var newEntries=nd.entries.filter(_.obj!=candidateEntry.obj):+candidateEntry;
    		mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));      
    	} else {
    		var parentNdId=nd.dist2ParentNd._1;
    		var newEntries=nd.entries.filter(_.obj!=candidateEntry.obj):+candidateEntry;      
    		mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));
    		var radius=getRadius(newEntries,nd.typ);
    		delUpdate(id,content,mtNdMap(parentNdId),Entry(nd.parentObj,nd.id,nd.dist2ParentNd._2,radius));
    	}
    }
  }
  def delete(id:String,content:Array[Double]){
    
    var masterNdId=rangeQuery(id,0.1)(mtNdMap(rootNd.id)).filter(_.objId==id).head.ndId;
	  var masterNd=mtNdMap(masterNdId);
	  var newEntries=masterNd.entries.filter(_.obj!=id);
	  //delUpdate(id,masterNd,masterNd.entries.filter(_.obj==id).head);
	  mtNdMap=mtNdMap-masterNdId+(masterNdId->MtNd(masterNd.id,masterNd.parentObj,newEntries,masterNd.dist2ParentNd,masterNd.typ));
	  	  
    if (mtNdMap.filter(_._2.parentObj==id).isEmpty){
		  ptMap=ptMap-id;  
	  }
	  rootNd=mtNdMap(rootNd.id);
  }
}
*/