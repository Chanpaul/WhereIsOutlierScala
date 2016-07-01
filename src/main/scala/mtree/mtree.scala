import whereIsOutLier._
import breeze.linalg._
import operators._
import breeze.numerics._
import scala.util.control._     //break need

package mtree  {  
//class mtree[T >: Null <: AnyRef,T1 >: Null <: AnyRef] {
class mtree[T >: Null <: AnyRef] () extends util {  
	implicit var colName=null:Array[String];
	implicit var colAttr=null:Array[(String,String,String,String)];
  type Node = mtree.Node[T]

  private[mtree] implicit var root = null: Node

  /** The root node in the mtree. */
  def rootNode = root

  private[mtree] var n = 0

  /** Number of nodes in the mtree. */
  def size = n
  def init(capacity:Int,pcolName:Array[String],
      pcolAttr:Array[(String,String,String,String)]){
	  colName=pcolName;
	  colAttr=pcolAttr;
  }
  /**
   * Removes all elements from this root.
   */
  def clear() {
    root = null
    n = 0
  }
  /**
   * Achieve range query with radius given.
   * @data the center of query   
   * @R  radius of query
   * Return IDs of the neighbors found 
   */   
   
  def query(item:Tuple2[T,DenseVector[Double]],R:Double):Array[Tuple2[T,DenseVector[Double]]]={    
   var queryResult=root.query(item,R,root);
   return queryResult;
  }
  
  /**
   * Deletes a item from the tree.
   *
   * @param  item  data to remove from mtree.
   */
  def delete(item:Tuple2[T,DenseVector[Double]]):Array[Tuple2[T,DenseVector[Double]]]={		  
    var deleted=root.delete(item)(root);   
    
    return(deleted);
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
  def insert(item:Tuple2[T,DenseVector[Double]]){    
   if (root==null){
			  root = new Node(item);
			  root.insert(item);
		  } else {
		    root.insert(item);
		  }
  }

}

object mtree {

  /** Implements a node of the mtree. */
  class Node[T](val anchor:Tuple2[T,DenseVector[Double]])(implicit pcolName:Array[String],
      pcolAttr:Array[(String,String,String,String)]) extends util{  
	  
	  private[mtree] var parent = null: Node[T] ;
	  private[mtree] var capacity = 20;
	  private[mtree] implicit var minRange=0;     //The minimum checking range for query;

	  private[mtree] var children = new Array[Tuple3[Double,Tuple2[T,DenseVector[Double]],Node[T]]](capacity); //null: Tuple2[Double,Node] ;   
	  private[mtree] var idleSlot = (0 to capacity-1 ).toArray;
	  private[mtree] var mark = false;  //leaf or route
	  private[mtree] var radius:Double=0.0; 
	  
	  private[mtree] def insert(item:Tuple2[T,DenseVector[Double]]):DenseVector[Double]={
	    var objectiveAnchor=DenseVector[Double]();
		  if (mark==false){
			  insertItem(item);
		  } else {      
			  var busySlot=(0 to capacity-1).toArray.diff(idleSlot.toSeq);
			  var dists=busySlot.map(x=>Tuple2(x,eucDistance(children(x)._2._2,item._2)));
			  var obj1= dists.filter(x=>x._2<=children(x._1)._3.radius);
					  if (obj1.isEmpty==false){
						  children(obj1.minBy(_._2)._1)._3.insert(item);
					  } else {
						  var obj2= dists.minBy(x=>x._2-children(x._1)._3.radius);
						  children(obj2._1)._3.insert(item);
					  }
		  }
		  return objectiveAnchor;
	  }
	  
    private[mtree] def insertItem(item: Tuple2[T,DenseVector[Double]]) {      
      var dist=eucDistance(item._2,this.anchor._2);
      children(idleSlot.head)=Tuple3(dist,item,null);
      idleSlot=idleSlot.diff(Seq(idleSlot.head));
      radius=getRadius();
      if (idleSlot.isEmpty==true){        
        this.split();        
      }      
    }
   
     private[mtree] def insertItem(item: Node[T]) {       
      var dist=eucDistance(item.anchor._2,this.anchor._2);
      children(idleSlot.head)=Tuple3(dist,item.anchor,item);
      idleSlot=idleSlot.diff(Seq(idleSlot.head)); 
      radius=getRadius();
      if (idleSlot.isEmpty==true){
        this.split();        
      }
    }
     private[mtree] def split(){  
       
       var z=parent;       
       var busySlot=(0 to capacity-1 ).toArray.diff(idleSlot.toSeq);
       var tChildren=busySlot.map(x=>this.children(x));
       if (z==null){
         var altNd1=new Node(tChildren.minBy(_._1)._2);
         altNd1.parent=this
         altNd1.mark=this.mark;
         var altNd2=new Node(tChildren.maxBy(_._1)._2);
         altNd2.parent=this
         altNd2.mark=this.mark;
         for (s<-busySlot){
           var dist1=eucDistance(children(s)._2._2,altNd1.anchor._2);
           var dist2=eucDistance(children(s)._2._2,altNd2.anchor._2);
           (dist1>=dist2) match {
             case true  => mark match {case true => altNd2.insertItem(children(s)._3); case false=> altNd2.insertItem(children(s)._2);};
             case false => mark match {case true => altNd1.insertItem(children(s)._3); case false=> altNd1.insertItem(children(s)._2);};
           }              
         }
         this.idleSlot = (0 to capacity-1 ).toArray;
         this.insertItem(altNd1);
         this.insertItem(altNd2); 
         this.mark=true;
       } else {            
    	   var altNd=new Node(tChildren.maxBy(_._1)._2);    	   
    	   altNd.parent=this.parent;
    	   altNd.mark=this.mark;
    	   
    	   for (s<-busySlot){
    		   var dist1=eucDistance(children(s)._2._2,altNd.anchor._2);  
    		   if (dist1<children(s)._1){  
    		     this.mark match {case true => altNd.insertItem(children(s)._3); case false=> altNd.insertItem(children(s)._2);};
    			   //altNd.insertItem(children(s)._2);
    			   this.idleSlot=this.idleSlot:+s;    			   
    		   }              
    	   }
    	   this.parent.insertItem(altNd);    	   
       }
    }
     private[mtree] def query(item:Tuple2[T,DenseVector[Double]],R:Double,nd:Node[T]):Array[Tuple2[T,DenseVector[Double]]]={
    		 var queryRes=Array[Tuple2[T,DenseVector[Double]]]();
    		 var busySlot=(0 to nd.capacity-1).toArray.diff(nd.idleSlot.toSeq);
    		 var dist=eucDistance(item._2,nd.anchor._2);
    		 if (nd.mark==true){
    			 for (s<-busySlot){  
    				 var tt= nd.children(s);
    				 if (abs(dist-tt._1)<=R+tt._3.radius){
    					 var dist1=eucDistance(item._2,tt._2._2);
    					 if (dist1<=R+tt._3.radius){
    						 queryRes= queryRes++tt._3.query(item,R,tt._3);  
    					 }
    				 }  
    			 }
    		 } else {
    			 for (s<-busySlot){  
    				 var tt= nd.children(s);
    				 if (abs(dist-tt._1)<=R){
    					 var dist1=eucDistance(item._2,tt._2._2);
    					 if (dist1<=R){
    						 queryRes=queryRes:+tt._2;  
    					 }
    				 }  
    			 }
    		 }
    		 return queryRes
     }
     
    private[mtree] def getRadius():Double={
      var busySlot=(0 to capacity-1).toArray.diff(idleSlot.toSeq)
      var r= mark match {
        case true => busySlot.map(x=>children(x)._1+children(x)._3.radius).max
        case false => busySlot.map(x=>children(x)._1).max
      }
      return r;
    } 
    private[mtree] def delete(item:Tuple2[T,DenseVector[Double]])(implicit nd:Node[T]):Array[Tuple2[T,DenseVector[Double]]]={      
    	
      var deleted=Array[Tuple2[T,DenseVector[Double]]]();      
    	var busySlot=(0 to nd.capacity-1).toArray.diff(nd.idleSlot.toSeq);
    	var dist=eucDistance(item._2,nd.anchor._2);    	
    		if (nd.mark==true){
    			for (s<-busySlot){  
    				var tt= nd.children(s);
    				if (abs(dist-tt._1)<=tt._3.radius){
    					var dist1=eucDistance(item._2,tt._2._2);
    					if (dist1<=tt._3.radius){
    						deleted= deleted++tt._3.delete(item)(tt._3);      						
    					}
    				}  
    			}
    		} else {
    			for (s<-busySlot){  
    				if (item._1==nd.children(s)._2._1){
    					nd.idleSlot=nd.idleSlot:+s;
    					deleted=deleted:+nd.children(s)._2;    					
    				}    		  
    			}
    			if ((0 to nd.capacity-1).toArray.diff(nd.idleSlot.toSeq).isEmpty==true){
    				radius=0.0;
    			} else {
    				nd.radius=getRadius();  
    			}

    		  
    	}   	
    	return deleted;
    } 
  }  
  
}

}