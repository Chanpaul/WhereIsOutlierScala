package mtree
import whereIsOutLier._
case class MtNd(centerId:String,coverTree:Array[Tuple2[String,Double]],radius:Double, distToParent:Tuple2[String,Double],typ:String);
case class Point(content:Array[Double],parentNd:String);
object mtree extends util {
  var mtNdMap=Map[String,MtNd]();
  var ptMap=Map[String,Point]();  
  //var distMap=Map[String,Double]();
  var ndCapacity=100;
  implicit var colName=Array[String]();
	implicit var colType=Array[(String,String)]();
	implicit var colTypeI=Array[(String,Int)]();     //category  orginal numberic;
	
  //var rootNd=MtNd("",Array[Double](),Array[String](),0.0,("None",0.0),"leaf");
  def setNdCapacity(capacity:Int){
    ndCapacity=capacity;
  }
  //private var rootNd=MtNd(Array[Double](),Array[String](),0.0,0.0);
  def create(id:String,content:Array[Double],pcolName:Array[String],
      pcolType:Array[(String,String)],pcolTypeI:Array[(String,Int)]):MtNd={
    colName=pcolName;
    colType=pcolType;
    colTypeI=pcolTypeI;
    var tRootNd=MtNd(id,Array[Tuple2[String,Double]](),0.0,("None",0.0),"leaf");
    mtNdMap=mtNdMap+(id->tRootNd);
    ptMap=ptMap+(id->Point(content,"0"));
    return(tRootNd);
  }
  def insert(id:String,curContent:Array[Double],nd:MtNd){
    //ptMap=ptMap+(id->Point(content));
    if (nd.typ!="leaf"){
      var Nin=Map[String,Double]();      
      for (tempStr<-nd.coverTree){
    	  var tempDist=eucDistance(ptMap(tempStr._1).content,curContent);  
    	  Nin=Nin+(tempStr._1->tempDist);               
      }
      var objNdId="";      
      var tempNin=Nin.filter(x=>x._2<=mtNdMap(x._1).radius);
      if (!tempNin.isEmpty){
        objNdId=tempNin.minBy(_._2)._1;
      } else {
        objNdId=Nin.minBy(x=>x._2-mtNdMap(x._1).radius)._1;
        var tempNd=mtNdMap(objNdId);
        mtNdMap=mtNdMap-objNdId;
        mtNdMap=mtNdMap+(objNdId->MtNd(objNdId,tempNd.coverTree,Nin(objNdId), 
            tempNd.distToParent,tempNd.typ));
      }
      insert(id,curContent, mtNdMap(objNdId));
    } else {
      if(nd.coverTree.size<ndCapacity){  
        var tempDist=eucDistance(ptMap(nd.centerId).content,curContent);
        mtNdMap=mtNdMap-nd.centerId;
        mtNdMap=mtNdMap+(nd.centerId->MtNd(nd.centerId,nd.coverTree:+(id,tempDist),
            math.max(nd.radius,tempDist),nd.distToParent,nd.typ));
        ptMap=ptMap+(id->Point(curContent,nd.centerId));
      } else {
        split(nd,id);
      }      
    }     
  }
  def split(nd:MtNd,id:String){
    var candidate="";
    var tempD=0.0;
    for (x<-nd.coverTree){
      if (x._2>tempD){
        tempD=x._2;
        candidate=x._1;
      }
    }    
    if (nd.distToParent._1=="None"){
      var tempDistSet=nd.coverTree.map(_._2);
      var minDist=tempDistSet.min;
      var maxDist=tempDistSet.max;
      var sons=nd.coverTree.filter(x=>x._2==minDist||x._2==minDist).map(_._1);      
      var entrySet=(nd.coverTree.map(_._1):+id).filter(y=> sons.contains(y)==false);     
      
      var separatedEntry=entryDistribution(sons(0),sons(1),entrySet); 
      for (x <-sons){
        var tempDist=nd.coverTree.filter(_._1==x).head._2;
    	  var tempCoverTree=separatedEntry(x);
    	  var newRadius=getRadius(tempCoverTree,nd.typ);
    	  mtNdMap=mtNdMap+(x->MtNd(x,tempCoverTree,newRadius, (nd.centerId,tempDist),nd.typ));
      };      
      var rootConverTree=nd.coverTree.filter(y=> sons.contains(y._1));
      var newRadius=getRadius(rootConverTree,"route");
      mtNdMap=mtNdMap-nd.centerId+
      (nd.centerId->MtNd(nd.centerId,rootConverTree,newRadius, nd.distToParent,"route"));
      
    } else {
    	var parent=nd.distToParent._1;      
    	var tempDistSet=nd.coverTree.map(_._2);    	  
    	var maxDist=tempDistSet.max;
    	var candidate=nd.coverTree.filter(_._2==maxDist).head._1; 
    	var tempEntries=(nd.coverTree.map(_._1):+id).filter(_!=candidate);

    	var newEntry=entryDistribution(candidate,nd.centerId,tempEntries);
    	for (x<-Array(candidate,nd.centerId)){
    		var tempDist=0.0
    				if(x==nd.centerId){
    					tempDist=nd.distToParent._2;
    				} else {
    					tempDist=eucDistance(ptMap(parent).content,ptMap(x).content);  
    				}
    		var tempCoverTree=newEntry(x);
    		var newRadius=getRadius(tempCoverTree,nd.typ);
    		mtNdMap=mtNdMap+(x->MtNd(x,tempCoverTree,newRadius, (parent,tempDist),nd.typ));
    	}
    	var parentNd=mtNdMap(parent);
    	if (parentNd.coverTree.length>ndCapacity-1){
    		split(mtNdMap(parent),candidate);
    	} else {
    		var rootConverTree=parentNd.coverTree:+(candidate,mtNdMap(candidate).distToParent._2);
    		var newRadius=getRadius(rootConverTree,"route");
    		mtNdMap=mtNdMap-parent+
    				(parent->MtNd(parent,rootConverTree,newRadius, parentNd.distToParent,"route"));
    	}
      
    }
    
  }
  def checkNdType(entries:Array[Tuple2[String,Double]]):String={
    var typ=mtNdMap.filter(x=>entries.map(_._1).contains(x._1)).isEmpty match {
      case true => "leaf"
      case default => "rout"
    }
    return typ
  }
  def getRadius(entries:Array[Tuple2[String,Double]],typ:String):Double={
    var dist= typ match {
      case "leaf" => entries.map(_._2).max
      case default => entries.map(x=>x._2+mtNdMap(x._1).radius).max
    }
    
    return dist;
  }
  def entryDistribution(o1:String,o2:String,entries:Array[String]):scala.collection.mutable.Map[String,Array[Tuple2[String,Double]]]={
    var tempSepMap=scala.collection.mutable.Map(o1->Array[Tuple2[String,Double]](),o2->Array[Tuple2[String,Double]]());
    for (id<-entries){
      if (id!=o1 && id!=o2){        
        var dist1=eucDistance(ptMap(o1).content,ptMap(id).content);
        var dist2=eucDistance(ptMap(o2).content,ptMap(id).content);          
        if (dist1>=dist2){
          tempSepMap(o2)=tempSepMap(o2):+(id,dist2);
        } else {
          tempSepMap(o1)=tempSepMap(o1):+(id,dist2);
        }
      }
    }
    return tempSepMap;
  }
  def rangeQuery(nd:MtNd,id:String,radius:Double):Array[String]={
    var neig=Array[String]();
    var dist=eucDistance(ptMap(nd.centerId).content,ptMap(id).content);
    if (nd.typ!="leaf"){
      for (x<-nd.coverTree if math.abs(dist-x._2)<=(radius+mtNdMap(x._1).radius)){
        var dist1=eucDistance(ptMap(x._1).content,ptMap(id).content);
        if (dist1<(radius+mtNdMap(x._1).radius)) {
          neig=neig++rangeQuery(mtNdMap(x._1),id,radius);
        }
      }
    } else {
      for (x<-nd.coverTree if math.abs(dist-x._2)<=radius){
        var dist1=eucDistance(ptMap(x._1).content,ptMap(id).content);
        if (dist1<=radius) {
          neig=neig:+x._1;
        }
      }
    }    
    return neig;
  }
  
  def delete(id:String){
    var parentNd=ptMap(id).parentNd;  
    
    if (mtNdMap.contains(id)){      
      var tempDist=100000000000.0;
      var candidate="";
      for (x<-mtNdMap(parentNd).coverTree if x._1!=id){
        var tempDist1=eucDistance(ptMap(x._1).content,ptMap(id).content);
        if (tempDist>tempDist1) {
          tempDist=tempDist1;
          candidate=x._1;
        }
    	    
      }    
      
      
    } else {
      var centerId=ptMap(id).parentNd;
      var mtNd=mtNdMap(centerId);
      var tempEntries=mtNd.coverTree.filter(_._1!=id);
      var tempRadius=getRadius(tempEntries,mtNd.typ);
      mtNdMap=mtNdMap-centerId+(centerId->MtNd(centerId,tempEntries,tempRadius, mtNd.distToParent,mtNd.typ));
      ptMap=ptMap-id;
    }
    
  }
}