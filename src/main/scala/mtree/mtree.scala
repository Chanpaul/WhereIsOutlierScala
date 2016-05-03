package mtree
import whereIsOutLier._
case class Entry(obj:String,nextNdId:String,distance2ParentObj:Double,radius:Double);
case class MtNd(id:String,parentObj:String,entries:Array[Entry],dist2ParentNd:Tuple2[String,Double],typ:String);
case class Point(content:Array[Double],ndId:String);
class mtree extends util {
  var mtNdMap=Map[String,MtNd]();
  var ptMap=Map[String,Point]();  
  var ndCount=1;
  var ndCapacity=100;
  implicit var rootNd=MtNd(ndCount.toString,"",Array[Entry](),("None",0.0),"leaf");
  implicit var colName=Array[String]();
	implicit var colType=Array[(String,String)]();
	implicit var colTypeI=Array[(String,Int)]();     //category  orginal numberic;
	
  //var rootNd=MtNd("",Array[Double](),Array[String](),0.0,("None",0.0),"leaf");
	def initialization(capacity:Int,pcolName:Array[String],
      pcolType:Array[(String,String)],pcolTypeI:Array[(String,Int)]){
	  colName=pcolName;
    colType=pcolType;
    colTypeI=pcolTypeI;
    ndCapacity=capacity;
	}
  
  def create(id:String,content:Array[Double]){
    var ndid=ndCount.toString;
    rootNd=MtNd(ndid,id,Array(Entry(id,"None",0.0,0.0)),("None",0.0),"leaf");
    mtNdMap=mtNdMap+(ndid->rootNd);
    ptMap=ptMap+(id->Point(content,ndid));    
  }
  def insert(objId:String,curContent:Array[Double])(implicit nd:MtNd){
    if (nd.typ=="leaf"){
      var dist2ParentObj=eucDistance(ptMap(nd.parentObj).content,curContent);
      var entryItem=Entry(objId,"None",dist2ParentObj,0.0);
    	if(nd.entries.size<ndCapacity){
    		mtNdMap=mtNdMap-nd.id;
    		mtNdMap=mtNdMap+(nd.id->MtNd(nd.id,nd.parentObj,nd.entries:+entryItem,nd.dist2ParentNd,nd.typ));
    		ptMap=ptMap+(objId->Point(curContent,nd.id));
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
      insert(objId,curContent)(mtNdMap(objNdId));      
    }     
  }
  def split(nd:MtNd,entry:Entry){        
    if (nd.dist2ParentNd._1=="None"){     //root node
      var tempDistSet=nd.entries.map(_.distance2ParentObj);
      var minDist=tempDistSet.min;
      var maxDist=tempDistSet.max;
      
      var sonObjs=nd.entries.filter(x=>x.distance2ParentObj==maxDist||x.distance2ParentObj==minDist).map(_.obj);
      
      var separatedEntry=entryDistribution(sonObjs(0),sonObjs(1),nd.entries); 
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
    	var tempDistSet=nd.entries.map(_.distance2ParentObj);    	  
    	var maxDist=tempDistSet.max;
    	var candidate=nd.entries.filter(_.distance2ParentObj==maxDist).head.obj;    	
    	var newEntry=Array[Entry]();
    	var separatedEntry=entryDistribution(candidate,nd.parentObj,nd.entries);    	
    	for (x<-Array(candidate,nd.parentObj)){
    		var dist2ParentNd= x match{
    		  case nd.parentObj => nd.dist2ParentNd._2
    		  case default => eucDistance(ptMap(mtNdMap(parentNdId).parentObj).content,ptMap(x).content)
    		};    		
    	  var tempEntries=separatedEntry(x);
    	  var newRadius=getRadius(tempEntries,nd.typ);
    	  if (mtNdMap.contains(x)){
    	    mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,x,tempEntries,(parentNdId,dist2ParentNd),nd.typ));
    	  } else {
    		  ndCount=ndCount+1;
    		  var newNd=ndCount.toString;
    		  mtNdMap=mtNdMap+(newNd->MtNd(newNd,x,tempEntries,(parentNdId,dist2ParentNd),nd.typ));
    		  newEntry=newEntry:+Entry(x,newNd,dist2ParentNd,newRadius); 
    	  }    		
    	}
    	var parentNd=mtNdMap(parentNdId);
    	if (parentNd.entries.length>ndCapacity-1){
    		split(parentNd,newEntry.head);
    	} else {    		
    		mtNdMap=mtNdMap-parentNdId+
    				(parentNdId->MtNd(parentNdId,parentNd.parentObj,parentNd.entries++newEntry,parentNd.dist2ParentNd,nd.typ));
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
  def getRadius(entries:Array[Entry],typ:String):Double={
    var dist= typ match {
      case "leaf" => entries.map(_.distance2ParentObj).max
      case default => entries.map(x=>x.distance2ParentObj+x.radius).max
    }    
    return dist;
  }
  def entryDistribution(o1:String,o2:String,entries:Array[Entry]):scala.collection.mutable.Map[String,Array[Entry]]={
    var tempSepMap=scala.collection.mutable.Map(o1->Array[Entry](),o2->Array[Entry]());
    for (id<-entries){
    	var dist1=eucDistance(ptMap(o1).content,ptMap(id.obj).content);
    	var dist2=eucDistance(ptMap(o2).content,ptMap(id.obj).content);          
    	if (dist1>=dist2){
    		tempSepMap(o2)=tempSepMap(o2):+id;
    	} else {
    		tempSepMap(o1)=tempSepMap(o1):+id;
    	}

    }
    return tempSepMap;
  }
  def rangeQuery(id:String,radius:Double)(implicit nd:MtNd):Array[String]={
    var neig=Array[String]();
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
          neig=neig:+x.obj;
        }
      }
    }    
    return neig;
  }
  
  def delete(id:String){
	  var parentNdId=ptMap(id).ndId; 	 
	  var parentNd=mtNdMap(parentNdId);
	  var newEntries=parentNd.entries.filter(_.obj!=id);	  
	  mtNdMap=mtNdMap-parentNdId+(parentNdId->MtNd(parentNdId,parentNd.parentObj,newEntries,parentNd.dist2ParentNd,parentNd.typ));
	  ptMap=ptMap-id;    
  }
}