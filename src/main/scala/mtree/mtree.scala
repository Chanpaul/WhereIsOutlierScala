package mtree
import whereIsOutLier._
case class Entry(obj:String,nextNdId:String,distance2ParentObj:Double,radius:Double);
case class MtNd(id:String,parentObj:String,entries:Array[Entry],dist2ParentNd:Tuple2[String,Double],typ:String);
case class Point(content:Array[Double],ndId:String);
case class  QueryResult(ndId:String,objId:String);
class mtree extends util {
  var mtNdMap=Map[String,MtNd]().par;
  var ptMap=Map[String,Point]().par;  
  var ndCount=1;
  var ndCapacity=100;
  implicit var rootNd=MtNd(ndCount.toString,"",Array[Entry](),("None",0.0),"leaf");
  implicit var colName=Array[String]();
	implicit var colType=Array[(String,String)]();
	implicit var colTypeI=Array[(String,Int)]();     //category  orginal numberic;
	implicit var used=Array[Int]();
	
  //var rootNd=MtNd("",Array[Double](),Array[String](),0.0,("None",0.0),"leaf");
	def initialization(capacity:Int,pcolName:Array[String],
      pcolType:Array[(String,String)],pcolTypeI:Array[(String,Int)],pused:Array[Int]){
	  colName=pcolName;
    colType=pcolType;
    colTypeI=pcolTypeI;
    ndCapacity=capacity;
    used=pused;
	}
  
  def create(id:String,content:Array[Double]){
    var ndid=ndCount.toString;
    rootNd=MtNd(ndid,id,Array(Entry(id,"None",0.0,0.0)),("None",0.0),"leaf");
    mtNdMap=mtNdMap+(ndid->rootNd);
    ptMap=ptMap+(id->Point(content,ndid));    
  }
  
  def insert(objId:String,curContent:Array[Double])(implicit nd:MtNd){
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
      insert(objId,curContent)(mtNdMap(objNdId));      
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
  def query(id:String,radius:Double)(implicit nd:MtNd):Array[String]={
    var qr=rangeQuery(id,radius)(nd);    
    return(qr.map(_.objId));
  }
  /*
  def delUpdate(id:String,nd:MtNd,candidateEntry:Entry){
    if (nd.dist2ParentNd._1=="None"){
      var newEntries=nd.entries.filter(_.obj!=id):+candidateEntry;      
      if (nd.parentObj==id){
        var minDist=newEntries.map(_.distance2ParentObj).min;
        var candidate=newEntries.filter(_.distance2ParentObj==minDist).head.obj; 
        newEntries=newEntries.map(x=>Entry(x.obj,x.nextNdId,eucDistance(ptMap(candidate).content,ptMap(x.obj).content),x.radius));
        mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,candidate,newEntries,nd.dist2ParentNd,nd.typ));
      } else {
        mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));
      }
    } else if(nd.typ=="leaf") {
      var parentNdId=nd.dist2ParentNd._1;
      var newEntries=nd.entries.filter(_.obj!=id);
      if (nd.parentObj==id){
        var minDist=newEntries.map(_.distance2ParentObj).min;
        var candidate=newEntries.filter(_.distance2ParentObj==minDist).head.obj;        
        newEntries=newEntries
        		.map(x=>Entry(x.obj,x.nextNdId,eucDistance(ptMap(candidate).content,ptMap(x.obj).content),0.0));
        var dist2ParentNd=eucDistance(ptMap(candidate).content,ptMap(mtNdMap(parentNdId).parentObj).content);
        mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,candidate,newEntries,(parentNdId,dist2ParentNd),nd.typ));
        var radius=getRadius(newEntries,nd.typ);
        delUpdate(id,mtNdMap(parentNdId),Entry(candidate,nd.id,dist2ParentNd,radius));  
      } else {
        mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));
        var radius=getRadius(newEntries,nd.typ);
        delUpdate(id,mtNdMap(parentNdId),Entry(nd.parentObj,nd.id,nd.dist2ParentNd._2,radius));
      }    
      
    } else {
      var newEntries=nd.entries.filter(_.obj==id):+candidateEntry;
      var parentNdId=nd.dist2ParentNd._1;
      if (nd.parentObj==id){
        var minDist=newEntries.map(_.distance2ParentObj).min;
        var candidate=newEntries.filter(_.distance2ParentObj==minDist).head.obj;        
        newEntries=newEntries
        		.map(x=>Entry(x.obj,x.nextNdId,eucDistance(ptMap(candidate).content,ptMap(x.obj).content),x.radius));
        var dist2ParentNd=eucDistance(ptMap(candidate).content,ptMap(mtNdMap(parentNdId).parentObj).content);        
        mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,id,nd.entries.filter(_.obj!=id):+candidateEntry,(parentNdId,dist2ParentNd),nd.typ));
        var radius=getRadius(newEntries,nd.typ);
        delUpdate(id,mtNdMap(parentNdId),Entry(candidate,nd.id,dist2ParentNd,radius));
      } else {
        mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));
        var radius=getRadius(newEntries,nd.typ);
        delUpdate(id,mtNdMap(parentNdId),Entry(nd.parentObj,nd.id,nd.dist2ParentNd._2,radius));
      }
    } 
  }
  */
  def delUpdate(id:String,nd:MtNd,candidateEntry:Entry){
    if (nd.typ=="leaf"){
      var parentNdId=nd.dist2ParentNd._1;
      var newEntries=nd.entries.filter(_.obj!=id);      
      mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));
      var radius=getRadius(newEntries,nd.typ);
      if(parentNdId!="None")
    	  delUpdate(id,mtNdMap(parentNdId),Entry(nd.parentObj,nd.id,nd.dist2ParentNd._2,radius));
      
    } else {
    	if (nd.dist2ParentNd._1=="None"){
    		var newEntries=nd.entries.filter(_.obj!=candidateEntry.obj):+candidateEntry;
    		mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));      
    	} else {
    		var parentNdId=nd.dist2ParentNd._1;
    		var newEntries=nd.entries.filter(_.obj!=candidateEntry.obj):+candidateEntry;      
    		mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,nd.parentObj,newEntries,nd.dist2ParentNd,nd.typ));
    		var radius=getRadius(newEntries,nd.typ);
    		delUpdate(id,mtNdMap(parentNdId),Entry(nd.parentObj,nd.id,nd.dist2ParentNd._2,radius));
    	}
    }
  }
  def delete(id:String){
	  //var masterNdId=ptMap(id).ndId; 	 
	  //var masterNd=mtNdMap(masterNdId);
	  //var newEntries=parentNd.entries.filter(_.obj!=id);	  
	  //mtNdMap=mtNdMap-parentNdId+(parentNdId->MtNd(parentNdId,parentNd.parentObj,newEntries,parentNd.dist2ParentNd,parentNd.typ));
    //println(ptMap.contains("1"));
    var masterNdId=rangeQuery(id,0.1)(mtNdMap(rootNd.id)).filter(_.objId==id).head.ndId;
	  var masterNd=mtNdMap(masterNdId);
	  delUpdate(id,masterNd,masterNd.entries.filter(_.obj==id).head);
	  
    if (mtNdMap.filter(_._2.parentObj==id).isEmpty){
		  ptMap=ptMap-id;  
	  }
	  
  }
}