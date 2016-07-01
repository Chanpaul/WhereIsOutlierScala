package emtree
import whereIsOutLier._
case class Pt(id:String,content:Array[Double]);
case class Entry(obj:Pt,nextNdId:String,distance2ParentObj:Double,radius:Double);
case class MtNd(id:String,parentObj:Pt,entries:Array[Entry],dist2ParentNd:Tuple2[String,Double],typ:String);
case class  QueryResult(ndId:String,objId:String);
class emtree extends util {
  var mtNdMap=scala.collection.mutable.Map[String,MtNd]();
  var pt2Nd=scala.collection.mutable.Map[String,String]();
  //var ptMap=scala.collection.mutable.Map[String,Point]();  
  var ndCount=1;
  var ndCapacity=10;   //100
  implicit var rootNd=MtNd(ndCount.toString,Pt("",Array[Double]()),Array[Entry](),("None",0.0),"leaf");
  implicit var colName=Array[String]();
  implicit var colAttr=Array[(String,String,String,String)]();
	//implicit var colType=Array[(String,String)]();
	//implicit var colTypeI=Array[(String,Int)]();     //category  orginal numberic;
	//implicit var used=Array[Int]();
	
  //var rootNd=MtNd("",Array[Double](),Array[String](),0.0,("None",0.0),"leaf");
	def initialization(capacity:Int,pcolName:Array[String],
      pcolAttr:Array[(String,String,String,String)]){
	  colName=pcolName;
    colAttr=pcolAttr;
    //colTypeI=pcolTypeI;
    ndCapacity=capacity;
    //used=pused;
	}
  
  def create(id:String,content:Array[Double]){
    var ndid=ndCount.toString;
    rootNd=MtNd(ndid,Pt(id,content),Array(Entry(Pt(id,content),"None",0.0,0.0)),("None",0.0),"leaf");
    mtNdMap=mtNdMap+(ndid->rootNd); 
    pt2Nd=pt2Nd+(id->ndid);
  }
  def insert(objId:String,curContent:Array[Double]){
    var ndId=privateInsert(Pt(objId,curContent))(rootNd);
    pt2Nd=pt2Nd+(objId->ndId);
  }
  def privateInsert(pt:Pt)(implicit nd:MtNd):String={
    var objInNd=nd.id;
    if (nd.typ=="leaf"){
      var dist2ParentObj=eucDistance(nd.parentObj.content,pt.content);
      var entryItem=Entry(pt,"None",dist2ParentObj,0.0);
    	if(nd.entries.size<ndCapacity){
    		mtNdMap=mtNdMap-nd.id;
    		mtNdMap=mtNdMap+(nd.id->MtNd(nd.id,nd.parentObj,nd.entries:+entryItem,nd.dist2ParentNd,nd.typ));  		
    		objInNd=nd.id;
    	} else {
    		split(nd,entryItem);
    	}  
    } else {    
      var objNdId=""; 
      var Nin=Map[String,Double]();   
      var subNin=Map[String,Double]();
      
      for (entry<-nd.entries){
        var tempDist=eucDistance(entry.obj.content,pt.content); 
    	  
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
      objInNd=privateInsert(pt)(mtNdMap(objNdId));      
    }
    rootNd=mtNdMap(rootNd.id);
    return(objInNd);
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
        var dist2parentObj=nd.entries.filter(_.obj.id==x.id).head.distance2ParentObj;
    	  var tempEntries=separatedEntry(x.id);
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
    		  case default => eucDistance(mtNdMap(parentNdId).parentObj.content,x.content)
    		};    		
    	  var tempEntries=separatedEntry(x.id);
    	  var newRadius=getRadius(tempEntries,nd.typ);
    	  if (x.id==nd.parentObj.id){
    	    mtNdMap=mtNdMap-nd.id+(nd.id->MtNd(nd.id,x,tempEntries,nd.dist2ParentNd,nd.typ));
    	    var newParentEntries=parentNd.entries.filter(_.obj.id!=x.id):+Entry(x,nd.id,newRadius,nd.dist2ParentNd._2);
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
  def entryDistribution(o1:Pt,o2:Pt,entries:Array[Entry]):scala.collection.mutable.Map[String,Array[Entry]]={
    var tempSepMap=scala.collection.mutable.Map(o1.id->Array[Entry](),o2.id->Array[Entry]());
    for (id<-entries){       
    	var dist1=eucDistance(o1.content,id.obj.content);
    	var dist2=eucDistance(o2.content,id.obj.content);          
    	if (dist1>=dist2){    	  
    		tempSepMap(o2.id)=tempSepMap(o2.id):+Entry(id.obj,id.nextNdId,dist2,id.radius);
    	} else {
    		tempSepMap(o1.id)=tempSepMap(o1.id):+Entry(id.obj,id.nextNdId,dist1,id.radius);
    	}
    }    
    return  tempSepMap;   
    
  }
  def reverseQuery(curPt:Pt,radius:Double)(implicit nd:MtNd):Array[QueryResult]={    
    var neig=Array[QueryResult]();
    if (nd.dist2ParentNd._1!="None"){
    	var nxNd=mtNdMap(nd.dist2ParentNd._1);
    	var tempEntries=nxNd.entries;
    	var dist=eucDistance(nxNd.parentObj.content,curPt.content);
    	var con=false;
    	for (x<-tempEntries if x.nextNdId!=nd.id){
    		if (math.abs(dist-x.distance2ParentObj)<=(radius+x.radius)){    	  
    			var dist1=eucDistance(x.obj.content,curPt.content);
    			if (dist1<(radius+x.radius)) {
    				con=true;
    				neig=neig++rangeQuery(curPt,radius)(mtNdMap(x.nextNdId));
    			}  
    		}    	
    	}
    	if (con==true){
    		neig=neig++reverseQuery(curPt,radius)(nxNd);
    	}  
    }
    return neig;    
  }
  def rangeQuery(curPt:Pt,radius:Double)(implicit nd:MtNd):Array[QueryResult]={
    var neig=Array[QueryResult]();     
    var dist=eucDistance(nd.parentObj.content,curPt.content);
    if (nd.typ!="leaf"){      
      for (x<-nd.entries if math.abs(dist-x.distance2ParentObj)<=(radius+x.radius)){        	
        var dist1=eucDistance(x.obj.content,curPt.content);
        if (dist1<(radius+x.radius)) {
          neig=neig++rangeQuery(curPt,radius)(mtNdMap(x.nextNdId));
        }
      }
    } else {
      for (x<-nd.entries if math.abs(dist-x.distance2ParentObj)<=radius){
        var dist1=eucDistance(x.obj.content,curPt.content);
        if (dist1<=radius) {
          neig=neig:+QueryResult(nd.id,x.obj.id);
        }
      }
    }    
    return neig;
  }
  def query(id:String,content:Array[Double],radius:Double):Array[String]={
    var ndId=pt2Nd(id);
    var qr=rangeQuery(Pt(id,content),radius)(mtNdMap(ndId));    
    qr=qr++reverseQuery(Pt(id,content),radius)(mtNdMap(ndId));
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
  def delete(id:String,content:Array[Double]){    
    var masterNdId=rangeQuery(Pt(id,content),0.1)(mtNdMap(pt2Nd(id))).filter(_.objId==id).head.ndId;
	  var masterNd=mtNdMap(masterNdId);
	  var newEntries=masterNd.entries.filter(_.obj!=id);
	  //delUpdate(id,masterNd,masterNd.entries.filter(_.obj==id).head);
	  mtNdMap=mtNdMap-masterNdId+(masterNdId->MtNd(masterNd.id,masterNd.parentObj,newEntries,masterNd.dist2ParentNd,masterNd.typ));
	  pt2Nd=pt2Nd.filter(_._1!=id);	    
    rootNd=mtNdMap(rootNd.id);	  
  }
}