package whereIsOutLier

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders
import com.thesamet.spatial
import com.thesamet.spatial._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import scala.util.matching.Regex
import java.io.File
import java.io.File._
import java.util.Scanner
import java.io._
import scala.math
import com.typesafe.config._    //config need
import scala.util.control._     //break need

//case class Slide(slidId:Int,element:Seq[String],expTriger:Seq[String]);
case class Evidence(succ:Int,lastSlid:Int,prev:Map[Int,Int],checkOnLeave:Int,status:String);
case class LeapPtInfo(id:String,slideID:Int,startTime:Double,content:Row);

object leap extends util{
  var window=Window(12.0,1);  //width=12.0,slide=1;	  
  var outlierParam=OutlierParam("ThreshOutlier",12.62,6); 
  
  def setConfig(config:Config){
    outlierParam=OutlierParam(config.getString("outlier.typ"),
        config.getDouble("outlier.R"),
        config.getInt("outlier.k")); 
    window=Window(config.getDouble("win.width"),
			  config.getInt("win.slideLen"));
  }
  
  def leapMain(dataFile:String,sqlContext:SQLContext){    
	  val df = sqlContext.read
				  .format("com.databricks.spark.csv")
				  .option("delimiter",",")
				  .option("header", "true") // Use first line of all files as header
				  .option("inferSchema", "true") // Automatically infer data types
				  .load(dataFile);   //"cars.csv"  
	  implicit var colName=df.columns;
	  implicit var colType=df.dtypes;	  
	  
    var firstDataItem=df.first;
    var id="1";
    distMap=distMap+(id+","+id->0.0);   //distance matrix;
    var tPtInfo=LeapPtInfo(id,1,1.0,df.first);      
    var ptInWindow=Map(id->tPtInfo);  //can also use tuple like (id,df.first)    
	  var ptCount=1;	
	  var outliers=Seq("none");
	  
	  while(ptCount<df.count) {
	    thresh(ptInWindow,ptEvidence);
	  }
	  
  }
  def thresh(ptInWindow:Map[String,LeapPtInfo],ptEvidence:Map[String,Evidence]){
    var tempPtInWindow=ptInWindow;
    var tempPtEvidence=ptEvidence;
    
    var maxID=ptInWindow.map(_._2.slideID).max;
    var minID=ptInWindow.map(_._2.slideID).min;
    for(it<-ptInWindow.filter(_._2.slideID==maxID).iterator){
      tempPtEvidence=tempPtEvidence-it._1;
    	var curPtEvi=leap(it._1,ptInWindow.filter(_._2.slideID!=minID),ptEvidence); 
    	tempPtEvidence=tempPtEvidence+(it._1->curPtEvi);
    }
    for(it<-ptEvidence.filter(_._2.checkOnLeave==minID).iterator){
      var curPtEvi=it._2.prev-minID;
      var newPtEvi=leap(it._1,ptInWindow,ptEvidence);
      
      //it._1;
      tempPtEvidence=tempPtEvidence-it._1;
    	var curPtEvi=leap(it._1,ptInWindow.filter(_._2.slideID!=minID),ptEvidence); 
    	tempPtEvidence=tempPtEvidence+(it._1->curPtEvi);
    }
    
  }
  def knn(){
    
  }
  def leap(ptId:String,ptInWindow:Map[String,LeapPtInfo],ptEvidence:Map[String,Evidence]):Evidence={
    var temprev=ptEvidence(ptId).prev;
    var checkOnLeave=ptEvidence(ptId).checkOnLeave;
    var status=ptEvidence(ptId).status;
    var succ=ptEvidence(ptId).succ;
    
    val loop = new Breaks;
    var tempID=ptInWindow(ptId).slideID;
    var lastSlid=ptEvidence(ptId).lastSlid;
    var subWindowIt=ptInWindow.filter(_._2.slideID>lastSlid).iterator;  
    lastSlid=subWindowIt.map(_._2.slideID).toList.max;
    loop.breakable{
    	for (it<-subWindowIt){
    		var distId=ptId+","+it._1;
    		if (!distMap.exists(_._1==distId)){
    			var tempDist=eucDistance(ptInWindow(ptId).content,it._2.content);
    			distMap=distMap+(distId->tempDist);
    		}
    		if (distMap(distId)<=outlierParam.R){
    			succ=succ+1;
    			if (succ>outlierParam.k){
    				status="Safe";    				
    				return(Evidence(succ,lastSlid,temprev,checkOnLeave,status));
    				loop.break;
    			}
    		}
    	}
    }
    if (status!="Safe"){
    	val loop1 = new Breaks;
    	loop1.breakable{
    		for (i<-1 to (tempID-1)){    			
    			if (!temprev.exists(_._1==tempID-i)){
    			  var numOfNeig=0;
    				var subWindowIt2=ptInWindow.filter(_._2.slideID==tempID-i).iterator;
    				for (it1<-subWindowIt2){
    					var distId1=it1._1+","+ptId;
    					if (!distMap.exists(_._1==distId1)){
    						var tempDist=eucDistance(ptInWindow(ptId).content,it1._2.content);
    						distMap=distMap+(distId1->tempDist);
    					}
    					if ((distMap(distId1)<=outlierParam.R)){
    						numOfNeig=numOfNeig+1;
    					}
    				}
    				temprev=temprev+(tempID-i->numOfNeig);
    			}
    			if (succ+temprev.map(_._2).sum>outlierParam.k){
    			  status="Unsafe";
    			  checkOnLeave=tempID-i;
    			  return(Evidence(succ,lastSlid,temprev,checkOnLeave,status));
    			  loop1.break;
    			}
    		}
    	}

    }
    return(Evidence(succ,lastSlid,temprev,checkOnLeave,status));
  }
  
}