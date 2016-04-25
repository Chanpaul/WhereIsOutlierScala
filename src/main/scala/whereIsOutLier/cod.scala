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
import com.typesafe.config._

case class PtInfo(id:String,preNeig:Seq[String],succNeigNum:Int,startTime:Double,expTime:Double);
case class Event(time:Double,id:String);
object cod extends util{ 
  var window=Window(12.0,1);  //width=12.0,slide=1;	  
  var outlierParam=OutlierParam("ThreshOutlier",12.62,6);
  def setConfig(config:Config){
    outlierParam=OutlierParam(config.getString("outlier.typ"),
        config.getDouble("outlier.R"),
        config.getInt("outlier.k")); 
    window=Window(config.getDouble("win.width"),
			  config.getInt("win.slideLen"));
  }
  def detect(dataDir:String,sqlContext:SQLContext){
    import sqlContext.implicits._;
    val dataFile=dataDir+"//Patient1.csv//"+"part-00000";
    //val ds=sqlContext.read.text(dataFile).as[String].map(_.split(","));
	  val df = sqlContext.read
				  .format("com.databricks.spark.csv")
				  .option("delimiter",",")
				  .option("header", "true") // Use first line of all files as header
				  .option("inferSchema", "true") // Automatically infer data types
				  .load(dataFile);   //"cars.csv"  
	  var colName=df.columns;
	  var colType=df.dtypes;
	  colType.map(x=>println(x._1,x._2));
	  //colName.map(x=>println(x));
	  //df.show(2);
	  var evtQue=Seq(Event(0.0,"0")).toDS;	 
    var firstDataItem=df.first;
    var id="1";
    var distMap=Map(id+","+id->0.0);   //distance matrix;
    var tPtInfo=PtInfo(id,Seq(id),0,1,window.width+1);  
    var ptInfo=Map(id->tPtInfo);    
    var ptInWindow=Map(id->df.first);  //can also use tuple like (id,df.first)    
	  var ptCount=1;	
	  var outliers=Seq("none");
	  case class CurWindow(width:Double,slide:Int,start:Double);
	  var curWindow=CurWindow(window.width,window.slideLen,1);
	  
	  while(ptCount<df.count) {
	    println(ptCount);
	    ptCount+=1;
	    var curPt= df.head(ptCount).last;	
	    var ptLen=curPt.length;
	    ptInWindow=ptInWindow+(ptCount.toString->curPt);
	    id=ptCount.toString;    
	    tPtInfo=PtInfo(id,Seq(id),0,ptCount,window.width+ptCount);
	    ptInfo=ptInfo+(id->tPtInfo);
	    /***********************Departure**********************************/	    
	    if (ptInWindow.size>window.width){
	      curWindow=CurWindow(curWindow.width,curWindow.slide,curWindow.start+curWindow.slide);
	      var expired=ptInfo.iterator.filter(_._2.startTime<curWindow.start);
	      for (expIt<-expired){
	        ptInWindow=ptInWindow-expIt._1;
	        ptInfo=ptInfo-expIt._1;
	        var temp=ptInfo.iterator.filter(_._2.preNeig.contains(expIt._1));
	        for (it3<-temp){
	          ptInfo=ptInfo-it3._1;
	          ptInfo=ptInfo+(it3._1->PtInfo(it3._1,it3._2.preNeig.filter(_!=expIt._1),
	              it3._2.succNeigNum,it3._2.startTime,it3._2.expTime));
	        }
	        
	      }
	      evtQue=evtQue.filter(_.time>curWindow.start);
	    }  
	    /*************************End of Departure*****************************/	    
	    /******************update the distance map*************************/
	    
	    for (it<-ptInWindow.filter(_._1!=id).iterator){
	    	var tsum=0.0;	    		    	
	    	for (attrName<-colName.iterator){
	    	  var attr=colType.filter(_._1==attrName).head._2; 
	    	 
	    	  var temp=getAttr(attr)(it._2,attrName)-getAttr(attr)(curPt,attrName);	    	  
	    		//var temp=it._2.getAs[Double](attrName)-curPt.getAs[Double](attrName);
	    		tsum=tsum+scala.math.pow(temp,2.0);	    		
	    	}  
	    	distMap=distMap+(it._1+","+id->scala.math.sqrt(tsum));
	    } 
	    
	    
	    /**********************End of update distance map*********************/
	    /**********************find neighbors********************************/
	    var strPat=new Regex(id);
	    var idCheckTime=ptInfo(id).expTime;
	    
	    for(it<-distMap.iterator){
	    	var isExist=strPat findFirstIn it._1;
	    	if (!isExist.isEmpty && it._2<outlierParam.R){	    	  
	    	  var oid=it._1.split(",").filter(_!=id).head
	    	  var idPtInfo=ptInfo(id);
	    	  var oidPtInfo=ptInfo(oid);
	    	  idCheckTime=scala.math.min(oidPtInfo.expTime,idCheckTime);
	    	  ptInfo=ptInfo-id-oid;
	    	  ptInfo=ptInfo+(id->PtInfo(id,idPtInfo.preNeig:+oid,idPtInfo.succNeigNum,
	    	      idPtInfo.startTime,idPtInfo.expTime));
	    	  ptInfo=ptInfo+(oid->PtInfo(oid,oidPtInfo.preNeig,oidPtInfo.succNeigNum+1,
	    	      oidPtInfo.startTime,oidPtInfo.expTime));		    	  
	    	  if (outliers.contains(oid)) {
	    	    outliers=outliers.filter(_!=oid);
	    	    var checkExpTime=ptInfo(oid).expTime;
	    	    for(it1<-ptInfo(oid).preNeig.iterator){
	    	      checkExpTime=scala.math.min(checkExpTime,ptInfo(it1).expTime);
	    	    }
	    	    evtQue=evtQue.union(Seq(Event(checkExpTime,oid)).toDS);	 
	    	  }
	    	  
	    	}	    	
	    }
	    if (ptInfo(id).preNeig.length + ptInfo(id).succNeigNum > outlierParam.k){
	      evtQue=evtQue.union(Seq(Event(idCheckTime,id)).toDS);
	    } else {
	      outliers=outliers:+id;
	    }
	    /********************End of finding Neighbors**************************/
	    
	  }       
    
  }  
}