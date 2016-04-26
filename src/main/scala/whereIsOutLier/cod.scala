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

case class CodMeta(preNeig:Seq[String],succ:Int,checkOnLeave:Double,status:String);
case class CodPt(id:String,startTime:Double,expTime:Double,content:Row);
//case class Event(time:Double,id:String);
object cod extends util{ 
  var window=Window(12.0,1);  //width=12.0,slide=1;	  
  var outlierParam=OutlierParam("ThreshOutlier",12.62,6);
  private var distMap=scala.collection.mutable.Map[String,Double]();
  private var ptInWindow=Map[String,CodPt]();
  private var ptMeta=Map[String,CodMeta]();
  private var curWindowStart=0;
  implicit var colName=Array[String]();
	implicit var colType=Array[(String,String)]();
	var srcDataDir="";
	var srcDataFileName="";
	var resDataDir="";
	var resDataFileName="";
  def setConfig(config:Config){
    outlierParam=OutlierParam(config.getString("outlier.typ"),
        config.getDouble("outlier.R"),
        config.getInt("outlier.k")); 
    window=Window(config.getDouble("win.width"),
			  config.getInt("win.slideLen"));
    srcDataDir=config.getString("dataset.directory");
    srcDataFileName=config.getString("dataset.dataFile")
    resDataDir=config.getString("outlier.directory")
    resDataFileName=config.getString("outlier.fileName");
  }
  def depart(){		  
		  var expired=ptInWindow.filter(_._2.startTime<=curWindowStart).iterator;		
		  curWindowStart=curWindowStart+window.slideLen;
		  for (expIt<-expired){
			  ptInWindow=ptInWindow-expIt._1;
			  
			  if (ptMeta.exists(_._1==expIt._1)){
				  ptMeta=ptMeta-expIt._1;  
			  }
			  
			  var temp=ptMeta.filter(_._2.preNeig.contains(expIt._1)).iterator;
			  
			  for (it3<-temp){				 
				  ptMeta=ptMeta-it3._1;
				  var status=it3._2.status;
				  var tempNeig=it3._2.preNeig.filter(_!=expIt._1);			  
				 var checkOnLeave=0.0;
				  if (it3._2.succ+tempNeig.length<outlierParam.k){
				    status="outlier";
				  } else if (!ptInWindow.filter(x=>tempNeig.contains(x._1)).isEmpty){
					  checkOnLeave=ptInWindow.filter(x=>tempNeig.contains(x._1)).map(_._2.expTime).min;
				  }
				  
				  ptMeta=ptMeta+(it3._1->CodMeta(tempNeig,it3._2.succ,checkOnLeave,status));				  
			  }
		  }		  	  
  }
  
  def codMain(sqlContext:SQLContext){
    import sqlContext.implicits._;
    val dataFile=srcDataDir+"//"+srcDataFileName;
    //val ds=sqlContext.read.text(dataFile).as[String].map(_.split(","));
	  val df = sqlContext.read
				  .format("com.databricks.spark.csv")
				  .option("delimiter",",")
				  .option("header", "true") // Use first line of all files as header
				  .option("inferSchema", "true") // Automatically infer data types
				  .load(dataFile);   //"cars.csv"  
	  colName=df.columns;
	  colType=df.dtypes;
	  colType.map(x=>println(x._1,x._2));
	  val writer = new PrintWriter(new File(resDataDir+"//"+resDataFileName));
	  //var evtQue=Seq(Event(0.0,"0")).toDS;	 
    var firstDataItem=df.first;
    var id="1";
    distMap=distMap+(id+","+id->0.0);   //distance matrix;
    //var tPtInfo=CodMeta(preNeig:Seq[String],succ:Int,checkOnLeave:Int,status:String);  
    //var ptInfo=Map(id->tPtInfo); 
    curWindowStart=1;
    ptInWindow=ptInWindow+(id->CodPt(id,1.0,1.0+window.width,df.first));  //can also use tuple like (id,df.first)  
    ptMeta=ptMeta+(id->CodMeta(Seq[String](),0,0.0,"Outlier"));
	  var ptCount=1;	
	  var dfCount=df.count;
	  while(ptCount<dfCount) {
	    println(ptCount);
	    ptCount+=1;
	    if (ptCount==15)
	      println("debug here");
	    id=ptCount.toString;
	    var curPt= df.head(ptCount).last;	
	    println(curPt);
	    var ptLen=curPt.length;
	    ptInWindow=ptInWindow+(id->CodPt(id,ptCount,ptCount+window.width,curPt));	      
	    if (ptInWindow.size>window.width){
	    	depart();   
	    }
	    /****************setup metrics measure***********************/
	    var begTime=System.nanoTime;
	    //var meter=new MemoryMeter;
	    var runtime=Runtime.getRuntime();
	    var mem1=runtime.freeMemory();
	    /******************end of setup*********************/
	    /*********************Update distance map*************************/
	    for (it<-ptInWindow.filter(_._1!=id).iterator){
	      var tempDist=eucDistance(curPt,it._2.content);	    	
	    	distMap=distMap+(it._1+","+id->tempDist);
	    }	  
	    /*********************end of update distance map***********************/
	    if(!ptMeta.exists(_._1==id)){
	    	ptMeta=ptMeta+(id->CodMeta(Seq[String](),0,0.0,"Outlier"));
	    }
	    searchNeighbor(id);	 
	    /************collect metrics***********************/
	    var mem2=runtime.freeMemory();
	    var cpuUsage=(System.nanoTime-begTime)/1000000000.0;
	    var memUsage=math.abs(mem1-mem2)/(1024*1024);
	    var from=ptInWindow.map(_._2.startTime).min.toString;
	    var to=ptInWindow.map(_._2.startTime).max.toString;
	    printOutlier(writer,from,to,memUsage,cpuUsage);
	    /*********************end of collection*********************/
	  }           
  }  
  def printOutlier(writer:PrintWriter,from:String,to:String,memUsage:Double,cpuUsage:Double){        
    var outliers=ptMeta.filter(_._2.status=="Outlier").map(_._1);
    writer.write(s"From $from to $to, the ourliers are: $outliers,"
        +s"memory usage is: $memUsage,"
        +s"cpu usage is: $cpuUsage"
        +"\n");    
  }
  def searchNeighbor(id:String){    
	    var strPat=new Regex(id);
	    var idCheckTime=ptInWindow(id).expTime;	 
	    var tempPreNeig=ptMeta(id).preNeig;
	    var succ=ptMeta(id).succ;
	    var checkOnLeave=ptMeta(id).checkOnLeave;
	    var status=ptMeta(id).status;
	    
	    for(it<-distMap.iterator){
	    	var isExist=strPat findFirstIn it._1;
	    	if (!isExist.isEmpty && it._2<outlierParam.R){	    	  
	    	  var oid=it._1.split(",").filter(_!=id).head
	    	  tempPreNeig=tempPreNeig:+oid;
	    	}
	    }
	    if (tempPreNeig.length>outlierParam.k){
	      status="Unsafe";
	      checkOnLeave=ptInWindow.filter(x=>tempPreNeig.contains(x._1)).map(_._2.expTime).min;	      
	    }
	    ptMeta=ptMeta-id+(id->CodMeta(tempPreNeig,succ,checkOnLeave,status));
	    for (it1<-tempPreNeig.iterator)
	    tempPreNeig.foreach{(x:String)=>{
	      var tStatus= (ptMeta(x).preNeig.length+ptMeta(x).succ+1) match{
	        case y if y<outlierParam.k  =>"Outlier"
	        case z if z>=outlierParam.k =>"Unsafe"	          
	      }	      
	      var tcodMeta=CodMeta(ptMeta(x).preNeig,ptMeta(x).succ+1,ptMeta(x).checkOnLeave,tStatus);
	      ptMeta=ptMeta-x+(x->tcodMeta);
	      }
	    };	    	  
  }
}