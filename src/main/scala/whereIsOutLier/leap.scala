package whereIsOutLier

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.parallel
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
import mtree._
import emtree._
import breeze.linalg._
//import org.github.jamm

//case class Slide(slidId:Int,element:Seq[String],expTriger:Seq[String]);
case class Evidence(succ:Int,lastSlid:Int,prev:Map[Int,Int],checkOnLeave:Int,status:String);
case class LeapPtInfo(id:String,slideID:Int,startTime:Double,content:Array[Double]);

class leap extends util{
  private var window=Window(12.0,1);  //width=12.0,slide=1;	  
  private var outlierParam=OutlierParam("ThreshOutlier",12.62,6); 
  private var distMap=scala.collection.mutable.Map[String,Double]();   //global variable
  private var ptInWindow=scala.collection.mutable.Map[String,LeapPtInfo]();
  private var ptEvidence=scala.collection.mutable.Map[String,Evidence]();
  
  
  var mt=new mtree.mtree;
  implicit var colName=Array[String]();
	implicit var colType=Array[(String,String)](); 
	implicit var colTypeI=Array[(String,Int)]();     //category  orginal numberic;	
	implicit var used=Array[Int]();
	var notUsed=Array[Int]();
	private var srcDataDir="";
	private var srcDataFileName="";
	var srcMiddle="";
	private var resDataDir="";
	private var resDataFileName="";
  def setConfig(config:Config){
    outlierParam=OutlierParam(config.getString("outlier.typ"),
        config.getDouble("outlier.R"),
        config.getInt("outlier.k")); 
    window=Window(config.getDouble("win.width"),
			  config.getInt("win.slideLen"));
    srcDataDir=config.getString("dataset.directory");
    srcMiddle=config.getString("dataset.middle");
    srcDataFileName=config.getString("dataset.dataFile")
    resDataDir=config.getString("outlier.directory")
    resDataFileName=config.getString("outlier.fileName");
    colTypeI=config.getString("dataattr.type").split(" ").map(_.drop(1).dropRight(1).split(",")).map(x=>(x(0),x(1).toInt));
    notUsed=config.getString("dataattr.notUsed").split(",").map(_.toInt);
    var tempUsed=config.getString("dataattr.used");
    used=tempUsed match{
      case " "=> (0 to (colTypeI.length-1)).toArray.filter(x=>(!notUsed.contains(x)))
      case default =>tempUsed.split(",").map(_.toInt)
    };
    
  }
  
  def printOutlier(writer:PrintWriter,from:String,to:String,memUsage:Double,cpuUsage:Double){        
    var outliers=ptEvidence.seq.filter(_._2.status=="Outlier").map(_._1).map(x=>ptInWindow(x).content);
    var msg=s"From $from to $to, the outliers are:\n";
    for (otly<-outliers){      
      var tempMsg="----------"
      for (x<-notUsed){
        tempMsg=tempMsg+otly.apply(x)+"-";
      }
      msg=msg+tempMsg+"\n";
    }
    msg=msg+s"memory usage is: $memUsage,"+s"cpu usage is: $cpuUsage"+"\n************************************\n";
    writer.write(msg);    
  }
  def printAll(writer:PrintWriter,from:String,to:String,memUsage:Double,cpuUsage:Double){
	  var msg=s"From $from to $to:\n";
	  for (pt <-ptEvidence){    	      
		  var tempMsg="----------";
		  for (x<-notUsed){
			  tempMsg=tempMsg+ptInWindow(pt._1).content.apply(x)+"-";
		  }    	
		  msg=msg+tempMsg+pt._2.status+"-"+(pt._2.succ+pt._2.prev.map(_._2).sum)+"\n";
		  //println(msg);
	  }
	  msg=msg+s"memory usage is: $memUsage,"+s"cpu usage is: $cpuUsage"+"\n************************************\n";
	  writer.write(msg);     
  }
  
  def leapMain(sqlContext:SQLContext){    
    import sqlContext.implicits._;
    val dataFile=srcDataDir+srcMiddle+srcDataFileName;
    /*
	  val df = sqlContext.read
				  .format("com.databricks.spark.csv")
				  .option("delimiter",",")
				  .option("header", "true") // Use first line of all files as header
				  .option("inferSchema", "true") // Automatically infer data types
				  .load(dataFile);   //"cars.csv"  
	  colName=df.columns;
	  colType=df.dtypes;	  
	  
    var firstDataItem=df.first;
    * */         
	  var ptCount=0;	
    var id="1";	  
	  var slideUnit=Map[String,LeapPtInfo]();
	  var slideId=1;
	  val writer = new PrintWriter(new File(resDataDir+"//"+resDataFileName));
	  var lines=scala.io.Source.fromFile(dataFile).getLines;
	  //var mt=new emtree.emtree;
	  mt.initialization(10,colName,colType,colTypeI,used);
	  //initialize profile;
	  var begTime=System.nanoTime;
	  //var meter=new MemoryMeter;
	  var runtime=Runtime.getRuntime();
	  var totalMem=runtime.totalMemory();
	  
	  val pattern=new Regex("^[\\s]+\n");
	  
	  for(line<-lines if (pattern.findAllIn(line).isEmpty)) {	 	    
	    println(ptCount);
		  id=ptCount.toString;
		  var curPt=Array[Double]();
		  if (ptCount==0){
			  colName=line.split(",").map(_.trim).filter(_!="ID");
		  } else{
		    curPt=line.split(",").map(_.trim).map(x=>{x match {
		    case y if x.contains(".") =>x.toDouble
		    case z if !x.contains(".") =>x.toInt.toDouble
		    }
			    });			    
		    var tPtInfo=LeapPtInfo(id,slideId,ptCount.toDouble,curPt);	  	    
		    slideUnit=slideUnit+(id->tPtInfo);
		  }
	    ptCount=ptCount+1;
	    
	    if (slideUnit.size==window.slideLen){
	      var expSlidID=0;
	      if ((ptInWindow.size+slideUnit.size)>window.width){
	        var curFreeMem=runtime.freeMemory();
	    		var cpuUsage=(System.nanoTime-begTime)/1000000000.0;
	    		var memUsage=math.abs(totalMem-curFreeMem)/(1024*1024);    
	    		var from=ptInWindow.map(_._2.startTime).min.toString;
	    		var to=ptInWindow.map(_._2.startTime).max.toString;
	    		printAll(writer,from,to,memUsage,cpuUsage);
	    		
	    		expSlidID=ptInWindow.map(_._2.slideID).min;	    		
	    		ptInWindow=ptInWindow.filter(_._2.slideID!=expSlidID);
	    		ptEvidence=ptEvidence.filter(x=>ptInWindow.contains(x._1));	    			    		
	    		
	    		//printOutlier(writer,from,to,memUsage,cpuUsage);
	    		//reset profile;
	    		begTime=System.nanoTime;
	      }
	      ptInWindow=ptInWindow++slideUnit;     //current Window
	    	
	      var expTrigered=Array[LeapPtInfo]();
	      ptEvidence.filter(_._2.checkOnLeave==expSlidID).map(_._1).foreach(x=>expTrigered=expTrigered++Array(ptInWindow(x)));
	      for (p<-expTrigered){
	        var tempEvidence=ptEvidence(p.id)
	        ptEvidence=ptEvidence-p.id+(p.id->Evidence(tempEvidence.succ,tempEvidence.lastSlid,tempEvidence.prev-expSlidID,
	            tempEvidence.checkOnLeave,tempEvidence.status));
	    		  var lastSlidId=ptEvidence(p.id).lastSlid;
	    			thresh(p,ptInWindow.filter(_._2.slideID>lastSlidId).map(_._2).toArray);  
	    		}	      
	      for (p<-slideUnit){
	    	  thresh(p._2,ptInWindow.map(_._2).toArray); 
	      }
	      slideUnit=slideUnit.empty;
	      slideId=slideId+1;	    	    		            	       
	    }	    	    
	  }
	  writer.close;
  }
  def thresh(pt:LeapPtInfo,ptSet:Array[LeapPtInfo]){
	  var succ=0;
	  var temprev=Map[Int,Int]();
	  var status="Outlier";
	  var checkOnLeave=pt.slideID;
	  var lastSlidId=pt.slideID;
	  if (ptEvidence.contains(pt.id)){
		  succ=ptEvidence(pt.id).succ;
		  temprev=ptEvidence(pt.id).prev;
		  status=ptEvidence(pt.id).status;
		  checkOnLeave=ptEvidence(pt.id).checkOnLeave;
		  lastSlidId=ptEvidence(pt.id).lastSlid+1;
		  ptEvidence=ptEvidence-pt.id;
    }    
    succ=succ+knn(pt,ptSet.filter(_.slideID>=lastSlidId),outlierParam.k-succ);
    lastSlidId=ptSet.map(_.slideID).max;
    if (succ>=outlierParam.k){
      status="Safe";
      temprev=Map[Int,Int]();
    } else if (temprev.isEmpty){         
    	val loop1 = new Breaks;
    	var totalNN=succ;
    	var slidIdRang=ptSet.filter(_.slideID<pt.slideID).map(_.slideID).toArray.distinct.sortWith(_>_); 
    	loop1.breakable{
    		for (x<-slidIdRang){
    		  temprev=temprev+(x->knn(pt,ptSet.filter(_.slideID==x),outlierParam.k-succ));
    			totalNN=totalNN+temprev(x);
    			if (totalNN>=outlierParam.k){
    			  status="Unsafe";
    			  checkOnLeave=x;
    			  loop1.break;
    			}
    		}
    	}
    } else{      
      checkOnLeave=temprev.map(_._1).min;
      status = succ+temprev.size match {
        case y if y>=outlierParam.k =>"Unsafe"
        case default =>"Outlier"
      }
    }    
    ptEvidence=ptEvidence+(pt.id->Evidence(succ,lastSlidId,temprev,checkOnLeave,status));        
  }  
  
  def knn(pt:LeapPtInfo,left:Array[LeapPtInfo],k:Int):Int={
		  var nn=0;   
		  val loop1 = new Breaks;
		  loop1.breakable{
			  for (x<-left if x.id!=pt.id ){      
				  var dist1=eucDistance(pt.content,x.content);
				  if (dist1<=outlierParam.R) {
					  nn=nn+1;
				  }
				  if (nn>=k){
				    loop1.break;
				  }
			  }
		  }
		  return nn;
  }
}