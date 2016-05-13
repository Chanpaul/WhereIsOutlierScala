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
//import org.github.jamm

//case class Slide(slidId:Int,element:Seq[String],expTriger:Seq[String]);
case class Evidence(succ:Int,lastSlid:Int,prev:Map[Int,Int],checkOnLeave:Int,status:String);
case class LeapPtInfo(id:String,slideID:Int,startTime:Double,content:Array[Double]);

class leap extends util{
  private var window=Window(12.0,1);  //width=12.0,slide=1;	  
  private var outlierParam=OutlierParam("ThreshOutlier",12.62,6); 
  private var distMap=scala.collection.mutable.Map[String,Double]();   //global variable
  private var ptInWindow=Map[String,LeapPtInfo]();
  private var ptEvidence=Map[String,Evidence]();
  
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
    
    //distMap=distMap+(id+","+id->0.0);   //distance matrix;
    //var tPtInfo=LeapPtInfo(id,1,1.0,df.first);      
    //ptInWindow=ptInWindow+(id->tPtInfo);  //can also use tuple like (id,df.first)    
	  var ptCount=0;	
    var id="1";
	  //var outliers=Seq("none");
	  var slideUnit=Map[String,LeapPtInfo]();
	  var slideId=1;
	  val writer = new PrintWriter(new File(resDataDir+"//"+resDataFileName));
	  var lines=scala.io.Source.fromFile(dataFile).getLines;
	  var mt=new mtree.mtree;
	  mt.initialization(500,colName,colType,colTypeI,used);
	  //initialize profile;
	  var begTime=System.nanoTime;
	  //var meter=new MemoryMeter;
	  var runtime=Runtime.getRuntime();
	  var mem1=runtime.freeMemory();
	  
	  val pattern=new Regex("^[\\s]+\n");
	  
	  for(line<-lines if (pattern.findAllIn(line).isEmpty)) {
	    //var test=pattern.findAllIn(line).isEmpty;
	    if (slideUnit.size==window.slideLen){	  
	      var expSlidID=0;
	    	if (ptInWindow.size>=window.width){
	    		expSlidID=ptInWindow.map(_._2.slideID).min;
	    		var expPt=ptInWindow.seq.filter(_._2.slideID==expSlidID);
	    		ptInWindow=ptInWindow.filter(_._2.slideID!=expSlidID);
	    		ptEvidence=ptEvidence.filter(x=>ptInWindow.contains(x._1));
	    		for (expIt<-expPt.iterator){				    
	    			mt.delete(expIt._1);
	    		}
	    		var mem2=runtime.freeMemory();
	    		var cpuUsage=(System.nanoTime-begTime)/1000000000.0;
	    		var memUsage=math.abs(mem1-mem2)/(1024*1024);    
	    		var from=ptInWindow.map(_._2.startTime).min.toString;
	    		var to=ptInWindow.map(_._2.startTime).max.toString;
	    		printOutlier(writer,from,to,memUsage,cpuUsage);
	    		//reset profile;
	    		begTime=System.nanoTime;
	    		runtime=Runtime.getRuntime();
	    		mem1=runtime.freeMemory();
	    	}
	      
	      ptInWindow=ptInWindow++slideUnit;
	      slideUnit=slideUnit.empty;
	      slideId=slideId+1;	      
	      
	      thresh(mt,expSlidID); 
	    }
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
			    })//.drop(1);		  	
		    if (ptCount==1){
		    	mt.create(id,curPt);  //creating m-tree  
		    } else if (ptCount!=0) {
		    	mt.insert(id,curPt)(mt.mtNdMap(mt.rootNd.id));
		    }	
	    var tPtInfo=LeapPtInfo(id,slideId,ptCount.toDouble,curPt);	  	    
	    slideUnit=slideUnit+(id->tPtInfo);
		  }
	    ptCount=ptCount+1;
	    
	  }
	  writer.close;
  }
  def thresh(mt:mtree,expSlidID:Int){
    var newSlidID=ptInWindow.map(_._2.slideID).max;   
        
    for(it<-ptInWindow.filter(_._2.slideID==newSlidID).iterator if(!ptEvidence.exists(_._1==it._1))){
    	{
    		var curPtEvi=leapThresh(it._1,ptInWindow,ptEvidence,mt); 
    		ptEvidence=ptEvidence+(it._1->curPtEvi);
    	}
    }
    
    for(it<-ptEvidence.filter(_._2.checkOnLeave==expSlidID).iterator if(expSlidID!=0)){
      ptEvidence=ptEvidence-it._1;  
      var tempEvi=Evidence(it._2.succ,it._2.lastSlid,it._2.prev-expSlidID,it._2.checkOnLeave,it._2.status);
      ptEvidence=ptEvidence+(it._1->tempEvi);
      var newEvi=leapThresh(it._1,ptInWindow,ptEvidence,mt);
    }    
   
  }
  
  def leapThresh(ptId:String,ptInWindow:Map[String,LeapPtInfo],ptEvidence:Map[String,Evidence],mt:mtree):Evidence={
    var temprev=Map[Int,Int]();
    var checkOnLeave=0;
    var status="Outlier";
    var succ=0;
    var lastSlid=ptInWindow(ptId).slideID;
    var tempSuccNeig=Array[String]();
    if (ptEvidence.contains(ptId)){
    	temprev=ptEvidence(ptId).prev;
    	checkOnLeave=ptEvidence(ptId).checkOnLeave;
    	status=ptEvidence(ptId).status;
    	succ=ptEvidence(ptId).succ;
    	lastSlid=ptEvidence(ptId).lastSlid;
    }
    
    var tempNeig=mt.query(ptId,outlierParam.R)(mt.mtNdMap(mt.rootNd.id));
    var tempID=ptInWindow(ptId).slideID;    
    var subWindowItem=ptInWindow.seq.filter(_._2.slideID>=tempID).map(_._1).toArray;
    
    succ=tempNeig.intersect(subWindowItem).length;
    if (succ>=outlierParam.k){
      status="Safe";
    }  
     
    if (status!="Safe"){
      var preWindow=ptInWindow.seq.filter(_._2.slideID<tempID);  
      val loop1 = new Breaks;
    	loop1.breakable{
    		for (i<-1 to (tempID-1)){    			
    			if (!temprev.exists(_._1==tempID-i)){    			  
    			  var subWindowItem1=preWindow.filter(_._2.slideID==tempID-i).map(_._1).toArray;    				
    				var numOfNeig=tempNeig.intersect(subWindowItem1).length;    				
    				temprev=temprev+(tempID-i->numOfNeig);
    			}
    			if (succ+temprev.map(_._2).sum>=outlierParam.k){
    			  status="Unsafe";
    			  checkOnLeave=tempID-i;    			  
    			  loop1.break;
    			}
    		}
    	}
    }
    
    /*
     * var tempID=ptInWindow(ptId).slideID;    
    var subWindowIt=ptInWindow.filter(_._2.slideID>lastSlid).iterator;  //lastSlid: the mostly recent slide checked;  
    lastSlid=subWindowIt.map(_._2.slideID).toList.max;   //update the lastSlid
     val loop = new Breaks;
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
    				//return(Evidence(succ,lastSlid,temprev,checkOnLeave,status));
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
    			  //return(Evidence(succ,lastSlid,temprev,checkOnLeave,status));
    			  loop1.break;
    			}
    		}
    	}

    }*/
    return(Evidence(succ,lastSlid,temprev,checkOnLeave,status));
  }
  
}