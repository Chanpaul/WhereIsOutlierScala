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


case class Evidence(succ:Int,lastSlid:Int,prev:Map[Int,Int],status:String);
case class LeapPtInfo(id:String,slideID:Int,startTime:Double,content:Array[Double]);
case class Slide(id:Int,eleMx:DenseMatrix[Double],expTriger:Array[Tuple2[String,Int]],
    ptMap:scala.collection.mutable.Map[String,Tuple2[LeapPtInfo,Evidence]]);

class leap extends util{
  private var window=Window(12.0,1);  //width=12.0,slide=1;	  
  private var outlierParam=OutlierParam("ThreshOutlier",12.62,6); 
     
  //var mt=new mtree.mtree;
  implicit var colName=Array[String]();
  implicit var columeAttr=Array[(String,String,String,String)]();  //name, attribute type, data type,label (used, not used, label)
	
	private var srcDataDir="";
	private var srcDataFileName="";
	var srcMiddle="";
	private var resDataDir="";
	private var resDataFileName="";
	
	var slides=scala.collection.mutable.Map[Int,Slide]();   //slide id slide, data item matrix
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
    
    var colType=config.getString("dataattr.type").split(" ").map(_.drop(1).dropRight(1).split(",")).map(x=>(x(0),x(1),x(2)));
    var label=config.getString("dataattr.label");
    var notUsed=config.getString("dataattr.notUsed").split(",");
    var tempUsed=config.getString("dataattr.used");
    var used=tempUsed match{
      case " "=> colType.map(_._1).filter(x=>notUsed.contains(x)==false & label!=x)
      case default =>tempUsed.split(",")
    };    
    for (cols <- colType){
      if (used.contains(cols._1)) {
        columeAttr=columeAttr:+(cols._1,cols._2,cols._3,"used")
      } else if (notUsed.contains(cols._1)){
        columeAttr=columeAttr:+(cols._1,cols._2,cols._3,"unUsed")
      } else if (label==cols._1){
        columeAttr=columeAttr:+(cols._1,cols._2,cols._3,"label")
      }  else {
        columeAttr=columeAttr:+(cols._1,cols._2,cols._3,"unUsed")
      }    
    }
  }
  
  def printOutlier(writer:PrintWriter,from:String,to:String,memUsage:Double,cpuUsage:Double){
    var outliers=Array[Array[Double]]();
    slides.foreach(x=>outliers=outliers++:x._2.ptMap.filter(_._2._2.status=="Outlier").map(_._2._1.content).toArray);
    
    var msg=s"From $from to $to, the outliers are:\n";
    var notUsed=columeAttr.filter(x=>x._4=="unUsed").map(_._1)
    for (otly<-outliers){      
      var tempMsg="----------"
      for (x<-notUsed){
        tempMsg=tempMsg+otly.apply(colName.indexOf(x))+"-";
      }
      msg=msg+tempMsg+"\n";
    }
    msg=msg+s"memory usage is: $memUsage,"+s"cpu usage is: $cpuUsage"+"\n************************************\n";
    writer.write(msg);    
  }
  def printAll(writer:PrintWriter,from:String,to:String,memUsage:Double,cpuUsage:Double){
	  var msg=s"From $from to $to:\n";
	  var notUsed=columeAttr.filter(x=>x._4=="unUsed").map(_._1);
	  for (s <-slides){
	    var tempMsg="----------";
	    for (pt<-s._2.ptMap){
	      notUsed.foreach(xx=>tempMsg=tempMsg+pt._2._1.content.apply(colName.indexOf(xx))+"-");	      
	      tempMsg=tempMsg+pt._2._2.status+"-"+(pt._2._2.succ+pt._2._2.prev.foldLeft(0)(_+_._2))+"\n";
	    }   		  		      	
		  msg=msg+tempMsg;
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
    
	  var slideUnit=scala.collection.mutable.Map[String,Tuple2[LeapPtInfo,Evidence]]();
	  var slideId=1;
	  val writer = new PrintWriter(new File(resDataDir+"//"+resDataFileName));
	  var lines=scala.io.Source.fromFile(dataFile).getLines;
	  //var mt=new emtree.emtree;
	  //mt.initialization(10,colName,columeAttr);
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
			  colName=line.split(",").map(_.trim)//.filter(_!="ID");
		  } else{
		    curPt=line.split(",").map(_.trim).map(x=>{x match {
		    case y if x.contains(".") =>x.toDouble
		    case z if !x.contains(".") =>x.toInt.toDouble
		    }
		    });			    
		    var tPtInfo=LeapPtInfo(id,slideId,ptCount.toDouble,curPt);	  	    
		    slideUnit=slideUnit+(id->Tuple2(tPtInfo,Evidence(0,0,Map[Int,Int](),"Outlier")));		    
		  }
	    ptCount=ptCount+1;
	    
	    if (slideUnit.size==window.slideLen){	      
	      	      
	      var expSlidID=0;
	      var expTrigered=Array[Tuple2[String,Int]]();
	      var curWindowSz=slideUnit.size;
	      slides.foreach(x=>curWindowSz=curWindowSz+x._2.ptMap.size)
	      if (curWindowSz>window.width){
	    	  var from=slides.minBy(_._1)._2.ptMap.minBy(_._2._1.startTime)._2._1.startTime.toString;
	    	  var to=slides.maxBy(_._1)._2.ptMap.maxBy(_._2._1.startTime)._2._1.startTime.toString;
	        var curFreeMem=runtime.freeMemory();
	    		var cpuUsage=(System.nanoTime-begTime)/1000000000.0;
	    		var memUsage=math.abs(totalMem-curFreeMem)/(1024*1024);  	    		
	    		printAll(writer,from,to,memUsage,cpuUsage);	    		
	    		expSlidID=slides.minBy(_._1)._1;
	    			 
	    		expTrigered=slides.apply(expSlidID).expTriger;
	    		slides=slides-expSlidID;
	    		//printOutlier(writer,from,to,memUsage,cpuUsage);
	    		//reset profile;
	    		begTime=System.nanoTime;
	      }
	      var tempSlideMx=DenseMatrix.zeros[Double](1,colName.length);      //record data item of each slide.
	      
	      slideUnit.foreach(s=>{tempSlideMx=DenseMatrix.vertcat(tempSlideMx,DenseMatrix(s._2._1.content.toSeq))});	      
	      slides=slides+(slideId->Slide(slideId,tempSlideMx(1 to -1,::),Array[Tuple2[String,Int]](),slideUnit));
	      
	    	
	     
	      if (expTrigered.isEmpty==false){
	    	  for (p<-expTrigered){
	    		  var tempPt=slides.apply(p._2).ptMap(p._1);
	    		  var tempEvidence=tempPt._2;
	    		  var tempPtInfo=tempPt._1;
	    		  
	    		  var lastSlidId=tempEvidence.lastSlid;	    		  
	    		  
	    		  thresh(tempPt,slides.filter(_._1>lastSlidId));
	    		    
	    	  }  
	      }
	      
	      slideUnit.foreach(s=>thresh(s._2,slides));
	      
	      slideUnit=slideUnit.empty;
	      slideId=slideId+1;	    	    		            	       
	    }	    	    
	  }
	  writer.close;
  }
  
  def thresh(pt:Tuple2[LeapPtInfo,Evidence],slidSet:scala.collection.mutable.Map[Int,Slide]){
	  var succ=pt._2.succ;
	  var temprev=pt._2.prev;
	  var status=pt._2.status;	  
	  var lastSlidId=pt._2.lastSlid;
	  var checkOnLeave=0;    
    
	  succ=succ+knn(pt._1.content,slidSet.filter(_._1>=lastSlidId),outlierParam.k-succ);
    lastSlidId=slidSet.maxBy(_._1)._1;
    if (succ>=outlierParam.k){
      status="Safe";
      temprev=Map[Int,Int]();      
    } else {
    	if (temprev.isEmpty){  
    		val loop1 = new Breaks;
    		var totalNN=succ; 
    		
    		var slidIdRang=slidSet.filter(_._1<pt._1.slideID).map(_._1).toArray.distinct.sortWith(_>_);
    		loop1.breakable{
    			for (x<-slidIdRang){
    				
    				temprev=temprev+(x->knn(pt._1.content,slidSet.filter(_._1==x),outlierParam.k-succ));
    				totalNN=totalNN+temprev(x);
    				if (totalNN>=outlierParam.k){    					
    					loop1.break;
    				} 
    			}
    		}    		
    	}
    	
    	if (temprev.foldLeft(succ)((a1,a2)=>a1+a2._2)>=outlierParam.k){
    		status="Unsafe";    		
    		checkOnLeave=temprev.minBy(_._1)._1;
    		var tempSlide=slides.apply(checkOnLeave);
    		slides=slides-checkOnLeave+(checkOnLeave->Slide(tempSlide.id,tempSlide.eleMx,
    				tempSlide.expTriger:+Tuple2(pt._1.id,pt._1.slideID),tempSlide.ptMap));  
    	} else {
    			status="Outlier";    			 
    	} 
    	  	
    }    
    var tempSlide2=slides.apply(pt._1.slideID)
    slides=slides-pt._1.slideID+(pt._1.slideID->Slide(pt._1.slideID,tempSlide2.eleMx,
          tempSlide2.expTriger,tempSlide2.ptMap-pt._1.id+(pt._1.id->Tuple2(pt._1,Evidence(succ,lastSlidId,temprev,status)))));
    
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
  def knn(pt:Array[Double],left:scala.collection.mutable.Map[Int,Slide],k:Int):Int={
    var ptDenseVector=new DenseVector(pt);
    var nn=0;   
		  val loop1 = new Breaks;
		  loop1.breakable{
			  for (x<-left){      
				  var distVec=eucDistance(ptDenseVector,x._2.eleMx);
				  nn=nn+breeze.linalg.sum(distVec.map(x=>x match {case z if z<=outlierParam.R =>1  case _ => 0}))				  
				  if (nn>=k){
				    loop1.break;
				  }
			  }
		  }
		  return nn;
  }
}