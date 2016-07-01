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
import collection.JavaConverters._
import scala.collection.parallel._
import mtree._
import emtree._
import breeze.linalg._
import heap._
case class CodMeta(preNeig:Seq[String],succ:Int,status:String);
case class CodPt(id:String,startTime:Double,expTime:Double,content:DenseVector[Double]);
case class CodSlide(ptMap:scala.collection.mutable.Map[String,Tuple2[CodPt,CodMeta]]);
//case class Event(time:Double,id:String);
class cod extends util{ 
  var window=Window(12.0,1);  //width=12.0,slide=1;	  
  var outlierParam=OutlierParam("ThreshOutlier",12.62,6);
  //private var distMap=scala.collection.mutable.Map[String,Double]();
  //private var ptInWindow=scala.collection.mutable.Map[String,CodPt]();
  //private var ptMeta=scala.collection.mutable.Map[String,CodMeta]();
  private[this] var slides=scala.collection.mutable.Map[Int,CodSlide]();
  private var curWindowStart=0;
  implicit var colName=Array[String]();
  implicit var columeAttr=Array[(String,String,String,String)]();  //name, attribute type, data type,label (used, not used, label)
  
	var ptQueue=new heap.FibonacciHeap[String];
	var evtQueue=new heap.FibonacciHeap[String];
	var mt=new mtree[String];	
	var srcDataDir="";
	var srcDataFileName="";
	var resDataDir="";
	var resDataFileName="";
	var srcMiddle="";
	
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
  def depart(expSlideID:Int):Array[Tuple2[String,DenseVector[Double]]]={
    var expPts=slides(expSlideID).ptMap.map(x=>Tuple2(x._1,x._2._1.expTime)).toArray.sortWith(_._2<_._2)
		  //var expPt=slides(expSlideID).ptMap.map(_._2._1.expTime).toArray.sortWith(_<_);
		  var expired=Array[Tuple2[String,DenseVector[Double]]]();
		  for (t<-expPts){
			  //expired=expired:+Tuple2(t._1,t._2._1.content);			  
			  while(evtQueue.minNode.value==t._2) {
				  var checkPtId=evtQueue.removeMin();	
				  var ptId=resolve(checkPtId);
				  var pt=slides(ptId._1).ptMap(ptId._2)
				  var tempMeta=pt._2;
				  
				  if (tempMeta.succ<outlierParam.k){
					  var status=tempMeta.status;
					  var preNeig=tempMeta.preNeig.diff(Seq(t._1));
					  if (tempMeta.succ+preNeig.length<outlierParam.k) {
						  status="Outlier";
					  } else {
						  var timeStampSet=Array[Double]();
						  preNeig.foreach(x=>timeStampSet=timeStampSet:+slides(resolve(x)._1).ptMap(resolve(x)._2)._1.expTime);
						  if (timeStampSet.isEmpty==false){
						    timeStampSet=timeStampSet.sortWith(_>_);
							  evtQueue.insert(checkPtId,timeStampSet(outlierParam.k-tempMeta.succ-1));  
						  }
					  }
					  var tPtMap=slides(ptId._1).ptMap-ptId._2+(ptId._2->Tuple2(pt._1,CodMeta(preNeig,tempMeta.succ,status)));
					  slides=slides-ptId._1+(ptId._1->CodSlide(tPtMap));  
				  }

			  } 			  
		  }		  
		  return(expired);
  }
  
  def codMain(sqlContext:SQLContext){
    import sqlContext.implicits._;
    val dataFile=srcDataDir+srcMiddle+srcDataFileName;
    /*
    //val ds=sqlContext.read.text(dataFile).as[String].map(_.split(","));    
	  var df = sqlContext.read
				  .format("com.databricks.spark.csv")
				  .option("delimiter",",")
				  .option("header", "true") // Use first line of all files as header
				  .option("inferSchema", "true") // Automatically infer data types
				  .load(dataFile);   //"cars.csv"  
	  
	  colName=df.columns.map(_.trim).filter(_!="ID");
	  colType=df.dtypes.map(x=>(x._1.trim,x._2.trim));	  
	  colTypeI.map(x=>println(x._1,x._2));  
	  var dfCount=df.count;
	  df.registerTempTable("df");     //register df as a temp table in order to use sql clause to lookup;
	  */
	  val writer = new PrintWriter(new File(resDataDir+"//"+resDataFileName));  	 
        
    curWindowStart=1;    
	  var ptCount=0;		  
	  var id=ptCount.toString;
	  var lines=scala.io.Source.fromFile(dataFile).getLines;
	  /****************setup metrics measure***********************/
	  var begTime=System.nanoTime;
	  //var meter=new MemoryMeter;
	  var runtime=Runtime.getRuntime();
	  var totalMem=runtime.totalMemory();
	  /******************end of setup*********************/
	  var slideId=1;
	  var slideUnit=scala.collection.mutable.Map[String,Tuple2[CodPt,CodMeta]]();
	  val pattern=new Regex("^[\\s]+\n");
	  for(line<-lines if (pattern.findAllIn(line).isEmpty)) {
		  println(ptCount);		  
		  id=ptCount.toString;
		  var rawPt=Array[Double]();
		  if (ptCount==0){
			  colName=line.split(",").map(_.trim)//.filter(_!="ID");		
			  mt.init(20,colName,columeAttr);
		  } else {
			  rawPt=line.split(",").map(_.trim).map(x=>{x match {
			    case y if x.contains(".") =>x.toDouble
			    case z if !x.contains(".") =>x.toInt.toDouble
			  }
			    })//.drop(1);
			  //var curPt=sqlContext.sql("""SELECT * FROM df WHERE ID==ptCount""").first;  //result of sql queries is dataframe;
			  //var curPt=df.filter(s"ID =$ptCount").first;  
			  //var curPt= df.head(ptCount).last;	
			  //println(curPt);
			  //var ptLen=curPt.length;		  			  
			  var curWindowSz=slideUnit.size;
	      slides.foreach(x=>curWindowSz=curWindowSz+x._2.ptMap.size)
	       
			  if (curWindowSz>window.width){
			     var expSlidID=slides.minBy(_._1)._1;			    
				  /************collect metrics***********************/
				  var curFreeMem=runtime.freeMemory();
				  var cpuUsage=(System.nanoTime-begTime)/1000000000.0;
				  var memUsage=math.abs(totalMem-curFreeMem)/(1024*1024);
				  var from=slides.minBy(_._1)._2.ptMap.minBy(_._2._1.startTime)._2._1.startTime.toString;
	    	  var to=slides.maxBy(_._1)._2.ptMap.maxBy(_._2._1.startTime)._2._1.startTime.toString;
				  
				  printAll(writer,from,to,memUsage,cpuUsage);
				  //printOutlier(writer,from,to,memUsage,cpuUsage);
				  /*********************end of collection*********************/				  
				  var expired=depart(expSlidID);
				  for (expIt<-expired.iterator){				    
				    var tt=mt.delete(expIt);
				    
				  }
				  
				  slides=slides-expSlidID;
				  slides=slides+(slideId->CodSlide(slideUnit));
				  slideId=slideId+1;
				  /****************renew metrics measure***********************/
				  begTime=System.nanoTime;				  
				 /******************end of setup*********************/
			  }
			  
			  var curPt=DenseVector(rawPt);
			  var ptId=slideId.toString+"_"+id;
			  slideUnit=slideUnit+(ptId->Tuple2(CodPt(id,ptCount,ptCount+window.width,curPt),CodMeta(Seq[String](),0,"Outlier")));
			  //ptInWindow=ptInWindow+(id->CodPt(id,ptCount,ptCount+window.width,curPt));	 
			  if (id!="0") {
				  mt.insert(Tuple2(ptId,curPt));
				  //ptQueue.insert(id,id.toDouble);
			  }		    
			  searchNeighbor(ptId);
		  }
		  ptCount+=1;
	  }   
	  writer.close;
  }  
  def resolve(id:String):Tuple2[Int,String]={
    var temp=id.split("_");
    Tuple2(temp(0).toInt,temp(1))
  }
  def printOutlier(writer:PrintWriter,from:String,to:String,memUsage:Double,cpuUsage:Double){        
    var outliers=Array[Array[Double]]();
    slides.foreach(x=>outliers=outliers++:x._2.ptMap.filter(_._2._2.status=="Outlier").map(_._2._1.content.data).toArray);    
    //ptMeta.filter(_._2.status=="Outlier").map(_._1).map(x=>ptInWindow(x).content);
    var msg=s"From $from to $to, the outliers are:\n";   
    var notUsed=columeAttr.filter(x=>x._4=="unUsed").map(_._1);
    for (otly<-outliers){      
      var tempMsg="----------"
      for (x<-notUsed){
        tempMsg=tempMsg+otly.apply(colName.indexOf(x))+"-";
      }
      msg=msg+tempMsg+"\n";
    }   
    
    msg=msg+s"memory usage is: $memUsage,"+s"cpu usage is: $cpuUsage"+"\n************************************";
    writer.write(msg);    
  }
  def printAll(writer:PrintWriter,from:String,to:String,memUsage:Double,cpuUsage:Double){
	  var msg=s"From $from to $to:\n";
	  var notUsed=columeAttr.filter(x=>x._4=="unUsed").map(_._1);
	  for (s <-slides){
	    var tempMsg="----------";
	    for (pt<-s._2.ptMap){
	      notUsed.foreach(xx=>tempMsg=tempMsg+pt._2._1.content.apply(colName.indexOf(xx))+"-");	      
	      tempMsg=tempMsg+pt._2._2.status+"-"+(pt._2._2.succ+pt._2._2.preNeig.size)+"\n";
	    }   		  		      	
		  msg=msg+tempMsg;
		  //println(msg);
	  }
	  msg=msg+s"memory usage is: $memUsage,"+s"cpu usage is: $cpuUsage"+"\n************************************\n";
	  writer.write(msg);      
  }
  def searchNeighbor(id:String){   
	  	  	    
	  var pt=slides(resolve(id)._1).ptMap(resolve(id)._2);
	  var idCheckTime=pt._1.expTime;	 
	  var tempPreNeig=pt._2.preNeig;
	  var succ=pt._2.succ;	    
	  var status=pt._2.status;
	  tempPreNeig=mt.query(Tuple2(id,pt._1.content),outlierParam.R).map(_._1).diff(Seq(id));	    	 
	  if (tempPreNeig.length>=outlierParam.k){
		  status="Unsafe";
		  var timeStampSet=Array[Double]();
		  tempPreNeig.foreach(x=>timeStampSet=timeStampSet:+slides(resolve(x)._1).ptMap(resolve(x)._2)._1.expTime);
		  timeStampSet=timeStampSet.sortWith(_>_);
		  evtQueue.insert(id,timeStampSet(outlierParam.k-1));	      	      
	  }
	  var tMap=slides(resolve(id)._1).ptMap-resolve(id)._2+(resolve(id)._2->Tuple2(pt._1,CodMeta(tempPreNeig,succ,status)))
	  slides=slides-resolve(id)._1+(resolve(id)._1->CodSlide(tMap))
	  //ptMeta=ptMeta-id+(id->CodMeta(tempPreNeig,succ,status));	 

	  tempPreNeig.foreach(x=>{
	     
		  var tStatus=slides(resolve(x)._1).ptMap(resolve(x)._2)._2.status;
		  var tSucc=slides(resolve(x)._1).ptMap(resolve(x)._2)._2.succ+1;
		  var tempNumNeig=slides(resolve(x)._1).ptMap(resolve(x)._2)._2.preNeig.length+tSucc;
		  var tempPreNeig=slides(resolve(x)._1).ptMap(resolve(x)._2)._2.preNeig;
		  var timeStampSet=Array[Double]();
		  /*
		  tempPreNeig.foreach(y=>	ptInWindow.contains(y) match {
			  case true=>timeStampSet=timeStampSet:+ptInWindow(y).expTime
			  case false=>tempPreNeig=tempPreNeig.diff(Seq(y))
			  })*/
		  if (tStatus=="Outlier" && tempPreNeig.length+tSucc>=outlierParam.k){
			  tStatus="Unsafe";
			  if (timeStampSet.isEmpty==false && tSucc<outlierParam.k){
			    timeStampSet=timeStampSet.sortWith(_>_)			    
				  evtQueue.insert(x,timeStampSet(outlierParam.k-tSucc-1));  
			  }			  
		  } 		        
		  var tcodMeta=CodMeta(tempPreNeig,tSucc,tStatus);
		  //ptMeta=ptMeta-x+(x->tcodMeta);
	  })  

	  	    	    	  
  }

}