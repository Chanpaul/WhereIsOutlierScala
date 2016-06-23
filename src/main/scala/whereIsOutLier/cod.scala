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
case class CodMeta(preNeig:Seq[String],succ:Int,checkOnLeave:Double,status:String);
case class CodPt(id:String,startTime:Double,expTime:Double,content:Array[Double]);
case class CodSlide(id:Int,eleMx:DenseMatrix[Double],expTriger:Array[Tuple2[String,Int]],
    ptMap:scala.collection.mutable.Map[String,Tuple2[LeapPtInfo,Evidence]]);
//case class Event(time:Double,id:String);
class cod extends util{ 
  var window=Window(12.0,1);  //width=12.0,slide=1;	  
  var outlierParam=OutlierParam("ThreshOutlier",12.62,6);
  private var distMap=scala.collection.mutable.Map[String,Double]();
  private var ptInWindow=scala.collection.mutable.Map[String,CodPt]();
  private var ptMeta=scala.collection.mutable.Map[String,CodMeta]();
  private var curWindowStart=0;
  implicit var colName=Array[String]();
  implicit var columeAttr=Array[(String,String,String,String)]();  //name, attribute type, data type,label (used, not used, label)
  
	
	var mt=new mtree.mtree;	
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
    var label=config.getString("dataattr.label");
    var colType=config.getString("dataattr.type").split(" ").map(_.drop(1).dropRight(1).split(",")).map(x=>(x(0),x(1),x(2)));
    //var colType=config.getString("dataattr.type").split(" ").map(_.drop(1).dropRight(1).split(",")).map(x=>(x(0).trim,x(1).toInt));
    var notUsed=config.getString("dataattr.notUsed").split(",").map(_.toInt);
    var tempUsed=config.getString("dataattr.used");
    var used=tempUsed match{
      case " "=> (0 to (colType.length-1)).toArray.filter(x=>(!notUsed.contains(x)))
      case default =>tempUsed.split(",").map(_.toInt)
    };
    for (cols <- colType){
      if (used.contains(cols._1)) {
        columeAttr=columeAttr:+(cols._1,cols._2,cols._3,"used")
      } else if (notUsed.contains(cols._1)){
        columeAttr=columeAttr:+(cols._1,cols._2,cols._3,"unUsed")
      } else if (label==cols._1){
        columeAttr=columeAttr:+(cols._1,cols._2,cols._3,"label")
      }      
    }
  }
  def depart():scala.collection.mutable.Map[String,CodPt]={
		  curWindowStart=curWindowStart+window.slideLen;
		  var expired=ptInWindow.filter(_._2.startTime<=curWindowStart);		
		  
		  for (expIt<-expired.iterator){
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
		  return(expired.seq);
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
	  	 
    //var firstDataItem=df.first;
    //var id="1";
    //distMap=distMap+(id+","+id->0.0);   //distance matrix;     
    curWindowStart=1;
    //ptInWindow=ptInWindow+(id->CodPt(id,1.0,1.0+window.width,df.first));  //can also use tuple like (id,df.first)  
    //ptMeta=ptMeta+(id->CodMeta(Seq[String](),0,0.0,"Outlier"));
	  var ptCount=0;		  
	  var id=ptCount.toString;
	  var lines=scala.io.Source.fromFile(dataFile).getLines;
	  /****************setup metrics measure***********************/
	  var begTime=System.nanoTime;
	  //var meter=new MemoryMeter;
	  var runtime=Runtime.getRuntime();
	  var totalMem=runtime.totalMemory();
	  /******************end of setup*********************/
	  //var mt=new emtree.emtree;
	  mt.initialization(10,colName,columeAttr);
	  val pattern=new Regex("^[\\s]+\n");
	  for(line<-lines if (pattern.findAllIn(line).isEmpty)) {
		  println(ptCount);
		  
		  id=ptCount.toString;
		  var curPt=Array[Double]();
		  if (ptCount==0){
			  colName=line.split(",").map(_.trim).filter(_!="ID");
		  } else {
			  curPt=line.split(",").map(_.trim).map(x=>{x match {
			    case y if x.contains(".") =>x.toDouble
			    case z if !x.contains(".") =>x.toInt.toDouble
			  }
			    })//.drop(1);
			  //var curPt=sqlContext.sql("""SELECT * FROM df WHERE ID==ptCount""").first;  //result of sql queries is dataframe;
			  //var curPt=df.filter(s"ID =$ptCount").first;  
			  //var curPt= df.head(ptCount).last;	
			  //println(curPt);
			  //var ptLen=curPt.length;		  			  
			  
			  if (ptInWindow.size+1>window.width){
			    //println(line);
				  /************collect metrics***********************/
				  var curFreeMem=runtime.freeMemory();
				  var cpuUsage=(System.nanoTime-begTime)/1000000000.0;
				  var memUsage=math.abs(totalMem-curFreeMem)/(1024*1024);
				  var from=ptInWindow.map(_._2.startTime).min.toString;
				  var to=ptInWindow.map(_._2.startTime).max.toString;
				  printAll(writer,from,to,memUsage,cpuUsage);
				  //printOutlier(writer,from,to,memUsage,cpuUsage);
				  /*********************end of collection*********************/				  
				  var expired=depart();
				  for (expIt<-expired.iterator){				    
				    mt.delete(expIt._1,expIt._2.content);
				  }
				  /****************renew metrics measure***********************/
				  begTime=System.nanoTime;				  
				 /******************end of setup*********************/
			  }
			  
			  ptInWindow=ptInWindow+(id->CodPt(id,ptCount,ptCount+window.width,curPt));	 
			  if (id=="1"){
				  mt.create(id,curPt);  //creating m-tree  
			  } else if (id!="0") {
			     mt.insert(id,curPt);
			  }
			  
			  if(!ptMeta.exists(_._1==id)){
				  ptMeta=ptMeta+(id->CodMeta(Seq[String](),0,0.0,"Outlier"));
			  }			    
			  searchNeighbor(id);	 
			  
		  }
		  ptCount+=1;
	  }   
	  writer.close;
  }  
  
  def printOutlier(writer:PrintWriter,from:String,to:String,memUsage:Double,cpuUsage:Double){        
    var outliers=ptMeta.filter(_._2.status=="Outlier").map(_._1).map(x=>ptInWindow(x).content);
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
	  for (pt <-ptMeta){    	      
		  var tempMsg="----------";
		  for (x<-notUsed){
			  tempMsg=tempMsg+ptInWindow(pt._1).content.apply(colName.indexOf(x))+"-";
		  }    	
		  msg=msg+tempMsg+pt._2.status+"-"+(pt._2.succ+pt._2.preNeig.size)+"\n";
		  //println(msg);
	  }
	  msg=msg+s"memory usage is: $memUsage,"+s"cpu usage is: $cpuUsage"+"\n************************************\n";
	  writer.write(msg);     
  }
  def searchNeighbor(id:String){    
	    var strPat=new Regex(id);
	    var idCheckTime=ptInWindow(id).expTime;	 
	    var tempPreNeig=ptMeta(id).preNeig;
	    var succ=ptMeta(id).succ;
	    var checkOnLeave=ptMeta(id).checkOnLeave;
	    var status=ptMeta(id).status;	    
	    
	    tempPreNeig=mt.query(id,ptInWindow(id).content,outlierParam.R);
	    
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