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
/***************data structure*******************
 * distMap: Map(id1,id2->distance);
 * ptInfoInWindow:  Map(id->DataPoint)
 *ptInWindow: Map(id->dataAttributeItem), dataAttributeItem is row data structure;
 *  
 */
 
 


case class DataItem(year:String,month:String,day:String,hour:String,
		Z:Double,Y:Double,X:Double,celsius:Double,eda:Double);
case class Window(width:Double,slide:Int);
case class PtInfo(id:String,preNeig:Seq[String],succNeigNum:Int,startTime:Double,expTime:Double);
case class Event(time:Double,id:String);
case class OutlierParam(R:Double,k:Int);    //R radius, k number of neighbors;

object outlierDetection {
  
  def main(args:Array[String]){
	  val dataDir="C://Users//wangc//DataSet//CleanedData";
	  val window=Window(12.0,1);  //width=12.0,slide=1;
	  val outlierParam=OutlierParam(12.62,6);
	  val conf = new SparkConf().setAppName("WhereIsOutlier")
			  .setMaster("local[2]")
			  //.setJars(Seq("/a/b/x.jar", "/c/d/y.jar"));
	  val sc = new SparkContext(conf);
	  val sqlContext = new org.apache.spark.sql.SQLContext(sc);
	  cod(dataDir,sqlContext,window,outlierParam);
	  //job4Clean(sqlContext);
  }
  def job4Clean(sqlContext:SQLContext){
    val srcDirName="C://Users//wangc//DataSet//CleanedData//temp";
	  var objDir="C://Users//wangc//DataSet//CleanedData";
	  val tempDir=new File(srcDirName);
	  if (tempDir.exists && tempDir.isDirectory){
		  for (curDir<- tempDir.listFiles){
			  var curDirName=curDir.getName;    
			  val srcFile=srcDirName+"//"+curDir.getName;
			  //val objFile=curDir.getName;
			  val objFile=objDir+"//"+curDir.getName;
			  clean(srcFile,objFile,sqlContext); 
		  }

	  }
  }
  def getAttr(typ:String)={
    typ match {
    case "IntegerType" => (r:Row,attrName:String)=>{r.getAs[Int](attrName).toDouble}
    case "DoubleType" => (r:Row,attrName:String)=>{r.getAs[Double](attrName)}   
  }    
  }
  
  def cod(dataDir:String,sqlContext:SQLContext,window:Window,outlierParam:OutlierParam){
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
	  var curWindow=CurWindow(window.width,window.slide,1);
	  
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
  
  def sortAndAgg(srcFile:String,objFile:String,sqlContext:SQLContext)  {    
  
	  val df = sqlContext.read
			  .format("com.databricks.spark.csv")
			  .option("header", "true") // Use first line of all files as header
			  .option("inferSchema", "true") // Automatically infer data types
			  .load(srcFile);
	  val tdf=df.groupBy("year","month","day","hour")
			  .agg(avg(df.col("Celsius")),avg(df.col("Z-axis")),avg(df.col("Y-axis")),avg(df.col("X-axis")),
					  avg(df.col("EDA")));

	  val sortedTdf=tdf.orderBy("year","month","day","hour").save(objFile,"com.databricks.spark.csv");   

  }
  def cleanSingleFile(dataDir:String,objDir:String,objFileName:String) {	  
	  //val dataDir="D://Wangjin//UmassMed//Code//Dataset//cocaine//00001CompletedCSVfiles"
	  val d = new File(dataDir)
	  //var objFileName:String="patient1.csv"
	  val writer = new PrintWriter(new File(objDir+"//"+objFileName))
	  //var startTime:Int=0;
	  var year:String="";
	  var month:String="";
	  var day:String="";
	  var hour:Int=0;
	  var minute:Int=0;
	  var seconds:Double=0;

	  if (d.exists && d.isDirectory) {
		  val tempFiles=d.listFiles.toList
				  var writeHeader:Boolean=true;
		  //println(tempFiles.toString)
		  tempFiles.foreach{tf=>
		  //println(tf)						
		  if (tf.getName.matches("\\d.+\\.csv\\b")){
			  println(tf.toString)
			  val bufferedSource = scala.io.Source.fromFile(tf.toString);
			  var lineCount:BigInt=0;
			  var flag=false;
			  for (line<-bufferedSource.getLines){
				  //println(line)
				  lineCount+=1;
				  if (lineCount==6){
					  //println(line)
					  val pattern1=new Regex("\\d{4}-\\d{2}-\\d{2}");
					  val pattern2=new Regex("\\d{2}:\\d{2}:\\d{2}");
					  val startDate=(pattern1 findAllIn line).mkString(",").split("-");								  
					  year=startDate(0)
							  month=startDate(1)
							  day=startDate(2)
							  //objFileName=objFileName+startDate
							  //println(objFileName);
							  val startTime=(pattern2 findAllIn line).mkString(",").split(":");
					  hour=startTime(0).toInt
							  minute=startTime(1).toInt
							  //startTime=tStartTime.substring(0,2).toInt
							  //println(startTime);									   

				  } else if (lineCount==7 && writeHeader==true){
					  writer.write("year,month,day,hour,minute,seconds,Time,Z-axis,Y-axis,X-axis,Battery,Celsius,EDA,Event"+"\n");
					  writeHeader=false;
				  } else if (lineCount>8){
					  val cols = line.split(",").map(_.trim);
					  val tDate=cols(0).split(":");
					  //println(line)
					  hour=tDate(0).toInt;
					  minute=tDate(1).toInt;
					  seconds=tDate(2).toDouble;					  
					  writer.write(year+","+month+","+day+","+hour+","+minute+","+seconds+","+line+"\n");
				  }
			  }
			  bufferedSource.close   

		  }
		  }		     

	  }
	  writer.close
  }
  def cleanMultiple(srcDir:String,objDir:String,objPrefix:String) {  

	  val srcDirFile=new File(srcDir);
	  if (srcDirFile.exists && srcDirFile.isDirectory) {
		  val tempFiles=srcDirFile.listFiles.toList;
		  for (fileIter<-tempFiles){
			  val tPattern=new Regex("\\d{5}");
			  var subFileName=fileIter.getName;
			  var objFilePattern= tPattern findFirstIn subFileName;
			  if (!objFilePattern.isEmpty){
				  var objFileName=objPrefix + objFilePattern.mkString(",").toInt+".csv";
				  println(objFileName)
				  cleanSingleFile(fileIter.toString,objDir,objFileName);  
			  }			   

		  }
	  }

  } 
   
  
  def clean(srcFile:String,objFile:String,sqlContext:SQLContext){	  
	  val df = sqlContext.read
			  .format("com.databricks.spark.csv")
			  .option("delimiter",",")
			  .option("header", "true") // Use first line of all files as header
			  .option("inferSchema", "true") // Automatically infer data types
			  .load(srcFile);   //"cars.csv"
	  val newDf=df.groupBy("year","month","day","hour")
			  .agg(avg(col("Z-axis")), avg(col("Y-axis")),avg(col("X-axis")),
	      avg(col("Celsius")),avg(col("EDA")),avg(col("Battery")),avg(col("Event")))
	      .sort("year", "month","day","hour");
	  
	  newDf                    
	  .coalesce(1)
	  .write
	  .format("com.databricks.spark.csv")	  
	  .option("header", "true")		  
	  .save(objFile);  //"newcars.csv"
  }
  def myRead[T](srcFile:String,delimiter:String,sqlContext:SQLContext) {
	  import sqlContext.implicits._
	  var tPattern=new Regex("\\.csv");
	  var existOrNot=tPattern findFirstIn srcFile;	  

	  if (!existOrNot.isEmpty){
		  val df = sqlContext.read
				  .format("com.databricks.spark.csv")
				  .option("delimiter",",")
				  .option("header", "true") // Use first line of all files as header
				  .option("inferSchema", "true") // Automatically infer data types
				  .load(srcFile);   //"cars.csv"
	  } else {
		  val dataFromText = sqlContext.read.text(srcFile).as[String]; //String
		  val ds = dataFromText.map(line => {
			  var cols = line.split(","); // parse each line
			  cols;
			  //DataItem(cols(0).toInt, cols(1).toDouble, cols(2).toDouble, 
				//	  cols(3).toDouble, cols(4).toDouble, cols(5).toDouble);
		  });
		  ds.show();
		  println(ds.count); 
	  }    

  }
}



/*
 package outlier.datastream.cod
import org.apache.spark.sql
import org.apache.spark.sql._

import org.apache.spark.{SparkContext, SparkConf}

object cod {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMe Application");
    val sc = new SparkContext(conf);
    val fileName = args(0);
    //val lines = sc.textFile(fileName);
    case class dataItem(mon:String,day:String,hour:String,Z:Double,Y:Double,X:Double, 
        cess:Double, eda:Double);
    val dataItemSet = sqlContext.read.text(fileName).as[String];
    val ds = dataItemSet.
    		map(line => {
    			val cols = line.split(","); // parse each line
    			dataItem(cols(0), cols(1), cols(2), 
    			    cols(3).toDouble, cols(4).toDouble,
    			    cols(5).toDouble,cols(6).toDouble);
    		});
    //ds.filter(_._)
    val firstDataItem=ds.first;
    var leftDs=ds.substract(firstDataItem)
    while (leftDs.count>0) {
      
      
      leftDs=leftDs.substract(firstDataItem)
    }
   
    		

    //val c = lines.count();
    //println(s"There are $c lines in $fileName");
  }
} 
 */