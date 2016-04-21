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

case class DataItem(year:String,month:String,day:String,hour:String,
		Z:Double,Y:Double,X:Double,celsius:Double,eda:Double);
case class Window(width:Double,slide:Int);
case class DataPoint(id:String,preNeig:Seq[String],succNeigNum:Int,expTime:Double);
case class Event(time:Int,id:Int);
case class OutlierParam(R:Double,k:Int);    //R radius, k number of neighbors;

object outlierDetection {
  
  def main(args:Array[String]){
	  val dataDir="C://Users//wangc//DataSet//CleanedData";
	  val window=Window(12.0,1);  //width=12.0,slide=1;
	  val outlierParam=OutlierParam(12.62,6);
	  val conf = new SparkConf().setAppName("WhereIsOutlier")
			  .setMaster("local[2]")
			  .setJars(Seq("/a/b/x.jar", "/c/d/y.jar"));
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
  def cod(dataDir:String,sqlContext:SQLContext,window:Window,outlierParam:OutlierParam){
    import sqlContext.implicits._;
    val dataFile=dataDir+"//Patient1.csv//"+"part-00000";
	  val ds = sqlContext.read.text(dataFile).as[DataItem];     
    var evtQue=Seq(Event(0,0)).toDS;
    var firstDataItem=Seq(ds.first).toDS;
    var id=ds.first.year+ds.first.month+ds.first.day+ds.first.hour;
    var pt=DataPoint(id,Seq(id),0,window.width+0);
    var ptInWindow=Seq(pt).toDS;
    var leftDs=ds.subtract(firstDataItem);
   
    while (leftDs.count>0) {  
      var dataSetForKNN=ptInWindow.select($"Z_axis".as[Double],
          $"Y_axis".as[Double],$"X_axis".as[Double],
          $"celsius".as[Double],$"eda".as[Double]).collect();
      var tempdata= for (i<-0 to (dataSetForKNN.length - 1)){
    	  val di= dataSetForKNN.apply(i);
    	  //yield (di(0),di(1),di(2),di(3),di(4));
    	  //yield(dataSetForKNN.apply(i)(0),dataSetForKNN.apply(i)(1));
    	  };     
      
    	//val t = KDTree.fromSeq();
    	firstDataItem=Seq(ds.first).toDS;
    	leftDs=leftDs.subtract(firstDataItem)
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