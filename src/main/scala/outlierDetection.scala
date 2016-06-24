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

import whereIsOutLier._

/***************data structure*******************
 * distMap: Map(id1,id2->distance);
 * ptInfoInWindow:  Map(id->DataPoint)
 *ptInWindow: Map(id->dataAttributeItem), dataAttributeItem is row data structure;
 *  
 */
 
case class DataItem(year:String,month:String,day:String,hour:String,
		Z:Double,Y:Double,X:Double,celsius:Double,eda:Double);
case class TimeUnit(quantity:Int,unite:String);
//case class OutlierParam(R:Double,k:Int);    //R radius, k number of neighbors;

object outlierDetection {
  
  def main(args:Array[String]){   //outlierdetectionmain
	  //val dataDir="C://Users//wangc//DataSet//CleanedData";
	  //val window=Window(12.0,1);  //width=12.0,slide=1;  
	  
	  //val outlierParam=OutlierParam("ThreshOutlier",12.62,6);
	  val conf = new SparkConf().setAppName("WhereIsOutlier")
			  .setMaster("local[2]")
			  //.setJars(Seq("/a/b/x.jar", "/c/d/y.jar"));
	  val sc = new SparkContext(conf);
	  val sqlContext = new org.apache.spark.sql.SQLContext(sc);
	  var confFileName="ForestCover.conf";
	  //var confFileName="application.conf";
	  var confDir="C://Users//wangc//workspace//WhereIsOutlierScala//src//main//resource//";
	  val myConfigFile = new File(confDir+confFileName);
	  
	  val fileConfig = ConfigFactory.parseFile(myConfigFile);
	  val config = ConfigFactory.load(fileConfig);
	  
	  for (slidSz<-Array(0.5,1,5,10,25,50).map(_*1000)){
	    /*
	    var newConfigLeap = config.withValue("win.slideLen", ConfigValueFactory.fromAnyRef(slidSz))
				  .withValue("win.width", ConfigValueFactory.fromAnyRef(100000))   //100000
				  .withValue("outlier.fileName", ConfigValueFactory.fromAnyRef("leap_slideSize_"+slidSz));
		  var tleap=new leap;
		  
		  println(s"Running slide size $slidSz");
		  tleap.setConfig(newConfigLeap);
		  tleap.leapMain(sqlContext);
		  */
		  
		  var newConfigCod = config.withValue("win.slideLen", ConfigValueFactory.fromAnyRef(slidSz))
				  .withValue("win.width", ConfigValueFactory.fromAnyRef(100000))
				  .withValue("outlier.fileName", ConfigValueFactory.fromAnyRef("cod_slideSize_"+slidSz));
		  var tcod=new cod;
		  tcod.setConfig(newConfigCod);
		  tcod.codMain(sqlContext);
	  }	   
	  
	  for (windSz<-Array(3,4,5,6).map(_*10000)){	  
		  var newConfigLeap = config.withValue("win.slideLen", ConfigValueFactory.fromAnyRef(10000))
				  .withValue("win.width", ConfigValueFactory.fromAnyRef(windSz))
				  .withValue("outlier.fileName", ConfigValueFactory.fromAnyRef("leap_windowSize_"+windSz));		  
		  println(s"Running window size $windSz");
		  var tleap=new leap;
		  tleap.setConfig(newConfigLeap);
		  tleap.leapMain(sqlContext);	
		  
		  var newConfigCod = config.withValue("win.slideLen", ConfigValueFactory.fromAnyRef(10000))
				  .withValue("win.width", ConfigValueFactory.fromAnyRef(windSz))
				  .withValue("outlier.fileName", ConfigValueFactory.fromAnyRef("cod_windowSize_"+windSz));
		  var tcod=new cod;
		  tcod.setConfig(newConfigCod);
		  tcod.codMain(sqlContext);
	    }	  
	  /*
	  var timeUnit=Array("1second","30second","1minute","10minute","30minute","1hour");
	  var unitPattern=new Regex("[a-zA-Z]+");
	  var objDir="C://Users//wangc//Results//WhereIsOutlierScala";
	  var winRange=3 to 24;
	  var outlierR=1 to 12;	 
	  for (tu<-timeUnit){	    
	    var tempObjDir=new File(objDir+"//"+tu)
	    if (tempObjDir.exists==false){
	      var tempFlag=tempObjDir.mkdir();
	      tempFlag match {
	      case false => println("directory was created successfully")
	      case true => println(s"failed trying to create the directory:$tempObjDir.getFileName")
	      }
	    }
		  for (i<-1 to 15){		  
			  for (windSz<-winRange){
				  for (r<-outlierR;k<-2 to windSz){
					  var newConfigLeap = config.withValue("win.slideLen", ConfigValueFactory.fromAnyRef(1))
							  .withValue("win.width", ConfigValueFactory.fromAnyRef(windSz))
							  .withValue("outlier.R", ConfigValueFactory.fromAnyRef(r))
							  .withValue("outlier.k", ConfigValueFactory.fromAnyRef(k))
							  .withValue("outlier.directory", ConfigValueFactory.fromAnyRef(objDir+"//"+tu))
							  .withValue("outlier.fileName", ConfigValueFactory.fromAnyRef(s"patient$i"+s"r_$r"+s"k_$k"+"leap_windowSize_"+windSz))
							  .withValue("dataset.middle",ConfigValueFactory.fromAnyRef("//"+tu+"//Patient"+i+".csv//"));		
					  unitPattern.findFirstIn(tu).getOrElse(" ") match {
					    case "second" => newConfigLeap.withValue("dataattr.notUsed",ConfigValueFactory.fromAnyRef("year,month,day,hour,minute,second"))
					    		.withValue("dataattr.used",ConfigValueFactory.fromAnyRef("Z_axis,Y_axis,X_axis,Celsius,EDA"));
					    case "minute" => newConfigLeap.withValue("dataattr.notUsed",ConfigValueFactory.fromAnyRef("year,month,day,hour,minute"))
					    		.withValue("dataattr.used",ConfigValueFactory.fromAnyRef("Z_axis,Y_axis,X_axis,Celsius,EDA"));
					    case "hour" => newConfigLeap.withValue("dataattr.notUsed",ConfigValueFactory.fromAnyRef("year,month,day,hour"))
					    		.withValue("dataattr.used",ConfigValueFactory.fromAnyRef("Z_axis,Y_axis,X_axis,Celsius,EDA"))
					  }
					  println(s"Running window size $windSz");
					  var outlierDetect=new leap;
					  outlierDetect.setConfig(newConfigLeap);
					  outlierDetect.leapMain(sqlContext);	
				  }
			  }  
		  }
	  }*/
  }
  def cleanBioSensorMain(args:Array[String]){      //cleanBioSensorMain
    val conf = new SparkConf().setAppName("WhereIsOutlier")
			  .setMaster("local[2]")			  
	  val sc = new SparkContext(conf);
	  val sqlContext = new org.apache.spark.sql.SQLContext(sc);
	  /*
	   * //preliminary integrate
	  var srcDir="C://Users//wangc//DataSet//BoyerData//";
	  var objDir="C://Users//wangc//DataSet//CleanedData//"
	  var objPrefix="Patient";
	  cleanMultiple(srcDir,objDir,objPrefix);
	  * */
	  //aggregate and sort
	  var timeUnit=Array(TimeUnit(1,"second"),TimeUnit(30,"second"),TimeUnit(1,"minute"),
	      TimeUnit(10,"minute"),TimeUnit(30,"minute"),TimeUnit(1,"hour"));    
	  for (tu<-timeUnit){
		  job4Clean(sqlContext,tu);  
	  }	  
  }
  
  def job4Clean(sqlContext:SQLContext,timeUnit:TimeUnit){
	  val srcDirName="C://Users//wangc//DataSet//CleanedData//temp";
	  var objDir="C://Users//wangc//DataSet//CleanedData"+"//"+timeUnit.quantity+timeUnit.unite;
	  var tempObjDir=new File(objDir);
	  if (tempObjDir.exists==false){
		  var tempFlag=tempObjDir.mkdir();
		  tempFlag match {
		  case false => println("directory was created successfully")
		  case true =>println(s"failed trying to create the directory:$objDir")
		  };

	  }
	  val tempDir=new File(srcDirName);
	  if (tempDir.exists && tempDir.isDirectory){
		  val tpattern1=new Regex("ReadMe");					  
		  for (curDir<- tempDir.listFiles){
			  var curDirName=curDir.getName;
			  val tFileFlag=tpattern1.findFirstIn(curDirName);
			  if (tFileFlag.isEmpty==true){
				  val srcFile=srcDirName+"//"+ curDirName;  //curDir.getName;			  
				  val objFile=objDir+"//"+curDirName;    //curDir.getName;
				  clean(srcFile,objFile,sqlContext,timeUnit);  
			  }
		  }
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
   
  
  def clean(srcFile:String,objFile:String,sqlContext:SQLContext,timeUnit:TimeUnit){    
	  var df = sqlContext.read
			  .format("com.databricks.spark.csv")
			  .option("delimiter",",")
			  .option("header", "true") // Use first line of all files as header
			  .option("inferSchema", "true") // Automatically infer data types
			  .load(srcFile);   //"cars.csv"
	  df=df.withColumnRenamed("Z-axis","Z_axis")
			  .withColumnRenamed("Y-axis","Y_axis")
			  .withColumnRenamed("X-axis","X_axis")
			  .filter("Celsius >30 and Celsius <45 and EDA>0 and EDA<500 and Z_axis>-10");
	  var newDf= timeUnit.unite match {
	    case "second" => df.withColumn("newSecond",(df("seconds")/timeUnit.quantity).cast("Int")).drop("seconds").withColumnRenamed("newSecond","second")	    
	    		.groupBy("year","month","day","hour","minute","second")	    		
	    		.agg(avg("Z_axis").alias("Z_axis"), avg("Y_axis").alias("Y_axis"),avg("X_axis").alias("X_axis"),avg("Celsius").alias("Celsius"),avg("EDA").alias("EDA"),avg("Battery"),avg("Event"))
	    		.sort("year", "month","day","hour","minute","second");
	    case "minute" => df.withColumn("newMinute",(df("minute")/timeUnit.quantity).cast("Int")).drop("minute").withColumnRenamed("newMinute","minute")
	    		.groupBy("year","month","day","hour","minute")
	    		.agg(avg("Z_axis").alias("Z_axis"), avg("Y_axis").alias("Y_axis"),avg("X_axis").alias("X_axis"),avg("Celsius").alias("Celsius"),avg("EDA").alias("EDA"),avg(col("Battery")),avg(col("Event")))
	    		.sort("year", "month","day","hour","minute");
	    case "hour"  => df.withColumn("newHour",(df("hour")/timeUnit.quantity).cast("Int")).drop("hour").withColumnRenamed("newHour","hour")
	    		.groupBy("year","month","day","hour")	    			    		
	    		.agg(avg("Z_axis").alias("Z_axis"), avg("Y_axis").alias("Y_axis"),avg("X_axis").alias("X_axis"),avg(col("Celsius")),avg(col("EDA")),avg(col("Battery")),avg(col("Event")))
	    		.sort("year", "month","day","hour");
 	  } 
	  
	  	  
	  newDf.coalesce(1)
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

