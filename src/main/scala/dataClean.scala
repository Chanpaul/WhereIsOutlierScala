import java.util.Scanner
import java.io.File
import java.io._
import scala.util.matching.Regex
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql._
object dataClean {	
  /*
  def main(args:Array[String]){
    var srcDir:String="D://Wangjin//UmassMed//Code//Dataset//cocaine//CSV files//Cleaned Data";
	  var srcFn:String="Patient1.csv";
	  var objDir:String="D://Wangjin//UmassMed//Code//Dataset//cocaine//CSV files//Cleaned Data//sortAndAgg";
	  var objFn:String="Patient1.csv";
	  sortAndAgg(srcDir+"//"+srcFn,objDir+"//"+objFn);
  }*/
  def sortAndAgg(srcFile:String,objFile:String)
  {
    //var objDir:String="D://Wangjin//UmassMed//Code//Dataset//cocaine//CSV files//Cleaned Data";
	  //var fn:String="Patient1.csv";
	  //var f=scala.io.Source.fromFile(objDir+"//"+fn);
	  //var lines=f.getLines.mkString;
	//val sc: SparkContext;//=new SparkContext; // An existing SparkContext.
  val conf = new SparkConf().setAppName("test4").setMaster("local").setJars(Seq("/a/b/x.jar", "/c/d/y.jar"));
  val sc= new SparkContext(conf);
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val df = sqlContext.read
		  .format("com.databricks.spark.csv")
		  .option("header", "true") // Use first line of all files as header
		  .option("inferSchema", "true") // Automatically infer data types
		  .load(srcFile);
  val tdf=df.groupBy("year","month","day","hour")
		  .agg(avg(df.col("Celsius")),avg(df.col("Z-axis")),avg(df.col("Y-axis")),avg(df.col("X-axis")),
		      avg(df.col("EDA")));
		  //.avg("Celsius",$"Z-axis",$"Y-axis",$"X-axis",$"EDA")
		  //.agg($"Celsius",$"Z-axis",$"Y-axis",$"X-axis",$"EDA");
		  //.agg(avg(df.col("Celsius")),avg(df.col("Z-axis")),avg(df.col("Y-axis")),avg(df.col("X-axis")),
		  //    avg(df.col("EDA")));
  val sortedTdf=tdf.orderBy("year","month","day","hour");
  sortedTdf.save(objFile);
  //val sortedTdf=tdf.orderBy(asc("year"),asc("month"),desc("day"),desc("hour"));
  //tdf.show();  //default 30 lines, you can set it
  //val selectedData = df.select("year", "model");
  
  //selectedData.save("newcars.csv", "com.databricks.spark.csv");
  
		  
  }
	def cleanMultiple(srcDir:String,objDir:String,objPrefix:String) {
	  
	  //var objDir:String="D://Wangjin//UmassMed//Code//Dataset//cocaine//CSV files//Cleaned Data";
	  //var objPrefix:String="Patient";
	  //val srcDir="D://Wangjin//UmassMed//Code//Dataset//cocaine//CSV files";
	  
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
								  
								  /*val test=cols(0).substring(0,2);					  
								  if (test.equals("00") && flag==true){
									  hour= hour+1;
									  flag=false;

								  } else if (!test.equals("00")){
									  flag=true;
								  }*/
								  // do whatever you want with the columns here
								  //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
								  writer.write(year+","+month+","+day+","+hour+","+minute+","+seconds+","+line+"\n");
							  }
						}
						  bufferedSource.close   
						  
						}
					}		     

		}
		writer.close
	}
    /*
    val	fn="0001_2014_01_10_01.csv"
    
    //val dataDir=args(0)
    //val fn=args(1)
    val newFn="patient_01_10_01_scala.csv"
    val bufferedSource = io.Source.fromFile(dataDir+fn)
    val writer = new PrintWriter(new File(dataDir+newFn))
    var lineStart:BigInt=3
    var flag=false  
    for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        val test=cols(0).substring(0,2)
        //println(test)
             
        if (test.equals("00") &&flag==false){
          lineStart= lineStart+1
          flag=true
          
        } else if (!test.equals("00")){
          flag=false
        }
        // do whatever you want with the columns here
        //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
        writer.write(line+","+lineStart+"\n")
    }
    bufferedSource.close
    writer.close
  }*/
}