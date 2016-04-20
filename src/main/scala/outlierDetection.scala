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

case class DataItem(Alcohol:Double, Malicacid:Double,Ash:Double,
        AlcalinityOfash:Double,Magnesium:Double, TotalPhenols:Double,
        Flavanoids:Double, NonflavanoidPhenols:Double, Proanthocyanins:Double,
        ColorIntensity:Double,Hue:Double, dilutedWines:Double, Proline:Double );
//case class DataItem(id:Int,Z:Double,Y:Double,X:Double,celsius:Double,eda:Double);
case class Window(width:Double,slide:Int);
case class DataPoint(id:Int,preNeig:Seq[Int],succNeigNum:Int,expTime:Double);
case class Event(time:Int,id:Int);

object outlierDetection {
  
  def main(args:Array[String]){
    val nn="D://UmassMed//Code//Dataset//wine//wine.data";
    val window=Window(12.0,1);  //width=12.0,slide=1;
    val conf = new SparkConf().setAppName("WhereIsOutlier")
    		.setMaster("local[2]")
    		.setAppName("WhereIsOutlier");
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc);
    
   
    //val ds = sqlContext.read.text(nn).as[DataItem];     

   
    /*
    var evtQue=Seq(Event(0,0)).toDS;
    var firstDataItem=Seq(ds.first).toDS;
    var pt=DataPoint(ds.first.id,Seq(ds.first.id),0,window.width);
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
    }*/
  }
  def clean(srcFile:String,objFile:String,sqlContext:SQLContext){
	  //val sqlContext = new SQLContext(sc);
	  val df = sqlContext.read
			  .format("com.databricks.spark.csv")
			  .option("delimiter",",")
			  .option("header", "true") // Use first line of all files as header
			  .option("inferSchema", "true") // Automatically infer data types
			  .load(srcFile);   //"cars.csv"
	  val newDf=df.groupBy("year","month","day","hour")
			  .agg(avg(col("Z_axis")), avg(col("Y_axis")),avg(col("X_axis")),
	      avg(col("Celsius")),avg(col("EDA"))
	      ).sort("year", "month","day","hour");
	  
	  newDf.write
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