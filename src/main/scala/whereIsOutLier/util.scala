package whereIsOutLier

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.typesafe.config._


trait util { 
  case class Window(width:Double,slideLen:Int);
  case class OutlierParam(typ:String,R:Double,k:Int);
	//implicit var colName:Array[String];
	//implicit var colType:Array[(String,String)];   
	def getAttr(typ:String)={
    typ match {
    case "IntegerType" => (r:Row,attrName:String)=>{r.getAs[Int](attrName).toDouble}
    case "DoubleType" => (r:Row,attrName:String)=>{r.getAs[Double](attrName)}   
  }    
  }
  def eucDistance(item1:Row,item2:Row)(implicit colName:Array[String], colType:Array[(String,String)]):Double ={
	  var tsum=0.0;	    		    	
	  for (attrName<-colName.iterator){
		  var attr=colType.filter(_._1==attrName).head._2; 

		  var temp=getAttr(attr)(item1,attrName)-getAttr(attr)(item2,attrName);  	  
		  
		  tsum=tsum+scala.math.pow(temp,2.0);	    		
	  }
	  return(tsum);
  }
  
    
}