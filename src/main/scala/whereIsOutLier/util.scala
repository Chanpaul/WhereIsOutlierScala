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
	//,colTypeI:Array[(String,Int)]
  def eucDistanceWithRow(item1:Row,item2:Row)(implicit colName:Array[String], colType:Array[(String,String)],colTypeI:Array[(String,Int)]):Double ={
	  var tsum=0.0;	
	  for (attrName<-colName.iterator){   
	    var attr=colType.filter(_._1==attrName).head._2;
		  var temp= colTypeI.filter(_._1==attrName).head._2 match {
		    case  0  => getAttr(attr)(item1,attrName)-getAttr(attr)(item2,attrName)    //numeric
		    case  1  => if (getAttr(attr)(item1,attrName)==getAttr(attr)(item2,attrName)) 0.0 else 1.0       //category	    
		  }
		  tsum=tsum+scala.math.pow(temp,2.0);	    		
	  }
	  return(tsum);
  }
  
   def eucDistance(item1:Array[Double],item2:Array[Double])(implicit colName:Array[String], colType:Array[(String,String)],colTypeI:Array[(String,Int)]):Double ={
	  var tsum=0.0;	
	  for (i<-0 to (item1.size-1)){   
	    //var attr=colType.apply(i)._2;
		  var temp= colTypeI.apply(i)._2 match {
		    case  0  => item1.apply(i)-item2.apply(i)    //numeric
		    case  1  => if (item1.apply(i)==item2.apply(i)) 0.0 else 1.0       //category	    
		  }
		  tsum=tsum+scala.math.pow(temp,2.0);	    		
	  }
	  return(scala.math.sqrt(tsum));
  } 
    
}