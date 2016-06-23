package whereIsOutLier

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.typesafe.config._
import breeze.linalg._
import operators._
import breeze.numerics._


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
  
   def eucDistance1(item1:Array[Double],item2:Array[Double])(implicit colName:Array[String], colAttr:Array[(String,String,String,String)]):Double ={
	  var tsum=0.0;	
	  
	  for (i<-0 to (item1.size-1) ){
	    var tcn=colAttr.filter(x=>x._1==colName(i)).head
	    if (tcn._4=="used"){
	    	var temp= tcn._2 match {
	    	case  "numeric"  => item1.apply(i)-item2.apply(i)    //numeric
	    	case  default  => if (item1.apply(i)==item2.apply(i)) 0.0 else 1.0       //category	    
	    	}  
	    	tsum=tsum+scala.math.pow(temp,2.0);	   
	    }	  
		   		
	  }
	  return(scala.math.sqrt(tsum));
  } 
   def eucDistance(item1:Array[Double],item2:Array[Double])(implicit colName:Array[String], colAttr:Array[(String,String,String,String)]):Double ={
	  var tsum=0.0;	
	  var usedAttr=colAttr.filter(x=>colName.contains(x._1)==true && x._4=="used")
	  var numericAttr=usedAttr.filter(_._2=="numeric").map(x=>colName.indexOf(x._1));
	  var nominalAttr=usedAttr.filter(_._2!="numeric").map(x=>colName.indexOf(x._1));  
	  var item1NumDenVec=new DenseVector(item1.filter(x=>numericAttr.contains(item1.indexOf(x))));
	  var item1NomDenVec=new DenseVector(item1.filter(x=>nominalAttr.contains(item1.indexOf(x))));
	  var item2NumDenVec=new DenseVector(item2.filter(x=>numericAttr.contains(item2.indexOf(x))));
	  var item2NomDenVec=new DenseVector(item2.filter(x=>nominalAttr.contains(item2.indexOf(x))));
	  var nomDist=breeze.linalg.sum((item2NomDenVec:==item1NomDenVec).map(x=>x match {case true => 1   case false=> 0}));
	  var numDist=breeze.linalg.sum(breeze.numerics.pow(item1NumDenVec-item2NumDenVec,2));
	  tsum=breeze.numerics.sqrt(nomDist+numDist);
	  return(tsum);
  }
   def eucDistance(item1:DenseVector[Double],item2:DenseMatrix[Double])(implicit colName:Array[String], colAttr:Array[(String,String,String,String)]):DenseVector[Double] ={
		var usedAttr=colAttr.filter(x=>colName.contains(x._1)==true && x._4=="used")
	  var numericAttr=usedAttr.filter(_._2=="numeric").map(x=>colName.indexOf(x._1)).toVector;
	  var nominalAttr=usedAttr.filter(_._2!="numeric").map(x=>colName.indexOf(x._1)).toVector;  
	  var tempNumDistVec=item2(::,numericAttr).toDenseMatrix-DenseVector.ones[Double](item2.rows)*item1(numericAttr).toDenseVector.t;	  
	  var numDistVec=breeze.linalg.sum(breeze.numerics.pow(tempNumDistVec,2.0),Axis._1);
	  var tempNomDistMx=(item2(::,numericAttr).toDenseMatrix:==DenseVector.ones[Double](item2.rows)*item1(numericAttr).toDenseVector.t);
	  var nomDistVec=breeze.linalg.sum(tempNomDistMx.map(x=>x match {case true => 1.0   case false=> 0.0}),Axis._1);	  
	  var tsum=breeze.numerics.sqrt(nomDistVec+numDistVec);
	  return(tsum);
  }
}