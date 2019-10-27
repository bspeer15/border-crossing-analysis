object Main extends App {

import scala.io.Source
import java.io._

val borderData = Source.
 fromFile("./input/Border_Crossing_Entry_Data.csv").
 getLines.
 toList.
 filter( r => r.split(",")(0) != "Port Name" && r != "" ).
 map( r => ( r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6).toInt ) )

val distBorderCrossing = borderData.
 map( bd => bd._1 + "|" + bd._2 + "|" + bd._3 ).
 distinct



val intResults = distBorderCrossing.map( dbc => {
 val cnts = borderData.
  filter( bd => bd._1 + "|" + bd._2 + "|" + bd._3 == dbc ).
  map( bd => bd._4 )
 val dates = borderData.
  filter( bd => (bd._2.substring(6,10) + bd._2.substring(0,2)).toInt  < (dbc.split("\\|")(1).substring(6,10) + dbc.split("\\|")(1).substring(0,2)).toInt && bd._1 + "|" + bd._3 == dbc.split("\\|")(0) + "|" + dbc.split("\\|")(2) ).
  map( bd => bd._4 )

 (dbc, cnts.sum, dates.sum)
})


val finalResults = intResults.map( ir => {
 if (ir._3 == 0 ) 
  ( ir._1, ir._2, 0 )
 else 
  {
  val numberMonths = intResults.filter( ir2 => 
   ir._1.split("\\|")(0) + "|" + ir._1.split("\\|")(2) == ir2._1.split("\\|")(0) + "|" + ir2._1.split("\\|")(2) &&
   ( ir2._1.split("\\|")(1).substring(6,10) + ir2._1.split("\\|")(1).substring(0,2) ).toInt < ( ir._1.split("\\|")(1).substring(6,10) + ir._1.split("\\|")(1).substring(0,2) ).toInt
   ).size
  
  
  ( ir._1, ir._2, (ir._3.toFloat/numberMonths.toFloat).round )
  } 
})


val sortedFinal = finalResults.
 sortWith( (s,t) => {
  if ( s._1.split("\\|")(1) == t._1.split("\\|")(1)  ) s._2 > t._2
  else s._1.split("\\|")(1).substring(6,10) + s._1.split("\\|")(1).substring(0,2) > t._1.split("\\|")(1).substring(6,10) + t._1.split("\\|")(1).substring(0,2)
})


val csvData = sortedFinal.map( sf => sf._1.split("\\|")(0) + "," + sf._1.split("\\|")(1) + "," + sf._1.split("\\|")(2) + "," + sf._2.toString + "," + sf._3.toString + "\n" )

// write to file 
val pw = new PrintWriter(new File("./output/report.csv" ))
pw.write("Border,Date,Measure,Value,Average\n")
csvData.foreach( line => pw.write(line) )

pw.close

}
