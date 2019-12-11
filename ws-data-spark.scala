```scala
/** Prerequisite -- Create Dataframe*/

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("WsDataSpark").master("local").getOrCreate()
val DataSample =  spark.read.option("header", "true").csv("file:///tmp/data/DataSample.csv")
val POIList = spark.read.option("header","true").csv("file:///tmp/data/POIList.csv")

/** The column name, TimeSt in csv file contains space*/
val columnsRenamed_Data = Seq("ID", "TimeSt", "Country", "Province", "City", "Latitude", "Longitude")
val DataSampleDF = DataSample.toDF(columnsRenamed_Data: _*)
/** The column name, Latitude in csv file contains space.*/
val columnRenamed_POI = Seq("POIID", "Latitude", "Longitude")
val POIListDF = POIList.toDF(columnsRenamed_POI: _*)

DataSampeDF.filter("ID is null").count() // 0
val tempDF = DataSampleDF.filter(row=>!row.anynull);


/** 1. Clean up */

DataSampleDF.count() //22025
DataSampleDF.select($"TimeSt", $"Latitude", $"Longitude").distinct().count() // 19999

/** I want to find the rows with count greater than 1,and then delete the first row of set of rows, but I couldn't write codes with Scala.
DataSampleDF.groupBy($"Latitude",$"Longitude", $"TimeSt").count().show()

/** 2. Label */
/** There are duplicate values in the first row and the second row.
* I try to find the closed POI using Square root of 
*(Latitude in DataSampleDF - Latitude in POIListDF)* (Latitude in DataSampleDF - Latitude in POIListDF)+
*(Longitude in DataSampleDF - Longitude in POIListDF)* (Longitude in DataSampleDF - Longitude in POIListDF)
* and mappartition function
*/

/** 4a.
* I could not create the code the number 2 and the number 3, hence I don't know the average and deviation. 
* According the given question, I am going to take Normal distribution so as to be mapped to a scale that range from -10 and to 10.
*/


