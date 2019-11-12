package com.bgc

import common.Utils
import org.apache.spark.sql.DataFrame


/**
  * Created by jnayak on 11/11/2019.
  */
class LoadData {

  // method to read data from flat files
  def readData(fileName:String, filePath:String, numPartitions:Int): DataFrame ={

    val df = Utils.spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load(filePath)
      .repartition(numPartitions)

    df.show()
    print(df.rdd.getNumPartitions)
    return df;
  }

}
