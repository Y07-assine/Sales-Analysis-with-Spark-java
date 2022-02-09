package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Main {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.setProperty("hadoop.home.dir", "c:/hadoop-3.2.1");
        SparkSession spark = SparkSession.builder()
                .appName("sales analysis")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> data = spark.read().option("header",true).csv("sales_data.csv");

        data.printSchema();


        //data cleaning

        data = data.na().drop();
        data = data.filter(data.col("Order Date").notEqual("Order Date"));

        data = data.withColumn("Price Each", data.col("Price Each").cast(DataTypes.DoubleType));
        data = data.withColumn("Quantity Ordered", data.col("Quantity Ordered").cast(DataTypes.IntegerType));

        //add month column
        data = data.withColumn("Month", data.col("Order Date").substr(0, 2).cast(DataTypes.IntegerType));

        //Add sales columns
        data = data.withColumn("sales", data.col("Price Each").multiply(data.col("Quantity Ordered")));

        //The best month for sales
        Dataset<Row> sales_per_month = data.groupBy("Month").sum("sales").orderBy(functions.col("sum(sales)").desc());

        //City with the highest number of sales

        //add city column


        data = data.withColumn("city", functions.split(data.col("Purchase Address"),",").getItem(1));

        Dataset<Row> sales_per_city = data.groupBy("city").sum().orderBy(functions.col("sum(sales)").desc());

        //Best time to display advertisements to maximise likelihood of customer's buying product

        //add hour column

        data = data.withColumn("hour", functions.split(functions.split(data.col("Order Date")," ").getItem(1),":").getItem(0).cast(DataTypes.IntegerType));

        Dataset<Row> time_for_ads = data.groupBy("hour").sum().orderBy(functions.col("sum(Quantity Ordered)").desc());


        //Product most often sold together

        Dataset<Row> dup = data.groupBy("Order ID")
                .agg(functions.concat_ws(",",functions.sort_array(functions.collect_list("Product"))).alias("same_cmd"))
                .groupBy("same_cmd")
                .agg(functions.count("Order ID").alias("count")).orderBy(functions.col("count").desc());

        //Product sold the most

        Dataset<Row> products = data.groupBy("Product").sum().orderBy(functions.col("sum(Quantity Ordered)").desc());

        products.show();

        spark.close();

    }

}

