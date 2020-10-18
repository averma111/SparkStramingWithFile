import sys
from lib.utils import get_spark_app_config
from lib.utils import read_from_stream ,wrirte_to_console_from_stream
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,expr
from lib.logger import Log4j

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting the Streaming application")

    # Reading the lines as datafrom 
    
    raw_df = read_from_stream(spark) 
    
    # Infer Json schema explicitly
    #raw_df.printSchema()
    
    #Selecting meaningful columns 
    
    exploded_df = raw_df.select(col("InvoiceNumber"),col("CreatedTime"),col("StoreID"),col("PosID"),
                  col("CustomerType"),col("PaymentMethod"),col("DeliveryType"),
                  col("DeliveryAddress.City").alias("City"), col("DeliveryAddress.State").alias("State"),col("DeliveryAddress.PinCode").alias("PinCode"),
                  explode(col("InvoiceLineItems")).alias("LineItems")
                  )
    # After exploded schema
    exploded_df.printSchema()
    
    flattened_df = exploded_df \
                   .withColumn("ItemCode",expr("LineItems.ItemCode")) \
                   .withColumn("ItemDescription",expr("LineItems.ItemDescription")) \
                   .withColumn("ItemPrice",expr("LineItems.ItemPrice")) \
                   .withColumn("ItemQty",expr("LineItems.ItemQty")) \
                   .withColumn("TotalValue",expr("LineItems.TotalValue")) \
                   .drop("LineItems")
                   
    flattened_df.printSchema()           
        # Writing the output to targt location               
    flattened_query_result = wrirte_to_console_from_stream(flattened_df)   

    flattened_query_result.awaitTermination()
    
    logger.info("Completing the Streaming application")
    
  

