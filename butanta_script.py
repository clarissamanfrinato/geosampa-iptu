from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col


# Create a SparkSession
spark = SparkSession.builder \
    .appName("CSV Query") \
    .getOrCreate()

# Read CSV file into a DataFrame
# df=spark.read.format("csv").option("inferSchema","true").load("Input\EG_2010.csv")

df = spark.read.options(inferSchema= True, header='True',  delimiter =";") \
  .csv("input\EG_2010.csv")

# Perform SQL-like queries on the DataFrame
df.createOrReplaceTempView("csv_table")

# df.printSchema()

# Example query
query = """
    WITH butanta as ( 
        SELECT 
            `NUMERO DO CONTRIBUINTE` AS NUMERO_DO_CONTRIBUINTE,
            CAST(`AREA CONSTRUIDA` AS FLOAT) AS AREA_CONSTRUIDA,
            CAST(`AREA DO TERRENO` AS FLOAT) AS AREA_DO_TERRENO,
            CAST(`AREA OCUPADA` AS FLOAT) AS AREA_OCUPADA,
            CAST(`QUANTIDADE DE PAVIMENTOS` AS INT) AS QTD_PAVIMENTOS,
            `TIPO DE USO DO IMOVEL` AS TIPO_USO_IMOVEL,
            `NUMERO DO CONDOMINIO` AS NUMERO_DO_CONDOMINIO

        FROM csv_table
        WHERE
            `NUMERO DO CONTRIBUINTE` LIKE '082%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '200%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '084%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '300%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '101%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '083%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '015%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '096%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '081%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '016%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '299%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '123%'

    )
    SELECT
        NUMERO_DO_CONTRIBUINTE,
        SUM(AREA_CONSTRUIDA) AS SOMA_AREA_CONSTRUIDA,
        AREA_DO_TERRENO,
        AREA_OCUPADA,
        QTD_PAVIMENTOS,
        TIPO_USO_IMOVEL,
        NUMERO_DO_CONDOMINIO
    FROM 
        butanta
    GROUP BY 
        NUMERO_DO_CONDOMINIO,
        NUMERO_DO_CONTRIBUINTE,
        AREA_DO_TERRENO,
        AREA_OCUPADA,
        QTD_PAVIMENTOS,
        TIPO_USO_IMOVEL
    ORDER BY
        NUMERO_DO_CONTRIBUINTE ASC
    
"""

result = spark.sql(query)

# result.show()


# Show the result
# result.show()

result.toPandas().to_csv("butanta.csv", header=True, encoding='utf-8-sig')
        

# Stop the SparkSession
spark.stop()