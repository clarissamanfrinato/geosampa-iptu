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
    WITH tatuape as ( 
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
            `NUMERO DO CONTRIBUINTE` LIKE '063%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '062%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '054%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '030%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '052%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '031%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '029%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '032%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '028%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '053%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '027%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '026%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '196%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '064%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '061%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '026%'
            
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
        tatuape
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

result.toPandas().to_csv("tatuape.csv", header=True, encoding='utf-8-sig')
        

# Stop the SparkSession
spark.stop()