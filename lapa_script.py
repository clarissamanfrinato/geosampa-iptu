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
    WITH lapa as ( 
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
            `NUMERO DO CONTRIBUINTE` LIKE '104%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '076%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '074%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '197%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '022%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '012%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '024%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '081%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '080%' 
            OR `NUMERO DO CONTRIBUINTE` LIKE '098%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '023%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '099%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '077%'
            OR `NUMERO DO CONTRIBUINTE` LIKE '078%'

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
        lapa
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

result.toPandas().to_csv("lapa.csv", header=True, encoding='utf-8-sig')
        

# Stop the SparkSession
spark.stop()