import findspark
findspark.init
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.ml import PipelineModel
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from kafka import KafkaConsumer
import json


def housePredict(stack_overflow_data):
	df = spark.read.json(stack_overflow_data)
	df.persist()

	df = df.withColumn("LotFrontage", F.when(F.col("LotFrontage") == 'NA',70).otherwise(F.col("LotFrontage"))) \
	.withColumn("LotFrontage", F.col("LotFrontage").cast("int")) \
	.withColumn("MasVnrArea", F.when(F.col("MasVnrArea") == 'NA',104).otherwise(F.col("MasVnrArea"))) \
	.withColumn("MasVnrArea", F.col("MasVnrArea").cast("int")) \
	.withColumn("GarageYrBlt", F.when(F.col("GarageYrBlt") == 'NA',1979).otherwise(F.col("GarageYrBlt"))) \
	.withColumn("GarageYrBlt", F.col("GarageYrBlt").cast("int")) \
	.withColumn("BsmtFinSF1", F.when(F.col("BsmtFinSF1") == 'NA',441).otherwise(F.col("BsmtFinSF1"))) \
	.withColumn("BsmtFinSF1", F.col("BsmtFinSF1").cast("int")) \
	.withColumn("BsmtFinSF2", F.when(F.col("BsmtFinSF2") == 'NA',50).otherwise(F.col("BsmtFinSF2"))) \
	.withColumn("BsmtFinSF2", F.col("BsmtFinSF2").cast("int")) \
	.withColumn("BsmtUnfSF", F.when(F.col("BsmtUnfSF") == 'NA',561).otherwise(F.col("BsmtUnfSF"))) \
	.withColumn("BsmtUnfSF", F.col("BsmtUnfSF").cast("int")) \
	.withColumn("TotalBsmtSF", F.when(F.col("TotalBsmtSF") == 'NA',1052).otherwise(F.col("TotalBsmtSF"))) \
	.withColumn("TotalBsmtSF", F.col("TotalBsmtSF").cast("int")) \
	.withColumn("BsmtFullBath", F.when(F.col("BsmtFullBath") == 'NA',0.43).otherwise(F.col("BsmtFullBath"))) \
	.withColumn("BsmtFullBath", F.col("BsmtFullBath").cast("float")) \
	.withColumn("BsmtHalfBath", F.when(F.col("BsmtHalfBath") == 'NA',0.06).otherwise(F.col("BsmtHalfBath"))) \
	.withColumn("BsmtHalfBath", F.col("BsmtHalfBath").cast("float")) \
	.withColumn("GarageCars", F.when(F.col("GarageCars") == 'NA',2).otherwise(F.col("GarageCars"))) \
	.withColumn("GarageCars", F.col("GarageCars").cast("int")) \
	.withColumn("GarageArea", F.when(F.col("GarageArea") == 'NA',473).otherwise(F.col("GarageArea"))) \
	.withColumn("GarageArea", F.col("GarageArea").cast("int"))

	numcols=['MSSubClass', 'LotFrontage', 'LotArea', 'OverallQual', 'OverallCond', 'YearBuilt', 'YearRemodAdd', 'MasVnrArea', 'BsmtFinSF1', 'BsmtFinSF2', 'BsmtUnfSF', 'TotalBsmtSF', '1stFlrSF', '2ndFlrSF', 'LowQualFinSF', 'GrLivArea', 'BsmtFullBath', 'BsmtHalfBath', 'FullBath', 'HalfBath', 'BedroomAbvGr', 'KitchenAbvGr', 'TotRmsAbvGrd', 'Fireplaces', 'GarageYrBlt', 'GarageCars', 'GarageArea', 'WoodDeckSF', 'OpenPorchSF', 'EnclosedPorch', '3SsnPorch', 'ScreenPorch', 'PoolArea', 'MiscVal', 'MoSold', 'YrSold']

	for col_name in numcols:
    		df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

	prediction = pipemodel.transform(df)

	print(prediction.select('prediction').show())

spark = SparkSession.builder.appName("HousePricePredict").getOrCreate()
stack_overflow_data = 'data.json'
pipemodel = PipelineModel.load("/home/subbu/Documents/model")
if __name__=="__main__":
    consumer=KafkaConsumer('HousePrice',bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id='consumer-group-a')
    print('starting the consumer')
    for msg in consumer:
        data=json.loads(msg.value)
        print('HouseData = {}'.format(json.loads(msg.value)))
        housePredict(data)
