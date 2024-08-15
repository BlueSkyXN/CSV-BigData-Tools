from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化 SparkSession，减少内存分配并调整并行度
spark = SparkSession.builder \
    .appName("Sort Large CSV") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.driver.memory", "30g") \
    .config("spark.executor.memory", "30g") \
    .getOrCreate()

# 读取CSV文件
input_file = r"H:\Data\extracted_qq_phone.csv"
df = spark.read.csv(input_file, header=True, inferSchema=True)

# 对QQ号进行排序，首先将QQ号填充到15位
sorted_df = df.withColumn("padded_qq_number", col("qq_number").cast("string").lpad(15, '0')) \
              .orderBy("padded_qq_number") \
              .drop("padded_qq_number")

# 使用持久化策略将排序后的数据保存在磁盘上，以减少内存占用
sorted_df = sorted_df.persist()

# 将排序后的数据写入输出文件
output_file = r"H:\Data\sorted_qq_phone.csv"
sorted_df.coalesce(1).write.csv(output_file, header=True)

# 停止SparkSession
spark.stop()
