from helper.scd2_pyspark import apply_scd_type2 
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\akazm\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\akazm\AppData\Local\Programs\Python\Python311\python.exe"
spark = SparkSession.builder.appName("Test SCD2").master("local[*]").getOrCreate()

schema = StructType([    
    StructField("id", IntegerType(), True),    
    StructField("name", StringType(), True),    
    StructField("update_date", StringType(), True),    
    StructField("valid_from", StringType(), True),    
    StructField("valid_to", StringType(), True),    
    StructField("is_current", BooleanType(), True),    
    StructField("surrogate_key", StringType(), True)])

schema2 = StructType([    
    StructField("id", IntegerType(), True),    
    StructField("name", StringType(), True),    
    StructField("update_date", StringType(), True),])

# source_data = [(1, "John", "2024-11-01"),               
#                (2, "Jane", "2024-11-01")]
# target_data = [(1, "John", "2024-10-01", "1900-01-01 00:00:00", "3000-01-01 00:00:00", False, "uuid-1"),               
#                (3, "Jack", "2024-11-01", "1900-01-01 00:00:00", "3000-01-01 00:00:00", True, "uuid-3")]

# Dados mockados incluindo:
# - Novos dados
# - Dados que não serão modificados
# - Dados que já foram modificados e serão novamente atualizados

source_data = [(1, "John", "2025-02-01"),               
               (2, "Jane", "2024-11-01"),
               (7, "Anna", "2024-12-01")]
target_data = [
               (1, "John", "2024-11-01", "1900-01-01 00:00:00", "2025-01-01", False, "cda93668-59d5-4d38-83a4-f0bd9d16d10c"),
               (1, "John", "2025-01-01", "2025-01-01", "3000-01-01 00:00:00", True, "9dfa0413-6366-4f4f-af9d-3450c594bca9"),
               (2, "Jane", "2024-10-01", "1900-01-01 00:00:00", "3000-01-01 00:00:00", True, "uuid-2"),              
               (3, "Jack", "2024-11-01", "1900-01-01 00:00:00", "3000-01-01 00:00:00", True, "uuid-3"),
               (4, "Joe", "2023-02-01", "1900-01-01 00:00:00", "3000-01-01 00:00:00", True, "uuid-4"),]

source_df = spark.createDataFrame(source_data, schema2)
target_df = spark.createDataFrame(target_data, schema)

# Definir colunas não versionadas
non_versioned_fields = {"name"}

# Testar a função
result_df = apply_scd_type2(    
    source=source_df,    
    target=target_df,    
    pk="id",    
    non_versioned_fields=non_versioned_fields,    
    control_column="update_date",    
    surrogate_key_name="surrogate_key",)

result_df.printSchema()

result_df.show(truncate=False)

