// Databricks notebook source
#Patryk Sroka 406788

// COMMAND ----------

// MAGIC %md 
// MAGIC Wykorzystaj dane z bazy 'svrsqldev1.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "svrsqldev1.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "dbdevsample"

val tabela = spark.read
  .format("jdbc")
  .option("url", s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user", "user123")
  .option("password", "Passw0rd$$")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query", "SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()


display(tabela)

// COMMAND ----------

// MAGIC %python
// MAGIC # from pyspark.sql import SparkSessio
// MAGIC 
// MAGIC # Definiowanie parametrów połączenia do bazy danych
// MAGIC jdbcHostname = "svrsqldev1.database.windows.net"
// MAGIC jdbcPort = 1433
// MAGIC jdbcDatabase = "dbdevsample"
// MAGIC 
// MAGIC # Odczyt danych z bazy danych
// MAGIC tabela = spark.read \
// MAGIC     .format("jdbc") \
// MAGIC     .option("url", f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}") \
// MAGIC     .option("user", "user123") \
// MAGIC     .option("password", "Passw0rd$$") \
// MAGIC     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
// MAGIC     .option("query", "SELECT * FROM INFORMATION_SCHEMA.TABLES") \
// MAGIC     .load()
// MAGIC 
// MAGIC # Wyświetlenie wyników
// MAGIC tabela.show()

// COMMAND ----------

// MAGIC %python
// MAGIC customer_data = spark.read \
// MAGIC     .format("jdbc") \
// MAGIC     .option("url", f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}") \
// MAGIC     .option("user", "user123") \
// MAGIC     .option("password", "Passw0rd$$") \
// MAGIC     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
// MAGIC     .option("dbtable", "SalesLT.Customer") \
// MAGIC     .load()
// MAGIC 
// MAGIC # Wyświetlenie zawartości tabeli "Customer"
// MAGIC display(customer_data)
// MAGIC 
// MAGIC product_data = spark.read \
// MAGIC     .format("jdbc") \
// MAGIC     .option("url", f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}") \
// MAGIC     .option("user", "user123") \
// MAGIC     .option("password", "Passw0rd$$") \
// MAGIC     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
// MAGIC     .option("dbtable", "SalesLT.Product") \
// MAGIC     .load()
// MAGIC display(product_data)
// MAGIC 
// MAGIC 
// MAGIC display(customer_data)
// MAGIC 
// MAGIC product_model_data = spark.read \
// MAGIC     .format("jdbc") \
// MAGIC     .option("url", f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}") \
// MAGIC     .option("user", "user123") \
// MAGIC     .option("password", "Passw0rd$$") \
// MAGIC     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
// MAGIC     .option("dbtable", "SalesLT.ProductModel") \
// MAGIC     .load()
// MAGIC display(product_model_data)
// MAGIC 
// MAGIC sales_order_detail = spark.read \
// MAGIC     .format("jdbc") \
// MAGIC     .option("url", f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}") \
// MAGIC     .option("user", "user123") \
// MAGIC     .option("password", "Passw0rd$$") \
// MAGIC     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
// MAGIC     .option("dbtable", "SalesLT.SalesOrderDetail") \
// MAGIC     .load()
// MAGIC display(sales_order_detail)

// COMMAND ----------

// MAGIC %python
// MAGIC # from pyspark.sql import SparkSession
// MAGIC 
// MAGIC # spark = SparkSession.builder.appName("Read CSV").getOrCreate()
// MAGIC 
// MAGIC # customer_data = spark.read.format("csv") \
// MAGIC #   .option("header", "true") \
// MAGIC #   .option("inferSchema", "true") \
// MAGIC #   .load("dbfs:/FileStore/tables/Customer.csv")
// MAGIC 
// MAGIC # display(customer_data)
// MAGIC 
// MAGIC # product_data = spark.read.format("csv") \
// MAGIC #   .option("header", "true") \
// MAGIC #   .option("inferSchema", "true") \
// MAGIC #   .load("dbfs:/FileStore/tables/Product.csv")
// MAGIC 
// MAGIC # display(product_data)
// MAGIC 
// MAGIC # product_model_data = spark.read.format("csv") \
// MAGIC #   .option("header", "true") \
// MAGIC #   .option("inferSchema", "true") \
// MAGIC #   .load("dbfs:/FileStore/tables/ProductModel.csv")
// MAGIC 
// MAGIC # display(product_model_data)

// COMMAND ----------

// MAGIC %python
// MAGIC #CROSS JOIN
// MAGIC display(customer_data.crossJoin(product_data))

// COMMAND ----------

// MAGIC %python
// MAGIC #INNER
// MAGIC joinExpression = product_data["ProductID"] == sales_order_detail["ProductID"]
// MAGIC joinType="inner"
// MAGIC joined_df = product_data.join(sales_order_detail, joinExpression, joinType)
// MAGIC display(joined_df)

// COMMAND ----------

// MAGIC %python
// MAGIC #LEFT OUTER
// MAGIC joinType="left_outer"
// MAGIC joinExpression = product_data["ProductID"] == sales_order_detail["ProductID"]
// MAGIC joined_df = product_data.join(sales_order_detail, joinExpression, joinType)
// MAGIC display(joined_df)

// COMMAND ----------

// MAGIC %python
// MAGIC #RIGHT OUTER
// MAGIC joinType="right_outer"
// MAGIC joinExpression = product_data["ProductID"] == sales_order_detail["ProductID"]
// MAGIC joined_df = product_data.join(sales_order_detail, joinExpression, joinType)
// MAGIC display(joined_df)

// COMMAND ----------

// MAGIC %python
// MAGIC #LEFT SEMI
// MAGIC joinType="left_semi"
// MAGIC joinExpression = product_data["ProductID"] == sales_order_detail["ProductID"]
// MAGIC joined_df = product_data.join(sales_order_detail, joinExpression, joinType)
// MAGIC display(joined_df)

// COMMAND ----------

// MAGIC %python
// MAGIC #LEFT ANTI
// MAGIC joinType="left_anti"
// MAGIC joinExpression = product_data["ProductID"] == sales_order_detail["ProductID"]
// MAGIC joined_df = product_data.join(sales_order_detail, joinExpression, joinType)
// MAGIC display(joined_df)

// COMMAND ----------

// MAGIC %md
// MAGIC Połącz tabele po tych samych kolumnach (Jeśli nie ma takich samych to dodaj kolumnę) i użyj metody na usunięcie duplikatów. 

// COMMAND ----------

// MAGIC %python
// MAGIC #DROP DUPLICATES
// MAGIC joinType="right_outer"
// MAGIC joinExpression = product_data["ProductID"] == sales_order_detail["ProductID"]
// MAGIC joined = product_data.join(sales_order_detail, joinExpression, joinType)
// MAGIC joined = joined.dropDuplicates()
// MAGIC display(joined)

// COMMAND ----------

// MAGIC %python
// MAGIC #DISTINCT
// MAGIC joinType = "left_outer"
// MAGIC joinExpression = product_data["ProductID"] == sales_order_detail["ProductID"]
// MAGIC joined = product_data.join(sales_order_detail, joinExpression, joinType).distinct()
// MAGIC display(joined)

// COMMAND ----------

// MAGIC %md Dla Chętnych
// MAGIC  Uzycie Nulls, fill, drop, replace
// MAGIC  * W kilku z tabel sprawdź ile jest nulls w kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wybranych tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle.
// MAGIC  * Jakie są 3 najlepiej się sprzedające produkty?
// MAGIC  * Skąd są kupujący, który kupili te najlepsze produkty
