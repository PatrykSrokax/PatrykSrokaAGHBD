// Databricks notebook source
// MAGIC %md
// MAGIC Notatnik 2 
// MAGIC Patryk Sroka 406788

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2
// MAGIC 
// MAGIC Wybierz jeden z plików csv z poprzednich ćwiczeń (Mini Kurs, Actors.csv, Names.csv ect..) lub inny plik z dbfs:/databricks-datasets/ i na jego podstawie stwórz ręcznie schemat danych. Przykład na wykładzie. 
// MAGIC 
// MAGIC a. Użyj funckji spark.read. i stwórz DataFrame (DataFrameReader) wczytując plik z użyciem schematu, który stworzyłeś. 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv") 
            .option("header","true") 
            .option("inferSchema","true") 
            .load(filePath)
namesDf.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 3
// MAGIC 
// MAGIC Celem tego zadanie jest stworzenie pliku json. Możesz to zrobić ręcznie do czego zachęcam, to pozwoli lepiej zrozumieć plik lub jakimś skryptem. Wykorzystaj jeden z pików z poprzedniego zadania wystarczy kilka rzędów danych z pliku csv i stworzenie/konwersję do json.  
// MAGIC 
// MAGIC Stwórz schemat danych do tego pliku, tak jak w poprzednim zadaniu. Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com/ 

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

val namesSchema = StructType(
  Array(
    StructField("imdb_name_id", StringType, true),
    StructField("name", StringType, true),
    StructField("birth_name", StringType, true),
    StructField("height", IntegerType, true),
    StructField("bio", StringType, true),
    StructField("birth_details", StringType, true),
    StructField("date_of_birth", StringType, true),
    StructField("place_of_birth", StringType, true),
    StructField("death_details", StringType, true),
    StructField("date_of_death", StringType, true),
    StructField("place_of_death", StringType, true),
    StructField("reason_of_death", StringType, true),
    StructField("spouses_string", StringType, true),
    StructField("spouses", IntegerType, true),
    StructField("divorces", IntegerType, true),
    StructField("spouses_with_children", IntegerType, true),
    StructField("children", StringType, true),
  )
)

val filePath = "dbfs:/FileStore/tables/Files/names.csv"

val namesDF = spark.read.format("csv") 
            .option("header","true") 
            .option("delimiter", ",")
            .schema(namesSchema)
            .load(filePath)


val firstRows = namesDF.limit(5)

display(firstRows)

firstRows.write
  .mode(SaveMode.Overwrite)
  .json("data_sample_actors.json")

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 4 
// MAGIC 
// MAGIC Użycie Read Modes.  
// MAGIC 
// MAGIC Wykorzystaj posiadane pliki bądź użyj nowe i użyj wszystkich typów read modes, zapisz co się dzieje. Poprawny plik nie wywoła żadnych efektów, więc popsuj dane tak aby każda z read modes zadziałała. 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"

val permissiveDF = spark.read.format("csv") 
            .option("mode", "PERMISSIVE") 
            .load(filePath)
// 

val dropmalformedDF = spark.read.format("csv") 
            .option("mode", "DROPMALFORMED") 
            .load(filePath)
// ignores the whole corrupted records

val failfastDF = spark.read.format("csv") 
            .option("mode", "FAILFAST") 
            .load(filePath)
// throws an exception when it meets corrupted records


// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 5 
// MAGIC 
// MAGIC Użycie DataFrameWriter. 
// MAGIC 
// MAGIC Zapisz jeden z wybranych plików do formatów (‘.parquet’, ‘.json’). Sprawdź, czy dane są zapisane poprawnie, użyj do tego spark.read (DataFrameReader).  

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"

val namesDF = spark.read.format("csv") 
            .option("header","true") 
            .option("delimiter", ",")
            .schema(namesSchema)
            .load(filePath)

val parquetPath = "dbfs:/FileStore/tables/Files/namesDF.parquet"
val jsonPath = "dbfs:/FileStore/tables/Files/namesDF.json"

namesDF.write
  .mode(SaveMode.Overwrite)
  .json(jsonPath)

namesDF.write
  .mode(SaveMode.Overwrite)
  .parquet(parquetPath)

val jsonNames = spark.read.json(jsonPath)
val parquetNames = spark.read.parquet(parquetPath)

// jsonNames.show()
parquetNames.show()

