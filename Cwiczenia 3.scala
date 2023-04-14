// Databricks notebook source
Patryk Sroka 406788

%md Names.csv 
* Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
* Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
* Odpowiedz na pytanie jakie jest najpopularniesze imię?
* Dodaj kolumnę i policz wiek aktorów 
* Usuń kolumny (bio, death_details)
* Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
* Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

import org.apache.spark.sql.functions._

// adding timestamp and height in feet columns
var newNamesDf = namesDf.withColumn("current_timestamp", unix_timestamp(current_timestamp()))
  .withColumn("height_in_ft", col("height")*3.28/100)

display(newNamesDf)

//display(newNamesDf.select(substring_index(col("name"), " ", 1)).groupBy("substring_index(name,  , 1)").count().sort(desc("count")).as("Most popular first names"))
// John is most popular name


// COMMAND ----------

newNamesDf = newNamesDf.withColumn("age_1", floor((unix_timestamp($"date_of_death", "dd.MM.yyyy")-unix_timestamp($"date_of_birth", "dd.MM.yyyy")) / 60 / 60 / 24 / 365))
newNamesDf = newNamesDf.withColumn("age_2", floor((unix_timestamp($"date_of_death", "yyyy-MM-dd")-unix_timestamp($"date_of_birth", "dd.MM.yyyy")) / 60 / 60 / 24 / 365))
newNamesDf = newNamesDf.withColumn("age_3", floor((unix_timestamp($"date_of_death", "dd.MM.yyyy")-unix_timestamp($"date_of_birth", "yyyy-MM-dd")) / 60 / 60 / 24 / 365))
newNamesDf = newNamesDf.withColumn("age_4", floor((unix_timestamp($"date_of_death", "yyyy-MM-dd")-unix_timestamp($"date_of_birth", "yyyy-MM-dd")) / 60 / 60 / 24 / 365))
newNamesDf = newNamesDf.withColumn("age_5", when(($"date_of_death").isNull, floor((unix_timestamp(current_timestamp())-unix_timestamp($"date_of_birth", "yyyy-MM-dd")) / 60 / 60 / 24 / 365)))
newNamesDf = newNamesDf.withColumn("age_6", when(($"date_of_death").isNull, floor((unix_timestamp(current_timestamp())-unix_timestamp($"date_of_birth", "dd.MM.yyyy")) / 60 / 60 / 24 / 365)))

newNamesDf = newNamesDf.withColumn("age", when(($"age_1").isNotNull, $"age_1")
                                   .when(($"age_2").isNotNull, $"age_2")
                                   .when(($"age_3").isNotNull, $"age_3")
                                   .when(($"age_4").isNotNull, $"age_4")
                                   .when(($"age_5").isNotNull, $"age_5")
                                   .when(($"age_6").isNotNull, $"age_6")
                                   .otherwise(null))
newNamesDf = newNamesDf.drop("age_1", "age_2", "age_3", "age_4", "age_5", "age_6", "bio", "death_details")

display(newNamesDf)

// COMMAND ----------

newNamesDf = newNamesDf.select(newNamesDf.columns.map(c => col(c).as(c.replaceAll("_", " ").capitalize)): _*)
newNamesDf = newNamesDf.sort(asc("Name"))
display(newNamesDf)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(moviesDf)

// COMMAND ----------

import org.apache.spark.sql.functions._

// adding timestamp 
var newMoviesDf = moviesDf.withColumn("current_timestamp", unix_timestamp(current_timestamp()))

newMoviesDf = newMoviesDf.withColumn("years_1", floor((unix_timestamp(current_timestamp())-unix_timestamp($"date_published", "dd.MM.yyyy")) / 60 / 60 / 24 / 365))
newMoviesDf = newMoviesDf.withColumn("years_2", floor((unix_timestamp(current_timestamp())-unix_timestamp($"date_published", "yyyy-MM-dd")) / 60 / 60 / 24 / 365))
newMoviesDf = newMoviesDf.withColumn("years_3", floor((unix_timestamp(current_timestamp())-unix_timestamp($"date_published", "yyyy")) / 60 / 60 / 24 / 365))
                                     
newMoviesDf = newMoviesDf.withColumn("years_since_release", when(($"years_1").isNotNull, $"years_1")
                                   .when(($"years_2").isNotNull, $"years_2")
                                   .when(($"years_3").isNotNull, $"years_3")
                                   .otherwise(null))
newMoviesDf = newMoviesDf.drop("years_1", "years_2", "years_3")      

newMoviesDf = newMoviesDf.withColumn("budget_numeric", split(col("budget"), " ")(1).cast("int"))

newMoviesDf = newMoviesDf.na.drop()

display(newMoviesDf)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
// MAGIC ratingsDf = spark.read.format("csv") \
// MAGIC               .option("header","true") \
// MAGIC               .option("inferSchema","true") \
// MAGIC               .load(filePath)
// MAGIC 
// MAGIC display(ratingsDf)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import IntegerType
// MAGIC import numpy as np
// MAGIC #import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC def median(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10):
// MAGIC   arr = [v1, v2, v3, v4, v5, v6, v7, v8, v9, v10]
// MAGIC   midpoint = np.sum(arr) / 2
// MAGIC   temp_sum = 0
// MAGIC   median = 0
// MAGIC   for i in range(0,10):
// MAGIC     temp_sum += arr[i]
// MAGIC     if temp_sum >= midpoint:
// MAGIC       median = i+1
// MAGIC       break
// MAGIC   return median
// MAGIC     
// MAGIC medianUDF = udf(median, IntegerType())
// MAGIC   
// MAGIC median(14, 5, 1, 9, 28, 28, 43, 10, 4, 12)
// MAGIC   
// MAGIC 
// MAGIC # adding timestamp 
// MAGIC #var newRatingsDf = ratingsDf.withColumn("current_timestamp", unix_timestamp(current_timestamp()))
// MAGIC newRatingsDf = ratingsDf.withColumn("current_timestamp", unix_timestamp(current_timestamp()))
// MAGIC newRatingsDf = newRatingsDf.withColumn("mean_value", (10*col("votes_10")+9*col("votes_9")+8*col("votes_8")+7*col("votes_7")+6*col("votes_6")+5*col("votes_5")+4*col("votes_4")+3*col("votes_3")+2*col("votes_2")+col("votes_1")) / col("total_votes"))
// MAGIC newRatingsDf = newRatingsDf.withColumn("median_value", medianUDF(col("votes_1"),col("votes_2"),col("votes_3"),col("votes_4"),col("votes_5"),col("votes_6"),col("votes_7"),col("votes_8"),col("votes_9"),col("votes_10")))
// MAGIC 
// MAGIC newRatingsDf = newRatingsDf.withColumn("mean_diff_1", col("weighted_average_vote") - col("mean_vote")) \
// MAGIC                 .withColumn("mean_diff_2", col("weighted_average_vote") - col("mean_value")) \
// MAGIC                 .withColumn("median_diff_1", col("weighted_average_vote") - col("median_vote")) \
// MAGIC                 .withColumn("median_diff_2", col("weighted_average_vote") - col("median_value"))
// MAGIC 
// MAGIC newRatingsDf = newRatingsDf.withColumn("total_votes_long", col("total_votes").cast("long"))
// MAGIC 
// MAGIC display(newRatingsDf)

// COMMAND ----------

newRatingsDf.select(mean("males_allages_avg_vote"), mean("females_allages_avg_vote")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Kilka słów o SparkUI:
// MAGIC - jobs można podejrzeć joby - ich status, czas trwania i postęp, ilość wczytanych i odczytanych danych
// MAGIC - stages - historie wykonywanych stages oraz jobs (podobne do Jobs)
// MAGIC - storage - zawartość dysku przy partycjonowaniu
// MAGIC - environment - szczegóły środowiska, wartości zmiennych środowiskowych
// MAGIC - executors - podsumowanie egzekutorów - ilść zajętej pamięci, ilość rdzeni, ukończonych jobów
// MAGIC - SQL/DF - czas trwania, jobsy, plany wykonania dla zapytań
// MAGIC - JDBC/ODBC server - aktualnie rozproszone silniki SQL, dane serwoerowe
// MAGIC - structured streaming - aktywna gdy uruchomimy Structured Streaming jobs

// COMMAND ----------


