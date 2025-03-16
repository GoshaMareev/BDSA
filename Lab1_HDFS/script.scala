import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDate

// Загрузка данных
val data = spark.read.option("header", "true").csv("file:///home/hadoop/slb.csv")

// Преобразование строковых столбцов в числовой тип
val dataWithNumericColumns = data.withColumn("Close", col("Close").cast("double"))
                                  .withColumn("High", col("High").cast("double"))
                                  .withColumn("Low", col("Low").cast("double"))
                                  .withColumn("Open", col("Open").cast("double"))
                                  .withColumn("Volume", col("Volume").cast("double"))

// Преобразование столбца Date в формат даты
val dataWithDate = dataWithNumericColumns.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

// 1. Вычисление средних значений
val averages = dataWithNumericColumns.agg(
  avg("Close").as("avg_Close"),
  avg("High").as("avg_High"),
  avg("Low").as("avg_Low"),
  avg("Open").as("avg_Open"),
  avg("Volume").as("avg_Volume")
)
println("Средние значения:")
averages.show()

// Сохранение средних значений в CSV
averages.write.option("header", "true").csv("file:///home/hadoop/output/averages")

// 2. Фильтрация данных за последние 3 года
val threeYearsAgo = LocalDate.now().minusYears(3).toString
val recentData = dataWithDate.filter(col("Date") >= threeYearsAgo)
println("Данные за последние 3 года:")
recentData.show()

// Сохранение данных за последние 3 года в CSV
recentData.write.option("header", "true").csv("file:///home/hadoop/output/recent_data")

// 3. Расчет медианной цены закрытия
val medianClose = dataWithNumericColumns.stat.approxQuantile("Close", Array(0.5), 0.01)(0)
println(s"Медианная цена закрытия: $medianClose")

// Создание DataFrame для медианы
val medianDF = Seq(("Median_Close", medianClose)).toDF("Metric", "Value")

// Сохранение медианы в CSV
medianDF.write.option("header", "true").csv("file:///home/hadoop/output/median_close")

// 4. Группировка по кварталам
val quarterlyData = dataWithDate.withColumn("Year", year(col("Date")))
                                .withColumn("Quarter", quarter(col("Date")))
val groupedData = quarterlyData.groupBy("Year", "Quarter")
                               .agg(avg("Close").as("avg_Close"))
println("Группировка по кварталам:")
groupedData.show()

// Сохранение группировки по кварталам в CSV
groupedData.write.option("header", "true").csv("file:///home/hadoop/output/grouped_data")