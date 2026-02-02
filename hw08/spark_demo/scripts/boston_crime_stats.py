import sys
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as spF
from pyspark.sql.types import *

def prepare(df_crime, df_codes):
    """Предварительная обработка данных"""

    # 1. очистка данных по преступлениям
    # -- удаление дубликатов по полю INCIDENT_NUMBER (уникальный код инцидента)
    # -- проверка на NULL в ключевых полях и их фильтрация
    # -- приведение названия района к верхнему регистру
    crime_window = Window.partitionBy("INCIDENT_NUMBER").orderBy(spF.col("OCCURRED_ON_DATE"))
    df_crime_dedup = df_crime.withColumn(
        "row_num",
        spF.row_number().over(crime_window)
    ).filter(
        spF.col("row_num") == 1
    ).drop("row_num")
    df_crime_clean = df_crime_dedup.filter(
        spF.col("DISTRICT").isNotNull() &
        spF.col("OFFENSE_CODE").isNotNull() &
        spF.col("OCCURRED_ON_DATE").isNotNull() &
        spF.col("Lat").isNotNull() &
        spF.col("Long").isNotNull()
    ).withColumn(
        "DISTRICT",
        spF.upper(spF.trim(spF.col("DISTRICT")))
    )

    # 2. очистка данных по кодам
    # -- удаление дубликатов по CODE, берем первую запись
    # -- разбиваем NAME на части для извлечения crime_type
    df_codes_dedup = df_codes.dropDuplicates(
        ["CODE"]
    ).withColumn(
        "OFFENSE_CODE",
        spF.trim(spF.col("CODE"))
    ).drop("CODE")
    df_codes_clean = df_codes_dedup.withColumn(
        "crime_type",
        spF.split(spF.col("NAME"), "-")[0]
    ).withColumn(
        "crime_type",
        spF.trim(spF.col("crime_type"))
    ).select("OFFENSE_CODE", "crime_type")

    # 3. подготовительная обработка
    # -- присоединение справочника
    # -- обогащение данных типом преступления (если тип не нашелся, то ставим значение '--other--')
    # -- формируем год-месяц для расчета месячной статистики
    df_joined = df_crime_clean.join(
        df_codes_clean,
        on="OFFENSE_CODE",
        how="left"
    )
    df_enriched = df_joined.withColumn(
        "crime_type",
        spF.coalesce(spF.col("crime_type"), spF.lit("--other--"))
    ).withColumn(
        "year_month",
        spF.date_format(spF.to_date(spF.col("OCCURRED_ON_DATE"), "yyyy-MM-dd"), "yyyy-MM")
    )
    return df_enriched

def make_data_mart(df):
    """построение итоговой витрины по районам"""

    # 1. общее количество преступлений в районе
    crimes_total_df = df.groupBy("DISTRICT").agg(
        spF.count("*").alias("crimes_total")
    )

    # 2. медиана числа преступлений в месяц в этом районе
    # Сначала считаем преступления по району и месяцу
    monthly_counts = df.groupBy("DISTRICT", "year_month").agg(
        spF.count("*").alias("monthly_crimes")
    ).groupBy("DISTRICT").agg(
        spF.expr("percentile_approx(monthly_crimes, 0.5)").alias("crimes_monthly")
    )

    # 3. три самых частых crime_type в районе
    top_crime_window = Window.partitionBy("DISTRICT").orderBy(spF.col("cnt").desc())
    top_crime_types = df.groupBy("DISTRICT", "crime_type").agg(
        spF.count("*").alias("cnt")
    ).withColumn(
        "rank",
        spF.row_number().over(top_crime_window)
    ).filter(
        spF.col("rank") <= 3
    ).groupBy("DISTRICT").agg(
        spF.concat_ws(", ", spF.collect_list(spF.col("crime_type"))).alias("frequent_crime_types")
    )

    # 4. координаты района, рассчитанные как среднее по всем широтам и долготам инцидентов
    avg_coords = df.groupBy("DISTRICT").agg(
        spF.mean("Lat").alias("lat"),
        spF.mean("Long").alias("lng")
    )

    # собираем всё вместе
    result = crimes_total_df.join(
        monthly_counts,
        on="DISTRICT",
        how="left"
    ).join(
        top_crime_types,
        on="DISTRICT",
        how="left"
    ).join(
        avg_coords,
        on="DISTRICT",
        how="left"
    )

    return result

# --
# --------------------------------
# --

def main(input_path, output_path):
    spark = SparkSession.builder.appName("Boston Crime Statistics").getOrCreate()

    df_crime = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{input_path}/crime.csv")
    df_codes = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{input_path}/offense_codes.csv")

    df_prepared = prepare(df_crime, df_codes)
    df_mart = make_data_mart(df_prepared)

    df_mart.write.mode("overwrite").parquet(f"{output_path}/mart_data.parquet")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: boston_crime_stats.py <input_dir_path> <output_dir_path>")
        sys.exit(1)

    input_path_arg = sys.argv[1]
    output_path_arg = sys.argv[2]
    main(input_path_arg, output_path_arg)
