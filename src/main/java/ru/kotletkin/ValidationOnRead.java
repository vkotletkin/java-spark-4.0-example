package ru.kotletkin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ValidationOnRead {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CSV Validation on Read")
                .master("local[*]")
                .getOrCreate();

        // Читаем CSV без валидации
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "PERMISSIVE")
                .csv("s3a://your-bucket/data.csv");

        // Валидация и добавление столбцов за один проход
        Dataset<Row> validatedDF = df.select(
                col("*"), // Все оригинальные колонки

                // Валидационные флаги
                when(col("email").isNotNull()
                        .and(col("email").rlike("^[A-Za-z0-9+_.-]+@(.+)$")), true)
                        .otherwise(false).alias("is_email_valid"),

                when(col("age").isNotNull()
                        .and(col("age").between(18, 100)), true)
                        .otherwise(false).alias("is_age_valid"),

                when(col("salary").isNotNull()
                        .and(col("salary").gt(0)), true)
                        .otherwise(false).alias("is_salary_valid"),

                when(col("name").isNotNull()
                        .and(length(col("name")).between(2, 100)), true)
                        .otherwise(false).alias("is_name_valid"),

                // Вычисляемые поля на основе валидации
                when(col("age").between(18, 30), "YOUNG")
                        .when(col("age").between(31, 50), "MIDDLE")
                        .when(col("age").gt(50), "SENIOR")
                        .otherwise("UNKNOWN").alias("age_group"),

                // Категория зарплаты
                when(col("salary").lt(50000), "LOW")
                        .when(col("salary").between(50000, 100000), "MEDIUM")
                        .when(col("salary").gt(100000), "HIGH")
                        .otherwise("NOT_SET").alias("salary_category"),

                // Общий флаг валидности строки
                when(col("email").isNotNull()
                        .and(col("email").rlike("^[A-Za-z0-9+_.-]+@(.+)$"))
                        .and(col("age").between(18, 100))
                        .and(col("salary").gt(0))
                        .and(col("name").isNotNull()), true)
                        .otherwise(false).alias("is_row_valid")
        );

        // Разделяем на корректные/некорректные
        Dataset<Row> validRows = validatedDF
                .filter(col("is_row_valid").equalTo(true));

        Dataset<Row> invalidRows = validatedDF
                .filter(col("is_row_valid").equalTo(false));

        // Показываем результаты
        System.out.println("Валидированный DataFrame с новыми столбцами:");
        validatedDF.show(10, false);

        System.out.println("\nКорректные строки: " + validRows.count());
        System.out.println("Некорректные строки: " + invalidRows.count());

        // Анализ ошибок
        System.out.println("\nАнализ ошибок:");
        invalidRows.groupBy(
                col("is_email_valid"),
                col("is_age_valid"),
                col("is_salary_valid"),
                col("is_name_valid")
        ).count().orderBy(col("count").desc()).show();
    }
}