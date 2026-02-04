package ru.kotletkin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        var spark = SparkSession.builder()
                .appName("Spark S3 Job")
                .master("local[*]")
                // Для production: используйте IAM Role или переменные окружения
                .config("spark.hadoop.fs.s3a.access.key", "any")
                .config("spark.hadoop.fs.s3a.secret.key", "any")
                // Регион и эндпоинт (для S3-compatible storage можно изменить)
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:8333")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                // Оптимизации для чтения
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .config("spark.hadoop.fs.s3a.connection.maximum", "100")
                // Для Spark 4.x
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();


        String pathS3 = "s3a://spark-test/aster30m_urls.txt";

        Dataset<Row> csvDF = spark.read().text(pathS3);
        long count = csvDF.count();



        csvDF.show();

        System.out.println(count);

// Создаем простой DataFrame
//        String[] data = {
//                "Анна,Разработка,75000",
//                "Иван,Аналитика,82000",
//                "Мария,Разработка,68000",
//                "Алексей,Маркетинг,90000"
//        };
//
//        Dataset<Row> df = spark.read()
//                .option("header", "false")
//                .option("delimiter", ",")
//                .csv(spark.createDataset(Arrays.asList(data), Encoders.STRING()))
//                .toDF("name", "department", "salary")
//                .selectExpr("name", "department", "CAST(salary AS INT) as salary");
//
//        df.show();
//
//        // Регистрируем как таблицу
//        df.createOrReplaceTempView("staff");
//
//        // Выполняем SQL запросы
//        System.out.println("1. Все сотрудники:");
//        spark.sql("SELECT * FROM staff").show();
//
//        System.out.println("2. Средняя зарплата по отделам:");
//        spark.sql("""
//                SELECT
//                    department,
//                    AVG(salary) as avg_salary,
//                    COUNT(*) as count
//                FROM staff
//                GROUP BY department
//                """).show();

        spark.stop();
    }
}