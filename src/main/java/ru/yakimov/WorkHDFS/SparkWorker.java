package ru.yakimov.WorkHDFS;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

public class SparkWorker {

    SparkSession spark;

    Dataset<Row> data;

    Runtime rt ;

    public static final String AVRO_DIR_PATH = "/avroData/firstAvro";

    public static final String WORK_TABLE = "users";
    public static final String WORK_DB = "usersDB";



    public static final String NEW_DATA_TEBLE = "newUsers";
    public static final String NEW_DATA_DB = "newUsersDB";




    public SparkWorker() {

        SparkContext context = new SparkContext(new SparkConf().setAppName("spark-App").setMaster("local[*]")
                .set("spark.hadoop.fs.default.name", "hdfs://localhost:8020").set("spark.hadoop.fs.defaultFS", "hdfs://localhost:30050")
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));
        context.setLogLevel("WARN");
        FileSystem hdfs = FileSystem.get()
        spark = SparkSession.builder().sparkContext(context).getOrCreate();

        rt = Runtime.getRuntime();

        try {
            if(new File(AVRO_DIR_PATH).exists())
                deleteDir(AVRO_DIR_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            readTableToHDFS(WORK_DB,WORK_TABLE, AVRO_DIR_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }





        data =spark.read().option("header", true).option("inferSchema", true).format("avro").load(AVRO_DIR_PATH+"/*.avro");
        data.show();



    }


    public void readTableToHDFS(String nameDB, String tableName, String compileToPath) throws IOException {

        rt.exec(String.format("sqoop import " +
                "--connect \"jdbc:mysql://localhost:3306/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username \"vladimir\"  " +
                "--password-file /user/sqoop.password " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by user_id  " +
                "--as-avrodatafile",nameDB,tableName,compileToPath));
    }



    public void deleteDir(String pathDir) throws IOException {
        rt.exec(String.format("hadoop fs -rm -R %s",pathDir));

    }


    public static void main(String[] args) {
        new SparkWorker();
    }
}
