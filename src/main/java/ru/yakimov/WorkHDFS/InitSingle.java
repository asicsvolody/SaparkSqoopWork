package ru.yakimov.WorkHDFS;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class InitSingle {
    private static volatile InitSingle instance;

    private SparkSession spark;
    private FileSystem fs;
    private Runtime rt;

    public InitSingle() {
        SparkContext context = new SparkContext(new SparkConf().setAppName("spark-App").setMaster("local[*]")
                .set("spark.hadoop.fs.default.name", "hdfs://localhost:8020")
                .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:30050")
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));

        context.setLogLevel("WARN");
        this.spark = SparkSession.builder().sparkContext(context).getOrCreate();
        this.rt = Runtime.getRuntime();

        try {
            this.fs = FileSystem.get(context.hadoopConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static InitSingle getInstance() {
        InitSingle localInstance = instance;
        if(localInstance == null){
            synchronized (InitSingle.class){
                localInstance = instance;
                if(localInstance == null){
                    localInstance = instance = new InitSingle();
                }
            }
        }
        return  localInstance;
    }


    public SparkSession getSpark() {
        return spark;
    }

    public FileSystem getFs() {
        return fs;
    }

    public Runtime getRt() {
        return rt;
    }
}
