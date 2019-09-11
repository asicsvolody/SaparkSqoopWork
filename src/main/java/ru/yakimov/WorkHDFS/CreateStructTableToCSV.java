package ru.yakimov.WorkHDFS;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;


public class CreateStructTableToCSV implements Serializable {


    public static final String TITLE_DB = "usersDB";
    public static final String TITLE_TABLE = "users";

    public static final Path EXPORT_CSV_DIR = new Path("/CsvData/users");
    public static final Path IMPORT_AVRO_STRUCT_TABLE = new Path("/avroData/usersStruct");
    public static final String SAVE_TO_STRING_PATH = EXPORT_CSV_DIR.toString()+"/data.csv";

    public static final String SPLIT = ";";

    Runtime rt;

    FileSystem fs;

    SparkSession spark;





    public CreateStructTableToCSV() throws IOException, InterruptedException {

        spark = SparkSession
                .builder()
                .appName("Create CSV")
                .config("spark.master", "local")
                .config("spark.hadoop.fs.default.name", "hdfs://localhost:8020")
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
                .config("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .config("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .config("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName())
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

        rt  = Runtime.getRuntime();

//        sqoopImportFieldsTable(IMPORT_AVRO_STRUCT_TABLE ,TITLE_DB, TITLE_TABLE);
        sqoopImportFieldsTableWithProcessBuilder(IMPORT_AVRO_STRUCT_TABLE ,TITLE_DB, TITLE_TABLE);


        writeAvroToCsvWithSplit();
       readCsvFile();


    }

    public void writeAvroToCsvWithSplit() throws IOException {

        if(fs.exists(EXPORT_CSV_DIR))
            fs.delete(EXPORT_CSV_DIR, true);

        spark.read()
                .format("avro")
                .load(IMPORT_AVRO_STRUCT_TABLE.toString()+"/*.avro")
                .write()
                .format("csv")
                .option("delimiter", ";")
                .save(SAVE_TO_STRING_PATH);
    }

    private void readCsvFile(){
       spark.read()
               .format("csv")
               .option("delimiter", ";")
               .load(SAVE_TO_STRING_PATH).show();
    }

    public void sqoopImportFieldsTable(Path dirTo, String nameDB, String tableName) throws IOException, InterruptedException {
        
        if(fs.exists(dirTo))
            fs.delete(dirTo, true);
        String str = String.format("sqoop import " +
                "--connect \"jdbc:mysql://localhost:3306/INFORMATION_SCHEMA?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username vladimir " +
                "--password-file /user/sqoop.password " +
                "--target-dir %s " +
                "--table COLUMNS " +
                "--split-by COLUMN_NAME " +
                "--columns TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH " +
                "--where \"TABLE_NAME='%s' AND TABLE_SCHEMA='%s'\" " +
                "--as-avrodatafile", dirTo.toString(), tableName, nameDB);
        System.out.println(str);
        
        Process process;
        process = rt.exec(str);
        process.waitFor();
        printProcessErrorStream(process);
    }

    public void sqoopImportFieldsTableWithProcessBuilder(Path dirTo, String nameDB, String tableName) throws IOException, InterruptedException {
        if(fs.exists(dirTo))
            fs.delete(dirTo, true);
        ProcessBuilder builder = new ProcessBuilder(
                "sqoop"
                ,"import"
                ,"--connect","jdbc:mysql://localhost:3306/INFORMATION_SCHEMA?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL"
                ,"--username","vladimir"
                ,"--password-file","/user/sqoop.password"
                ,"--target-dir",dirTo.toString()
                ,"--table","COLUMNS"
                ,"--split-by","COLUMN_NAME"
                ,"--columns","TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH"
                ,"--where",String.format("TABLE_NAME='%s' AND TABLE_SCHEMA='%s'",tableName,nameDB)
                ,"--as-avrodatafile"// We handle word splitting
                );
        Process process = builder.start();
        process.waitFor();
        printProcessErrorStream(process);

    }



    public static void printProcessErrorStream(Process process){
        String line;
        try
        {
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while((line = input.readLine()) != null)
            {
                System.out.println(line);
            }
            input.close();
        }

        catch(Exception e)
        {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        try {
            new CreateStructTableToCSV();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
