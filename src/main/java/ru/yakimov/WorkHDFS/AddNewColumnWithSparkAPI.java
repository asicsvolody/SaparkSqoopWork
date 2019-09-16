package ru.yakimov.WorkHDFS;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class AddNewColumnWithSparkAPI implements Serializable {

    private final SparkSession SPARK;
    private final FileSystem FS;
    private final Runtime RT;

    private static final  String PRIMARY_KEY = "user_id";

    private StructType dataType;



    private static final Path NEW_DATA_DIR = new Path("/avroData/newData");
    private static final String NEW_DATA_DB = "newUsersDB";
    private static final String NEW_DATA_TABLE = "newData";


    private AddNewColumnWithSparkAPI() throws IOException {
        SparkContext context = new SparkContext(new SparkConf().setAppName("spark-App").setMaster("local[*]")
                .set("spark.hadoop.fs.default.name", "hdfs://localhost:8020")
                .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:30050")
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));

        context.setLogLevel("WARN");

        SPARK = SparkSession.builder().sparkContext(context).getOrCreate();

        RT = Runtime.getRuntime();

        FS = FileSystem.get(context.hadoopConfiguration());

    }

    private void getDataFromTable() throws IOException, InterruptedException {

        Dataset<Row> data = getDataFromMySql(SparkWorker.WORK_DB, SparkWorker.WORK_TABLE, SparkWorker.USER_DIR_PATH);
        dataType = data.first().schema();

        Dataset<Row> newData = getDataFromMySql(NEW_DATA_DB, NEW_DATA_TABLE, NEW_DATA_DIR);

        StructType newDataType = newData.first().schema();
        newDataType.printTreeString();

        dataType = getNewStructType(dataType, newDataType);


        data = data.join(
                        newData
                        , data.col(PRIMARY_KEY).equalTo(newData.col(PRIMARY_KEY))
                        , "left")
                    .select(getUsingCols(data, newData, dataType))
                .union(
                        newData.join(
                                    data
                                    ,newData.col(PRIMARY_KEY).equalTo(data.col(PRIMARY_KEY))
                                    ,"left")
                                .select(getUsingCols(newData, data, dataType))
                )
                .persist(StorageLevel.MEMORY_AND_DISK());


        data.show();

        saveToHDFS(SparkWorker.USER_DIR_PATH, data);

        System.out.println("Читаю из записанного: ");

        getDatasetFromDir(SparkWorker.USER_DIR_PATH.toString()).show();

    }

    private Column[] getUsingCols(Dataset<Row> dataOne, Dataset<Row>  dataTwo, StructType dataType) {

        List<Column> resCols = new ArrayList<>();
        for (StructField field : dataType.fields()) {
            if(dataOne.first().schema().contains(field)){
                resCols.add(dataOne.col(field.name()));
            }else{
                resCols.add(dataTwo.col(field.name()));
            }
        }
        return resCols.toArray(new Column[0]);
    }


    private StructType getNewStructType(StructType dataType, StructType newDataType) {

        Set<StructField> dataTypeFieldSet = new HashSet<>(Arrays.asList(dataType.fields()));
        List<StructField> newDataList = Arrays.asList(newDataType.fields());

        if(dataTypeFieldSet.containsAll(newDataList))
            return dataType;

        dataTypeFieldSet.addAll(newDataList);
        return new StructType(dataTypeFieldSet.toArray(new StructField[0]));
    }

    private Dataset<Row> getDataFromMySql(String db, String table, Path dirPath) throws IOException, InterruptedException {
        Dataset<Row> data;
        sqoopImportTableToHDFS(db, table, dirPath);
        data = getDatasetFromDir(dirPath.toString());
        data.show();
        return data;
    }

    private void sqoopImportTableToHDFS(String nameDB, String tableName, Path compileToPath) throws IOException, InterruptedException {
        if(FS.exists(compileToPath))
            FS.delete(compileToPath, true);
        System.out.println("Sqoop imports table: " +tableName+" of database " + nameDB+ " to path directory: "+compileToPath.toString() );

        Process process = RT.exec(String.format("sqoop import " +
                "--connect \"jdbc:mysql://localhost:3306/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username \"vladimir\"  " +
                "--password-file /user/sqoop.password " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by user_id  " +
                "--as-avrodatafile",nameDB,tableName,compileToPath.toString()));
        process.waitFor();
        CreateStructTableToCSV.printProcessErrorStream(process);

    }

    private Dataset<Row> getDatasetFromDir(String userDirPath) {

        System.out.println("Spark read data from dir: "+userDirPath);

        return SPARK.read().option("header", true).option("inferSchema", true).format("avro").load(userDirPath + "/*.avro");
    }

    private void saveToHDFS(Path dirPath, Dataset<Row> data)  throws IOException {

        System.out.println("Spark write Dataset to HDFS path: "+dirPath.toString());

        if(FS.exists(dirPath))
            FS.delete(dirPath, true);


        data.write().option("header", true).option("inferSchema", true).format("avro").save(dirPath.toString());

    }


    public static void main(String[] args) {
        try {
            new AddNewColumnWithSparkAPI().getDataFromTable();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
