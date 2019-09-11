package ru.yakimov.WorkHDFS;

import org.apache.commons.lang3.ArrayUtils;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AddNewColumn implements Serializable {


    private final SparkSession SPARK;
    private final FileSystem FS;
    private final Runtime RT;

    public static final  String PRIMARY_KEY = "user_id";



    public static final Path NEW_DATA_DIR = new Path("/avroData/newData");
    public static final String NEW_DATA_DB = "newUsersDB";
    public static final String NEW_DATA_TABLE = "newData";


    public AddNewColumn() throws IOException {
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

    public void getDataFromTable() throws IOException, InterruptedException {

        Dataset<Row> data = getDataFromMySql(SparkWorker.WORK_DB, SparkWorker.WORK_TABLE, SparkWorker.USER_DIR_PATH);
        StructType dataType = data.first().schema();

        Dataset<Row> newData = getDataFromMySql(NEW_DATA_DB, NEW_DATA_TABLE, NEW_DATA_DIR);
        StructType newDataType = newData.first().schema();

        StructField[] newFields = getNewFields(dataType, newDataType);

        if(newFields != null){
            dataType = getNewStructType(dataType, newFields);

            System.out.println(dataType.toString());

            data = conversionDataForType(data.collectAsList(), dataType);

            data.show();
        }

        data =  data
                .union(conversionDataForType(newData.collectAsList(), dataType))
                .sort(PRIMARY_KEY)
                .persist(StorageLevel.MEMORY_AND_DISK());
        data.show();

        saveToHDFS(SparkWorker.USER_DIR_PATH, data);

    }

    private StructType getNewStructType(StructType dataType, StructField[] newFields) {
        StructField[] oldFields = dataType.fields();
        dataType = new StructType(ArrayUtils.addAll(newFields,oldFields));
        return dataType;
    }

    private Dataset<Row> conversionDataForType(List<Row> myDataList, StructType dataType) {
        List<Row> resList = new ArrayList<>();
        for (Row row : myDataList) {
            resList.add(getNewRowForType(row, dataType));
        }

        return SPARK.createDataFrame(resList, dataType);

    }

    private Row getNewRowForType(Row row, StructType dataType) {
        String [] newRowData = dataType.fieldNames();
        for (int i = 0; i <newRowData.length ; i++) {
            newRowData[i] = (isFieldWithName(newRowData[i], row))
                    ? getDataFromField(row, newRowData[i])
                    : null;
        }

        return createRowWithCentralSchema(newRowData , dataType);
    }

    private Row createRowWithCentralSchema(String[] newRowData, StructType structType) {
        Row row = RowFactory.create(newRowData);
        return SPARK.
                createDataFrame(Collections.singletonList(row), structType)
                .first();
    }

    private String getDataFromField(Row row, String fieldName) {
        int index = row.schema().fieldIndex(fieldName);
        return row.getString(index);
    }

    private boolean isFieldWithName(String fieldName, Row row) {
        for (StructField field : row.schema().fields()) {
            if(field.name().equals(fieldName))
                return true;
        }
        return false;
    }

//    private List<Row>  getRos



    private StructField[] getNewFields(StructType dataType, StructType newDataType) {
        StructField[] newDataFields = newDataType.fields();
        List<StructField> structFieldList = new ArrayList<>();
        for (StructField newDataField : newDataFields) {
            if(!isThisField(newDataField, dataType)){
                structFieldList.add(newDataField);
            }
        }
        return (structFieldList.isEmpty())?null: structFieldList.toArray(new StructField[0]);
    }

    private boolean isThisField(StructField newDataField, StructType dataType) {
        StructField[] dataFields = dataType.fields();
        for (StructField dataField : dataFields) {
            if(dataField.equals(newDataField))
                return true;
        }
        return false;
    }

    private Dataset<Row> getDataFromMySql(String db, String table, Path dirPath) throws IOException, InterruptedException {
        Dataset<Row> data;
        sqoopImportTableToHDFS(db, table, dirPath.toString());
        data = getDatasetFromDir(dirPath.toString());
        data.show();
        return data;
    }

    private void sqoopImportTableToHDFS(String nameDB, String tableName, String compileToPath) throws IOException, InterruptedException {

        System.out.println("Sqoop imports table: " +tableName+" of database " + nameDB+ " to path directory: "+compileToPath );

        RT.exec(String.format("sqoop import " +
                "--connect \"jdbc:mysql://localhost:3306/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username \"vladimir\"  " +
                "--password-file /user/sqoop.password " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by user_id  " +
                "--as-avrodatafile",nameDB,tableName,compileToPath))
                .waitFor();
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
            new AddNewColumn().getDataFromTable();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
