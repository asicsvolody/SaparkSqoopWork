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

public class AddNewColumnWithJavaCode implements Serializable {


    private final SparkSession SPARK;
    private final FileSystem FS;
    private final Runtime RT;

    public static final  String PRIMARY_KEY = "user_id";



    private static final Path NEW_DATA_DIR = new Path("/avroData/newData");
    private static final String NEW_DATA_DB = "newUsersDB";
    private static final String NEW_DATA_TABLE = "newData";


    public AddNewColumnWithJavaCode() {
        SPARK = InitSingle.getInstance().getSpark();

        FS = InitSingle.getInstance().getFs();

        RT = InitSingle.getInstance().getRt();

    }

    public void getDataFromTable() throws IOException, InterruptedException {

        Dataset<Row> data = getDataFromMySql(SparkWorker.WORK_DB, SparkWorker.WORK_TABLE, SparkWorker.USER_DIR_PATH);
        StructType dataType = data.first().schema();

        Dataset<Row> newData = getDataFromMySql(NEW_DATA_DB, NEW_DATA_TABLE, NEW_DATA_DIR);
        StructType newDataType = newData.first().schema();

        StructField[] newFields = getNewFields(dataType, newDataType);

        if(newFields != null){
            System.out.println(dataType.toString());

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

        System.out.println("Читаю из записанного: ");

        getDatasetFromDir(SparkWorker.USER_DIR_PATH.toString()).show();

    }

    private StructType getNewStructType(StructType dataType, StructField[] newFields) {
        StructField[] oldFields = dataType.fields();
        dataType = new StructType(ArrayUtils.addAll(newFields,oldFields));
        return dataType;
    }

    public Dataset<Row> conversionDataForType(List<Row> myDataList, StructType dataType) {
        List<Row> resList = new ArrayList<>();
        for (Row row : myDataList) {
            resList.add(getNewRowForType(row, dataType));
        }

        return SPARK.createDataFrame(resList, dataType);

    }

    private Row getNewRowForType(Row row, StructType dataType) {
        String [] fieldNames = dataType.fieldNames();
        Object[] newRowData = new Object[fieldNames.length];

        for (int i = 0; i <newRowData.length ; i++) {
            newRowData[i] = (isFieldWithName(fieldNames[i], row))
                    ? getDataFromField(row, fieldNames[i])
                    : null;
        }
        return createRowWithSchema(newRowData , dataType);
    }

    private Row createRowWithSchema(Object[] newRowData, StructType structType) {
        Row row = RowFactory.create(newRowData);
        return SPARK.
                createDataFrame(Collections.singletonList(row), structType)
                .first();
    }

    private Object getDataFromField(Row row, String fieldName) {
        int index = row.schema().fieldIndex(fieldName);
        return row.get(index);
    }

    private boolean isFieldWithName(String fieldName, Row row) {
        for (StructField field : row.schema().fields()) {
            if(field.name().equals(fieldName))
                return true;
        }
        return false;
    }



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
        sqoopImportTableToHDFS(db, table, dirPath);
        data = getDatasetFromDir(dirPath.toString());
        data.show();
        return data;
    }

    private void sqoopImportTableToHDFS(String nameDB, String tableName, Path compileToPath) throws IOException, InterruptedException {
        if(FS.exists(compileToPath))
            FS.delete(compileToPath, true);
        System.out.println("Sqoop imports table: " +tableName+" of database " + nameDB+ " to path directory: "+compileToPath.toString() );

        RT.exec(String.format("sqoop import " +
                "--connect \"jdbc:mysql://localhost:3306/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username \"vladimir\"  " +
                "--password-file /user/sqoop.password " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by user_id  " +
                "--as-avrodatafile",nameDB,tableName,compileToPath.toString()))
                .waitFor();
    }

    private Dataset<Row> getDatasetFromDir(String userDirPath) {

        System.out.println("InitSingle read data from dir: "+userDirPath);

        return SPARK.read().option("header", true).option("inferSchema", true).format("avro").load(userDirPath + "/*.avro");
    }

    private void saveToHDFS(Path dirPath, Dataset<Row> data)  throws IOException {

        System.out.println("InitSingle write Dataset to HDFS path: "+dirPath.toString());

        if(FS.exists(dirPath))
            FS.delete(dirPath, true);


        data.write().option("header", true).option("inferSchema", true).format("avro").save(dirPath.toString());

    }


    public static void main(String[] args) {
        try {
            new AddNewColumnWithJavaCode().getDataFromTable();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
