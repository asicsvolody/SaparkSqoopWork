package ru.yakimov.WorkHDFS;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import ru.yakimov.WorkHDFS.Exceptions.TypeNotSameException;

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

    private void getDataFromTable() throws IOException, InterruptedException, TypeNotSameException {

        Dataset<Row> data = getDataFromMySql(SparkWorker.WORK_DB, SparkWorker.WORK_TABLE, SparkWorker.USER_DIR_PATH);
        dataType = data.first().schema();

        Dataset<Row> newData = getDataFromMySql(NEW_DATA_DB, NEW_DATA_TABLE, NEW_DATA_DIR);
        StructType newDataType = newData.first().schema();
        newDataType.printTreeString();

        StructField[] newFields = getNewFields(dataType, newDataType);

        //####

        JavaRDD<Row> dataRdd = data.javaRDD().map(this::getNewRowForType);
        System.out.println(dataRdd.first().get(5));

        //#####

        if(newFields!= null){

            for (StructField newField : newFields) {
                System.out.println(newField.name());
                dataType = getNewStructType(dataType, newDataType);
                data = conversionDataForType(data.collectAsList());
            }
            data.show();
        }

                data =  data
                .union(conversionDataForType(newData.collectAsList()))
                .sort(PRIMARY_KEY)
                .persist(StorageLevel.MEMORY_AND_DISK());

        data.show();

        saveToHDFS(SparkWorker.USER_DIR_PATH, data);

        System.out.println("Читаю из записанного: ");

        getDatasetFromDir(SparkWorker.USER_DIR_PATH.toString()).show();

    }

    private StructType getNewStructType(StructType dataType, StructType newDataType) {

        Set<StructField> dataTypeFieldSet = new HashSet<>(Arrays.asList(dataType.fields()));
        List<StructField> newDataList = Arrays.asList(newDataType.fields());



        if(dataTypeFieldSet.containsAll(newDataList))
            return dataType;

        dataTypeFieldSet.addAll(newDataList);
        return new StructType(dataTypeFieldSet.toArray(new StructField[0]));
    }

    private StructField[] getNewFields(StructType dataType, StructType newDataType) {

        Set<StructField> newDataTypeHashMap = new HashSet<>(Arrays.asList(newDataType.fields()));
        List<StructField> dataList = Arrays.asList(dataType.fields());
        if(dataList.containsAll(newDataTypeHashMap))
            return null;
        newDataTypeHashMap.removeAll(dataList);
        return newDataTypeHashMap.toArray(new StructField[0]);
    }

    private Dataset<Row> conversionDataForType(List<Row> myDataList) throws TypeNotSameException {
        List<Row> resList = new ArrayList<>();
        for (Row row : myDataList) {

                resList.add(getNewRowForType(row));
        }

        return SPARK.createDataFrame(resList, dataType);

    }

    private Row getNewRowForType(Row row) throws TypeNotSameException {
        StructField [] fieldNames = dataType.fields();
        Object[] newRowData = new Object[fieldNames.length];

        for (int i = 0; i <newRowData.length ; i++) {

            newRowData[i] = getDataFromField(fieldNames[i], row);

        }

        return createRowWithSchema(newRowData);
    }

    private boolean isFieldWithName(StructField fieldForCheck, Row row) throws TypeNotSameException {
        for (StructField field : row.schema().fields()) {

            if(field.name().equals(fieldForCheck.name())){

                if(!field.dataType().equals(fieldForCheck.dataType()))
                    throw new TypeNotSameException(field.dataType(), fieldForCheck.dataType());

                return true;
            }
        }
        return false;
    }

    private Row createRowWithSchema(Object[] newRowData) {
        Row row = RowFactory.create(newRowData);
        return SPARK.
                createDataFrame(Collections.singletonList(row), dataType)
                .first();
    }

    private Object getDataFromField( StructField field, Row row ) throws TypeNotSameException {
        if(isFieldWithName(field, row)) {
            int index = row.schema().fieldIndex(field.name());
            return row.get(index);
        }

        return null;

    }




    private Dataset<Row> getDataFromMySql(String db, String table, Path dirPath) throws IOException, InterruptedException {
        Dataset<Row> data;
//        sqoopImportTableToHDFS(db, table, dirPath);
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

        } catch (IOException | InterruptedException | TypeNotSameException e) {
            e.printStackTrace();
        }
    }
}
