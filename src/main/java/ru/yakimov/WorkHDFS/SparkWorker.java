package ru.yakimov.WorkHDFS;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import ru.yakimov.WorkHDFS.Exceptions.MoreOneUserWithIdException;
import ru.yakimov.WorkHDFS.Exceptions.NotDirectoryException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SparkWorker {

    SparkSession spark;

    Dataset<Row> data;

    Runtime rt ;

    public static final String USER_DIR_PATH = "/avroData/usersDB";
    public static final String NEW_USERS_DIR_PATH = "/avroData/newUsersDB";

    private StructType structType;


    private final String PRIMARY_KEY_FILD_NAME = "user_id";
    private final String ACTION_FIELD = "user_action";



    public static final String WORK_TABLE = "users";
    public static final String WORK_DB = "usersDB";



    public static final String NEW_DATA_TEBLE = "newUsers";
    public static final String NEW_DATA_DB = "newUsersDB";




    public SparkWorker() throws FileNotFoundException, NotDirectoryException {

        SparkContext context = new SparkContext(new SparkConf().setAppName("spark-App").setMaster("local[*]")
                .set("spark.hadoop.fs.default.name", "hdfs://localhost:8020").set("spark.hadoop.fs.defaultFS", "hdfs://localhost:30050")
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));
        context.setLogLevel("WARN");
//        FileSystem hdfs = FileSystem.get()
        spark = SparkSession.builder().sparkContext(context).getOrCreate();

        rt = Runtime.getRuntime();

//        try {
//            if(new File(USER_DIR_PATH).exists())
//                deleteDir(USER_DIR_PATH);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        try {
            readTableToHDFS(WORK_DB,WORK_TABLE, USER_DIR_PATH);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        try{
            readTableToHDFS(NEW_DATA_DB, NEW_DATA_TEBLE, NEW_USERS_DIR_PATH);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }


        data = getDatasetFromDir(USER_DIR_PATH);
        structType = data.first().schema();
        data.show();

        usingNewDataFromDir(NEW_USERS_DIR_PATH);

        data.show();


//        Dataset<Row> newUsers = getDatasetFromDir(NEW_USERS_DIR_PATH);
//        newUsers.show();



    }

    private Dataset<Row> getDatasetFromDir(String userDirPath) {
        return spark.read().option("header", true).option("inferSchema", true).format("avro").load(userDirPath + "/*.avro");
    }

    private void usingNewDataFromDir(String dirPath) throws FileNotFoundException, NotDirectoryException {
        Dataset<Row> newData = null;
        List<Row> newRows = new ArrayList<>();

        newData = getDatasetFromDir(dirPath);

        newData.show();

        List<Row> rowsForUpdate = newData.collectAsList();

        for (Row newDataRow : rowsForUpdate) {
            switch (getDataFromField(newDataRow , ACTION_FIELD)){
                case "D" :
                    deleteThisLine(getPrimaryValue(newDataRow));
                    break;
                case "I" :

                    try {
                        newRows.add(insertRow(newDataRow));

                    } catch (MoreOneUserWithIdException e) {
                        e.printStackTrace();
                    }

                    break;
                case "U" :
                    try {
                        newRows.add(updateRow(newDataRow));
                        deleteThisLine(getPrimaryValue(newDataRow));

                    } catch (MoreOneUserWithIdException e) {
                        e.printStackTrace();
                    }
                    break;
            }
        }

        addToData(newRows);
//        save();

    }
    private Row insertRow(Row row) throws MoreOneUserWithIdException {
        if(isLineWithPrimaryKay(getPrimaryValue(row))){
            throw new MoreOneUserWithIdException(getPrimaryValue(row)+ " is used");
        }
        return getNewRow(row);

    }

    private String getPrimaryValue(Row row){
        return row
                .getString(row
                        .schema()
                        .fieldIndex(PRIMARY_KEY_FILD_NAME));
    }

    private void addToData(List<Row> newRows) {
        data = spark
                .createDataFrame(newRows, structType)
                .union(data)
                .sort(PRIMARY_KEY_FILD_NAME)
                .persist(StorageLevel.MEMORY_AND_DISK());
    }

    private Row createRowWithCentralSchema(String[] data){
        Row row = RowFactory.create(data);
        return spark.
                createDataFrame(Collections.singletonList(row), structType)
                .first();
    }


    private Row updateRow(Row updDataRow) throws MoreOneUserWithIdException {

        Row resRow = getNewRow(updDataRow);

        Row updatingRow = getRowForPrimaryKey(getPrimaryValue(updDataRow));

        if(updatingRow == null) {
            return resRow;
        }

        String [] newData = getNewDataFromTwoRows(resRow, updatingRow);

        return RowFactory.create(newData);

    }

    private String[] getNewDataFromTwoRows(Row newRow, Row oldRow) {
        String [] resData = oldRow.schema().fieldNames();
        for (int i = 0; i < resData.length; i++) {
            String fildData = getDataFromField(newRow , resData[i]);
            resData[i] = (fildData == null)
                    ? getDataFromField(oldRow , resData[i])
                    : fildData;
        }

        return resData;

    }

    private String getDataFromField(Row row, String fieldName) {
        int index = row.schema().fieldIndex(fieldName);
        return row.getString(index);
    }

    private Row getRowForPrimaryKey(String primaryValue) throws MoreOneUserWithIdException {
        data.createOrReplaceTempView("users");

        Dataset<Row> oneUser= spark.sql(String.format("SELECT * FROM users WHERE %s = %s", PRIMARY_KEY_FILD_NAME,primaryValue));
        if(oneUser.count() == 0)
            return null;
        if(oneUser.count() >1)
            throw new MoreOneUserWithIdException(PRIMARY_KEY_FILD_NAME + " = "+ primaryValue);
        return oneUser.first();
    }


    private void deleteThisLine(String value){
        data.createOrReplaceTempView("users");
        data = spark.sql(String.format("SELECT * FROM users WHERE %s != %s", PRIMARY_KEY_FILD_NAME, value)).persist(StorageLevel.MEMORY_AND_DISK());
    }

    private Row getNewRow(Row dataRow) {

        String [] newRowData = data.schema().fieldNames();
        for (int i = 0; i <newRowData.length ; i++) {
            newRowData[i] = (isFieldWithName(newRowData[i], dataRow))
                    ? getDataFromField(dataRow, newRowData[i])
                    : null;
        }

        return createRowWithCentralSchema(newRowData);
    }

    private boolean isLineWithPrimaryKay(String primaryValue) throws MoreOneUserWithIdException {
        return  getRowForPrimaryKey(primaryValue) != null;
    }

    private boolean isFieldWithName(String fieldName, Row row) {
        return row.schema().fieldIndex(fieldName)>= 0;
    }


    public void readTableToHDFS(String nameDB, String tableName, String compileToPath) throws IOException, InterruptedException {

        rt.exec(String.format("sqoop import " +
                "--connect \"jdbc:mysql://localhost:3306/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username \"vladimir\"  " +
                "--password-file /user/sqoop.password " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by user_id  " +
                "--as-avrodatafile",nameDB,tableName,compileToPath)).waitFor();
    }



    public void deleteDir(String pathDir) throws IOException {
        System.out.println(rt.exec(String.format("hadoop fs -rm -R %s",pathDir)));

    }


    public static void main(String[] args) {
        try {
            new SparkWorker();

        } catch (FileNotFoundException | NotDirectoryException e) {
            e.printStackTrace();
        }
    }
}
