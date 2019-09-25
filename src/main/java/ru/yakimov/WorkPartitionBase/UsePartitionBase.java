package ru.yakimov.WorkPartitionBase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import ru.yakimov.WorkHDFS.AddNewColumnWithJavaCode;
import ru.yakimov.WorkHDFS.CreateStructTableToCSV;
import ru.yakimov.WorkHDFS.Exceptions.UpdateCountException;
import ru.yakimov.WorkHDFS.InitSingle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UsePartitionBase {
    private static final int STARTING_WORK_COMPANY = 2010;
    private static final int COUNT_METRICAL_STATUS = 2;
    private static final int COUNT_YEARS_WORK_COMPANY = 10;
    private static final int MAX_COUNT_CHILDREN = 4;

    private final SparkSession SPARK;
    private final Runtime RT;
    private final FileSystem FS;

    private final String DATA_BASE = "usersDB";
    private final String DATA_TABLE = "usersBigData";

    private final String UPDATE_TABLE = "usersUpdate";
    private final Path DATA_DIR = new Path(String.format("/parquetData/%s/%s",DATA_BASE,DATA_TABLE));

    private final Path UPDATE_DIR = new Path(String.format("/parquetData/%s/%s",DATA_BASE,UPDATE_TABLE));

    private final String PRIMARY_KAY = "user_id";

    private final String[] PARTITION_FIELDS_ARR = {
            "user_reg_year",
            "user_count_children",
            "user_metrial_status"
    };

    private Dataset<Row> data;
    private Dataset<Row> updateData;

    public UsePartitionBase() throws Exception {

        SPARK = InitSingle.getInstance().getSpark();
        RT = InitSingle.getInstance().getRt();
        FS = InitSingle.getInstance().getFs();

        creatingData();

        loadingUpdateData(UPDATE_DIR);

        updatingData();

        SPARK.read().parquet(getActualDirForData(2015, 2,1)).sort(PRIMARY_KAY).show();


    }

    private void updatingData() throws Exception {

        AddNewColumnWithJavaCode classWithMethods = new AddNewColumnWithJavaCode();   //подтянул методы из прошлого занятия

        updateData.createOrReplaceTempView("updateData");

        Dataset<Row> beforeData =updateData.where("action='update_before'");
        beforeData.show();

        Dataset<Row> afterData = updateData.where("action='update_after'");

        afterData.show();

        if(beforeData.count() !=1 || afterData.count() != 1){
            throw new UpdateCountException("Wrong data");
        }

        Row beforeRow = beforeData.first();
        Row afterRow = afterData.first();

        if(isSameData(beforeRow, afterRow)){

            updatingDataFromDir(classWithMethods, afterRow, true);

        }else{

            updatingDataFromDirsDifferent(classWithMethods,beforeRow,afterRow);

        }
    }

    private void updatingDataFromDirsDifferent(AddNewColumnWithJavaCode classWithMethods, Row beforeRow, Row afterRow) throws Exception {
        updatingDataFromDir(classWithMethods, beforeRow, false);
        updatingDataFromDir(classWithMethods, afterRow, true);
    }



    private void updatingDataFromDir(AddNewColumnWithJavaCode classWithMethods, Row row, boolean isAddRow) throws Exception {
        String actualDir = getActualDirForData( getPartitionDataFromRow(row) );

        Dataset<Row> usingData = SPARK
                                    .read()
                                    .parquet(actualDir)
                                    .where(String.format(
                                                        "%s!=%s"
                                                        ,PRIMARY_KAY
                                                         ,row.get(row.schema().fieldIndex(PRIMARY_KAY))
                                                        )
                                    )
               .sort(PRIMARY_KAY) .persist(StorageLevel.MEMORY_AND_DISK());
        usingData.show();


        if(isAddRow) {
            StructType type = usingData.schema();

            usingData = usingData.union(
                            classWithMethods.conversionDataForType(Collections.singletonList(row), type)
            )
                    .sort(PRIMARY_KAY).persist(StorageLevel.MEMORY_AND_DISK());
        }

        usingData.show();



        writeActualData(actualDir, usingData);
    }


    private void writeActualData(String actualDir, Dataset<Row> usingData) throws IOException {

        deleteHadoopDirectory(new Path(actualDir));

        usingData.write().parquet(actualDir);

    }


    private Object[] getPartitionDataFromRow(Row afterRow) {

        Object[] objArr = new Object[PARTITION_FIELDS_ARR.length];

        for (int i = 0; i < PARTITION_FIELDS_ARR.length ; i++) {
            objArr[i] = afterRow.get(afterRow.schema().fieldIndex(PARTITION_FIELDS_ARR[i]));
        }
        return objArr;
    }

    private boolean isSameData(Row beforeRow, Row afterRow){
        boolean res = true;

        for (String field : PARTITION_FIELDS_ARR) {
            res &= beforeRow.get(
                    beforeRow.schema().fieldIndex(field)
            )
                    .equals(
                            afterRow.schema().fieldIndex(field)
                    );
        }

        return  res;
    }

    private void loadingUpdateData(Path dirPath) throws IOException, InterruptedException {

        sqoopImportTableToHDFS(DATA_BASE, UPDATE_TABLE, dirPath);
        updateData = SPARK
                .read()
                .parquet(dirPath.toString())
                .persist(StorageLevel.MEMORY_AND_DISK());
        updateData.show();

    }


    private void creatingData() throws Exception {

        sqoopImportTableToHDFS(DATA_BASE, DATA_TABLE, DATA_DIR);
        data = SPARK
                .read()
                .parquet(String.format("%s/*.parquet",DATA_DIR.toString()));


        data.show();
        data.printSchema();

        data = SPARK
                .createDataFrame(rowsGenerator(),data.schema())
                .union(data)
                .sort(PRIMARY_KAY)
                .persist(StorageLevel.MEMORY_AND_DISK());
        data.show();

        writePartitionData();

        data = null;

        data = SPARK
                .read()
                .parquet(getActualDirForData(2015,1,1))
                .sort(PRIMARY_KAY);
        System.out.println(data.count());
        data.show();

    }

    private String getActualDirForData(Object ... args) throws Exception {
        StringBuilder sb = new StringBuilder(DATA_DIR.toString());

        if(args.length != PARTITION_FIELDS_ARR.length)
            throw new Exception("Not same args");

        for (int i = 0; i < args.length; i++) {
           sb.append("/");
           sb.append(PARTITION_FIELDS_ARR[i]);
           sb.append("=");
           sb.append(args[i]);
        }
        return sb.toString();
    }

    private void writePartitionData() throws IOException {
        deleteHadoopDirectory(DATA_DIR);
        data.write()
                .partitionBy(PARTITION_FIELDS_ARR)
                .parquet(DATA_DIR.toString());
    }

    private boolean deleteHadoopDirectory(Path path) throws IOException {
        if(FS.exists(path)){
            FS.delete(path, true);
            return true;
        }
        return false;
    }

    private List<Row> rowsGenerator(){
        List<Row> resList = new ArrayList<>();
        Row typicalRow = data.first();
        for (int i = 10; i < 100000; i++) {
            resList.add(
                    RowFactory.create(
                            getRowDataArr(typicalRow, i)
                    )
            );
        }
        return resList;

    }

    private Object[] getRowDataArr(Row typicalRow, int rowIndex) {

        final String USER_REG_YEAR = "user_reg_year";
        final String USER_COUNT_CHILDREN = "user_count_children";
        final String USER_METRICAL_STATUS = "user_metrial_status";


        StructType structType = typicalRow.schema();
        Object [] rowDataArr = new Object[structType.fields().length];
        for (int j = 0; j < rowDataArr.length; j++) {
            Object obj = typicalRow.get(j);
            if(obj instanceof String){

                rowDataArr[j] = generateString(rowIndex, (String) obj);

            }else if(obj instanceof Integer){

                if(j == structType.fieldIndex(USER_REG_YEAR)) {
                    rowDataArr[j] = STARTING_WORK_COMPANY + ((int)(Math.random() * COUNT_YEARS_WORK_COMPANY));
                }else if(j == structType.fieldIndex(USER_METRICAL_STATUS)){
                    rowDataArr[j] = (int)(Math.random() * COUNT_METRICAL_STATUS);
                }
                else if(j == structType.fieldIndex(USER_COUNT_CHILDREN)){
                    rowDataArr[j] = (int)(Math.random() * MAX_COUNT_CHILDREN);
                }
                else
                    rowDataArr[j] = ((Integer) obj)+rowIndex;

            }
            else{
                rowDataArr[j] = obj;
            }
        }
        return rowDataArr;
    }

    private String generateString(int rowIndex, String obj) {
        return obj +rowIndex;
    }

    private void sqoopImportTableToHDFS(String nameDB, String tableName, Path compileToPath) throws IOException, InterruptedException {
        deleteHadoopDirectory(compileToPath);

        System.out.println("Sqoop imports table: " +tableName+" of database " + nameDB+ " to path directory: "+compileToPath.toString() );

        Process process = RT.exec(String.format("sqoop import " +
                "--connect \"jdbc:mysql://localhost:3306/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username \"vladimir\"  " +
                "--password-file /user/sqoop.password " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by user_id  " +
                "--as-parquetfile",nameDB,tableName,compileToPath.toString()));
        process.waitFor();
        CreateStructTableToCSV.printProcessErrorStream(process);

    }

    public static void main(String[] args) {
        try {
            new UsePartitionBase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
