package ru.yakimov.WorkHDFS;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import ru.yakimov.WorkHDFS.Exceptions.MoreOneUserWithIdException;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class AddNewColumn implements Serializable {

    private final SparkWorker SW;

    private final SparkSession SPARK;
    private final FileSystem FS;
    private final Runtime RT;

    public static final  String PRIMARY_KEY = "user_id";


    public static final Path NEW_DATA_DIR = new Path("/avroData/newData");
    public static final String NEW_DATA_DB = "newUsersDB";
    public static final String NEW_DATA_TABLE = "newData";


    public AddNewColumn() throws IOException {
        SW = new SparkWorker();
        this.SPARK = SW.getSpark();
        this.FS = SW.getFs();
        this.RT = SW.getRt();
    }

    public void getDataFromTable() throws IOException, InterruptedException, MoreOneUserWithIdException {

        Dataset<Row> data = getDataFromMySql(SparkWorker.WORK_DB, SparkWorker.WORK_TABLE, SparkWorker.USER_DIR_PATH);
        StructType dataType = data.first().schema();

        Dataset<Row> newData = getDataFromMySql(NEW_DATA_DB, NEW_DATA_TABLE, NEW_DATA_DIR);
        StructType newDataType = newData.first().schema();


        List<Row> rowsForRecord = new ArrayList<>();

        List<Row> newDataRows = newData.collectAsList();


        StructField[] newFields = getNewFields(dataType, newDataType);

        if(newFields != null){
            StructField[] oldFields = dataType.fields();
            StructType resType = new StructType(ArrayUtils.addAll(newFields,oldFields));
            System.out.println(resType.toString());


//             List<Row> dataList= data
//                     .toJavaRDD()
//                    .map(row -> SW.insertRow(row, PRIMARY_KEY, dataType)).collect();
//
//            newDataRows.addAll(dataList);


            for (Row newDataRow : newDataRows) {
                rowsForRecord.add(SW.insertRow(newDataRow, PRIMARY_KEY, resType, SparkWorker.WORK_TABLE, data));
            }

            data = SPARK.createDataFrame(rowsForRecord, resType);
            data.show();
        }else{

            for (Row newDataRow : newDataRows) {
                rowsForRecord.add(SW.insertRow(newDataRow, PRIMARY_KEY, dataType, SparkWorker.WORK_TABLE,data));
            }

            data = SPARK.createDataFrame(rowsForRecord, dataType).union(data);

            data.show();
        }

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
//        SW.sqoopImportTableToHDFS(db, table, dirPath);
        data = SW.getDatasetFromDir(dirPath.toString());
        data.show();
        return data;
    }



    public static void main(String[] args) {
        try {
            new AddNewColumn().getDataFromTable();

        } catch (IOException | InterruptedException | MoreOneUserWithIdException e) {
            e.printStackTrace();
        }
    }
}
