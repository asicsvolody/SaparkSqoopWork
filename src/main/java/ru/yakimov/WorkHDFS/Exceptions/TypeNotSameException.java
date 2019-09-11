package ru.yakimov.WorkHDFS.Exceptions;

import org.apache.spark.sql.types.DataType;

public class TypeNotSameException extends Exception {
    public TypeNotSameException(DataType dataTypeOne, DataType dataTypeTwo) {
        super(String.format("Not same DataType`s %s and %s", dataTypeOne, dataTypeTwo));
    }
}
