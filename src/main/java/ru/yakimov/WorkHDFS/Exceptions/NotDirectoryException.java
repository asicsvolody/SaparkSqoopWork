package ru.yakimov.WorkHDFS.Exceptions;

public class NotDirectoryException extends Exception {
    public NotDirectoryException(String directoryPath) {
        super("it is not directory: "+directoryPath);
    }
}
