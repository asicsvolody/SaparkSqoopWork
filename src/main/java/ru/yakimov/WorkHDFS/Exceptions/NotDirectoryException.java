package ru.yakimov.WorkHDFS.Exceptions;

public class NotDirectoryException extends Exception {
    public NotDirectoryException(String directuryPath) {
        super("it is not directory: "+directuryPath);
    }
}
