package ru.yakimov.WorkHDFS.Exceptions;

public class MoreOneUserWithIdException extends Exception {
    public MoreOneUserWithIdException(String selection) {
        super("Not unique id: "+ selection);
    }
}
