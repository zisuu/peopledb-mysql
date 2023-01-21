package ch.finecloud.peopledb.exception;

public class DataException extends RuntimeException {
    public DataException(String message, Throwable e) {
        super(message);
    }
}