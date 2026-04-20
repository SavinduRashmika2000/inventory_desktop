package com.sync.exception;

import lombok.Getter;

@Getter
public class SyncException extends RuntimeException {
    
    public enum ErrorType {
        NETWORK_ERROR,
        VALIDATION_ERROR,
        SERVER_ERROR,
        DATABASE_ERROR,
        UNKNOWN_ERROR
    }

    private final ErrorType type;
    private final String tableName;

    public SyncException(String message, ErrorType type, String tableName) {
        super(message);
        this.type = type;
        this.tableName = tableName;
    }

    public SyncException(String message, ErrorType type, String tableName, Throwable cause) {
        super(message, cause);
        this.type = type;
        this.tableName = tableName;
    }
}
