package com.citic.helper;


/**
 * The type My runtime exception.
 */
public class MyRuntimeException extends RuntimeException {

    /**
     * Instantiates a new My runtime exception.
     *
     * @param message the message
     */
    public  MyRuntimeException(String message) {
        super(message);
    }

    /**
     * Instantiates a new My runtime exception.
     *
     * @param cause the cause
     */
    public MyRuntimeException(Throwable cause) {
        super(cause);
    }

    /**
     * Instantiates a new My runtime exception.
     *
     * @param message the message
     * @param cause the cause
     */
    public MyRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}

