package com.citic.source.canal;


class ServerUrlsFormatException extends Exception {
    private static final long serialVersionUID = 1L;

    ServerUrlsFormatException(String msg) {
        super(msg);
    }

    ServerUrlsFormatException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
