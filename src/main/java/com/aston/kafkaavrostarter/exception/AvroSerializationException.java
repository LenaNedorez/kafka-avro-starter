package com.aston.kafkaavrostarter.exception;

public class AvroSerializationException extends RuntimeException{

    public AvroSerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AvroSerializationException(String message){
        super(message);
    }
}
