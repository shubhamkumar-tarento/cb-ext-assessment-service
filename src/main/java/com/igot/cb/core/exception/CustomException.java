package com.igot.cb.core.exception;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
public class CustomException extends RuntimeException{

    private final String code;
    private final String message;
    private final HttpStatus httpStatusCode;

    @Autowired
    public CustomException() {
        this(null, null, null);
    }

    public CustomException(String code, String message, HttpStatus httpStatusCode) {
        this.code = code;
        this.message = message;
        this.httpStatusCode = httpStatusCode;
    }

}
