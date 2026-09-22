package com.igot.cb.core.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;


import static org.junit.jupiter.api.Assertions.*;

class CustomExceptionTest {

    @Test
    void testNoArgsConstructor() {
        CustomException ex = new CustomException();
        assertNull(ex.getCode());
        assertNull(ex.getMessage());
        assertNull(ex.getHttpStatusCode());
    }

    @Test
    void testAllArgsConstructor() {
        CustomException ex = new CustomException("ERR001", "Something went wrong", HttpStatus.BAD_REQUEST);
        assertEquals("ERR001", ex.getCode());
        assertEquals("Something went wrong", ex.getMessage());
        assertEquals(HttpStatus.BAD_REQUEST, ex.getHttpStatusCode());
    }
}
