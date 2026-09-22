package com.igot.cb.core.exception;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ApplicationLogicErrorTest {

    @Test
    void testConstructorWithMessage() {
        ApplicationLogicError ex = new ApplicationLogicError("error occurred");
        assertEquals("error occurred", ex.getMessage());
    }

    @Test
    void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        ApplicationLogicError ex = new ApplicationLogicError("error with cause", cause);
        assertEquals("error with cause", ex.getMessage());
        assertEquals(cause, ex.getCause());
    }
}
