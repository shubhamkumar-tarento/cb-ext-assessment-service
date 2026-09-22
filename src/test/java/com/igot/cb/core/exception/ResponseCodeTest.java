package com.igot.cb.core.exception;

import com.igot.cb.common.util.Constants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ResponseCodeTest {

    @Test
    void testEnumValues() {
        assertEquals("UNAUTHORIZED_USER", ResponseCode.UNAUTHORIZED.getErrorCode());
        assertEquals("You are not authorized.", ResponseCode.UNAUTHORIZED.getErrorMessage());
        assertEquals("INTERNAL_ERROR", ResponseCode.INTERNAL_ERROR.getErrorCode());
        assertEquals("Process failed,please try again later.", ResponseCode.INTERNAL_ERROR.getErrorMessage());
        assertEquals(200, ResponseCode.OK.getResponseCode());
        assertEquals(400, ResponseCode.CLIENT_ERROR.getResponseCode());
        assertEquals(500, ResponseCode.SERVER_ERROR.getResponseCode());
    }

    @Test
    void testGetResponse_NullOrBlank() {
        assertNull(ResponseCode.getResponse(null));
        assertNull(ResponseCode.getResponse(""));
        assertNull(ResponseCode.getResponse("   "));
    }

    @Test
    void testGetResponse_Unauthorized() {
        assertEquals(ResponseCode.UNAUTHORIZED, ResponseCode.getResponse(Constants.UNAUTHORIZED));
    }

    @Test
    void testGetResponse_UnknownErrorCode() {
        // Should return null for unknown error code
        assertNull(ResponseCode.getResponse("UNKNOWN_CODE"));
    }
}
