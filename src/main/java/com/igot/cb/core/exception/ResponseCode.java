package com.igot.cb.core.exception;

import com.igot.cb.common.util.Constants;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Manzarul
 */
public enum ResponseCode {
    UNAUTHORIZED(ResponseMessage.Key.UNAUTHORIZED_USER, ResponseMessage.Message.UNAUTHORIZED_USER),
    INTERNAL_ERROR(ResponseMessage.Key.INTERNAL_ERROR, ResponseMessage.Message.INTERNAL_ERROR),

    OK(200),
    CLIENT_ERROR(400),
    SERVER_ERROR(500);
    private int statusCode;
    /**
     * error code contains String value
     */
    private String errorCode;
    /**
     * errorMessage contains proper error message.
     */
    private String errorMessage;

    /**
     * @param errorCode    String
     * @param errorMessage String
     */
    ResponseCode(String errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    ResponseCode(int responseCode) {
        this.statusCode = responseCode;
    }

    /**
     * This method will provide ResponseCode enum based on error code
     *
     * @param errorCode
     * @return String
     */
    public static ResponseCode getResponse(String errorCode) {
        if (StringUtils.isBlank(errorCode)) {
            return null;
        } else if (Constants.UNAUTHORIZED.equals(errorCode)) {
            return ResponseCode.UNAUTHORIZED;
        } else {
            ResponseCode value = null;
            ResponseCode[] responseCodes = ResponseCode.values();
            for (ResponseCode response : responseCodes) {
                if (errorCode.equals(response.getErrorCode())) {
                    return response;
                }
            }
            return value;
        }
    }

    /**
     * @return
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * @return
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    public int getResponseCode() {
        return statusCode;
    }
}