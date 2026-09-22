package com.igot.cb.core.exception;

/**
 * This class will hold all the response key and message
 *
 * @author Juhi
 */
public final class ResponseMessage {

    private ResponseMessage() {
    }

    public static final class Message {

        private Message() {
        }

        public static final String UNAUTHORIZED_USER = "You are not authorized.";
        public static final String INTERNAL_ERROR = "Process failed,please try again later.";
    }

    public static final class Key {

        private Key() {
        }

        public static final String UNAUTHORIZED_USER = "UNAUTHORIZED_USER";
        public static final String INTERNAL_ERROR = "INTERNAL_ERROR";
    }
}
