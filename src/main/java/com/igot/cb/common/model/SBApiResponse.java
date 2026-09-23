package com.igot.cb.common.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatus;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
public class SBApiResponse {

    private String id;
    private String ver;
    private String ts;
    private SunbirdApiRespParam params;
    private HttpStatus responseCode;

    @Getter(lombok.AccessLevel.NONE)
    @Setter(lombok.AccessLevel.NONE)
    private Map<String, Object> response = new HashMap<>();

    public SBApiResponse() {
        this.ver = "v1";
        this.ts = new Timestamp(System.currentTimeMillis()).toString();
        this.params = new SunbirdApiRespParam(UUID.randomUUID().toString());
    }

    public SBApiResponse(String id) {
        this();
        this.id = id;
    }

    public Map<String, Object> getResult() {
        return response;
    }

    public void setResult(Map<String, Object> result) {
        response = result;
    }

    public Object get(String key) {
        return response.get(key);
    }

    public void put(String key, Object vo) {
        response.put(key, vo);
    }

    public void putAll(Map<String, Object> map) {
        response.putAll(map);
    }

    public boolean containsKey(String key) {
        return response.containsKey(key);
    }

}
