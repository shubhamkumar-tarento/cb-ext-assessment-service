package com.igot.cb.common.util;


import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.model.SunbirdApiRespParam;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;
import java.util.*;

public class ProjectUtil {

    private ProjectUtil() {
    }

    public static SBApiResponse createDefaultResponse(String api) {
        SBApiResponse response = new SBApiResponse();
        response.setId(api);
        response.setVer(Constants.API_VERSION_1);
        response.setParams(new SunbirdApiRespParam(UUID.randomUUID().toString()));
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.setTs(LocalDateTime.now().toString());
        return response;
    }

    public static void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(responseCode);
    }

}