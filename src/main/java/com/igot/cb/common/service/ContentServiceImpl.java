package com.igot.cb.common.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.igot.cb.cache.DataCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import com.igot.cb.core.exception.ApplicationLogicError;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.keycloak.util.JsonSerialization.mapper;

@Service
public class ContentServiceImpl implements ContentService{

    private Logger logger = LoggerFactory.getLogger(ContentServiceImpl.class);

    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    final CbExtAssessmentServerProperties serverConfig;

    final RedisCacheMgr redisCacheMgr;

    final DataCacheMgr dataCacheMgr;

    public ContentServiceImpl(OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
            CbExtAssessmentServerProperties serverConfig, RedisCacheMgr redisCacheMgr, DataCacheMgr dataCacheMgr) {
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.serverConfig = serverConfig;
        this.redisCacheMgr = redisCacheMgr;
        this.dataCacheMgr = dataCacheMgr;
    }

    @Override
    public String getContentType(String resourceId) {
        String parentContentType = "";
        Map<String, Object> response = getHierarchyResponseMap(resourceId);
        if (Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> resultMap = (Map<String, Object>) response.get(Constants.RESULT);
            if (!ObjectUtils.isEmpty(resultMap)) {
                Map<String, Object> contentMap = (Map<String, Object>) resultMap.get(Constants.CONTENT);
                if (!ObjectUtils.isEmpty(contentMap)) {
                    parentContentType = (String) contentMap.get(Constants.CONTENT_TYPE_KEY);
                }
            }
        }
        return parentContentType;
    }

    public String getParentIdentifier(String resourceId) {
        String parentId = "";
        Map<String, Object> response = getHierarchyResponseMap(resourceId);
        if (Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> resultMap = (Map<String, Object>) response.get(Constants.RESULT);
            if (!ObjectUtils.isEmpty(resultMap)) {
                Map<String, Object> contentMap = (Map<String, Object>) resultMap.get(Constants.CONTENT);
                if (!ObjectUtils.isEmpty(contentMap)) {
                    parentId = (String) contentMap.get(Constants.PARENT);
                }
            }
        }
        return parentId;
    }

    @Override
    public String updateContentProgress(String userAuthToken, Map<String, Object> reqBody, String userId, SBApiResponse outgoingResponse) throws ApplicationLogicError {
        String response = "";
        try {
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
            headers.put(Constants.X_AUTH_TOKEN, userAuthToken);
            headers.put(Constants.AUTHORIZATION, serverConfig.getSbApiKey());

            Map<String, Object> req = new HashMap<>();
            Map<String, Object> request = new HashMap<>();
            List<Map<String, Object>> contents = new ArrayList<>();

            Map<String, Object> reqObj = new HashMap<>();
            reqObj.put(Constants.CONTENT_ID_KEY, reqBody.get(Constants.IDENTIFIER));
            reqObj.put(Constants.COURSE_ID, reqBody.get(Constants.COURSE_ID));
            reqObj.put(Constants.BATCH_ID, reqBody.get(Constants.BATCH_ID));
            reqObj.put(Constants.STATUS, 2);
            reqObj.put("lastAccessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ").format(new Date()));
            reqObj.put(Constants.COMPLETION_PERCENTAGE, 100);
            reqObj.put(Constants.LANGUAGE, reqBody.get(Constants.LANGUAGE));

            contents.add(reqObj);

            req.put(Constants.USER_ID, userId);
            req.put("contents", contents);
            request.put(Constants.REQUEST,req);

            Map<String, Object> apiResponse = outboundRequestHandlerService.fetchResultUsingPatch(
                    serverConfig.getCourseServiceHost() + serverConfig.getProgressUpdateEndPoint(),
                    request, headers);

            if ("OK".equals(apiResponse.get("responseCode"))) {
                response = Constants.SUCCESS;
                if (logger.isInfoEnabled()) {
                    logger.info("Successfully updated progress for user : {}, for assessment : {}, of course :{}", userId,
                            reqBody.get(Constants.IDENTIFIER), reqBody.get(Constants.COURSE_ID));
                }
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Failed to update progress for user : {}, for assessment : {}, of course :{}", userId,
                            reqBody.get(Constants.IDENTIFIER), reqBody.get(Constants.COURSE_ID));
                }
                outgoingResponse.setResult(null);
                updateErrorDetails(outgoingResponse, Constants.FAILED_TO_UPDATE_PROGRESS, HttpStatus.INTERNAL_SERVER_ERROR);
            }

        } catch (Exception e) {
            logger.error(String.format("Failed to update progress for user: %s, for assessment: %s, of course: %s. Exception: %s",
                    userId, reqBody.get(Constants.IDENTIFIER),reqBody.get(Constants.COURSE_ID), e.getMessage()), e);
            outgoingResponse.setResult(null);
            updateErrorDetails(outgoingResponse, Constants.FAILED_TO_UPDATE_PROGRESS, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(responseCode);
    }

    public Map<String, Object> getHierarchyResponseMap(String contentId) {
        StringBuilder url = new StringBuilder();
        url.append(serverConfig.getContentHost()).append(serverConfig.getHierarchyEndPoint()).append("/" + contentId)
                .append("?hierarchyType=detail");
        Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url.toString());
        if (ObjectUtils.isEmpty(response)) {
            return Collections.emptyMap();
        }

        return response;
    }

    public Map<String, Object> readContentFromCache(String contentId, List<String> fields) {
        if (CollectionUtils.isEmpty(fields)) {
            fields = serverConfig.getDefaultContentProperties();
        }
        Map<String, Object> responseData = null;

        responseData = dataCacheMgr.getContentFromCache(contentId);

        if (MapUtils.isEmpty(responseData) || responseData.size() < fields.size()) {
            // DataCacheMgr doesn't have data OR contains less content fields.
            // Let's read again
            String contentString = redisCacheMgr.getContentFromCache(contentId);
            if (StringUtils.isBlank(contentString)) {
                // Tried reading from Redis - but redis didn't have data for some reason.
                // Or connection failed ??
                responseData = readContent(contentId, fields);
            } else {
                try {
                    responseData = new HashMap<>();
                    Map<String, Object> contentData = mapper.readValue(contentString,
                            new TypeReference<Map<String, Object>>() {
                            });
                    if (MapUtils.isNotEmpty(contentData)) {
                        for (String field : fields) {
                            if (contentData.containsKey(field)) {
                                responseData.put(field, contentData.get(field));
                            }
                        }
                        dataCacheMgr.putContentInCache(contentId, responseData);
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse content info from redis. Exception: " + e.getMessage(), e);
                    responseData = readContent(contentId);
                }
            }
        } else {
            // We are going to send the data read from which might have more fields.
            // This is fine for now.
        }

        return responseData;
    }
    public Map<String, Object> readContent(String contentId) throws ApplicationLogicError {
        return readContent(contentId, Collections.emptyList());
    }

    @Override
    public String updatePreEnrolledAssessment(String userAuthToken, Map<String, Object> reqBody, String userId, SBApiResponse outgoingResponse) throws ApplicationLogicError {
        String response = "";
        try {
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
            headers.put(Constants.X_AUTH_TOKEN, userAuthToken);


            Map<String, Object> req = new HashMap<>();
            Map<String, Object> request = new HashMap<>();
            List<Map<String, Object>> contents = new ArrayList<>();

            Map<String, Object> reqObj = new HashMap<>();
            reqObj.put(Constants.CONTENT_ID_KEY, reqBody.get(Constants.IDENTIFIER));

            reqObj.put(Constants.STATUS, 2);
            reqObj.put("lastAccessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ").format(new Date()));
            reqObj.put(Constants.COMPLETION_PERCENTAGE, 100);
            reqObj.put(Constants.PROGRESS_DETAILS, new HashMap<>());

            contents.add(reqObj);

            req.put("contents", contents);
            request.put(Constants.REQUEST,req);

            Map<String, Object> apiResponse = outboundRequestHandlerService.fetchResultUsingPatch(
                    serverConfig.getExtCourseServiceHost() + serverConfig.getContentStateUpdate(),
                    request, headers);

            if ("OK".equals(apiResponse.get("responseCode"))) {
                response = Constants.SUCCESS;
                if (logger.isInfoEnabled()) {
                    logger.info("Successfully updated progress for user : {}, for assessment : {}", userId,
                            reqBody.get(Constants.IDENTIFIER));
                }
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Failed to update progress for user : {}, for assessment : {}, of course :{}", userId,
                            reqBody.get(Constants.IDENTIFIER), reqBody.get(Constants.COURSE_ID));
                }
                outgoingResponse.setResult(null);
                updateErrorDetails(outgoingResponse, Constants.FAILED_TO_UPDATE_PROGRESS, HttpStatus.INTERNAL_SERVER_ERROR);
            }

        } catch (Exception e) {
            logger.error(String.format("Failed to update progress for user: %s, for assessment: %s, of course: %s. Exception: %s",
                    userId, reqBody.get(Constants.IDENTIFIER),reqBody.get(Constants.COURSE_ID), e.getMessage()), e);
            outgoingResponse.setResult(null);
            updateErrorDetails(outgoingResponse, Constants.FAILED_TO_UPDATE_PROGRESS, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public Map<String, Object> readContent(String contentId, List<String> fields) throws ApplicationLogicError {
        StringBuilder url = new StringBuilder();
        url.append(serverConfig.getContentHost()).append(serverConfig.getContentReadEndPoint()).append("/" + contentId)
                .append(serverConfig.getContentReadEndPointFields());
        if (CollectionUtils.isNotEmpty(fields)) {
            StringBuffer stringBuffer = new StringBuffer(String.join(",", fields));
            url.append(",").append(stringBuffer);
        }
        Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url.toString());
        if (null != response && Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> contentResult = (Map<String, Object>) response.get(Constants.RESULT);
            return (Map<String, Object>) contentResult.get(Constants.CONTENT);
        }
        return Collections.emptyMap();
    }

    @Override
    public Set<String> readChildCoursesFromCache(String parentDoId) {
        return redisCacheMgr
                .getSetFromCacheAsCommaSeparated(parentDoId + ":" + parentDoId + ":" + Constants.CHILDREN_COURSES);
    }
}
