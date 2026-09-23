package com.igot.cb.common.service;

import com.igot.cb.cache.DataCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;


import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ContentServiceImplTest {

    @InjectMocks
    private ContentServiceImpl contentService;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    @Mock
    private CbExtAssessmentServerProperties serverConfig;
    @Mock
    private RedisCacheMgr redisCacheMgr;
    @Mock
    private DataCacheMgr dataCacheMgr;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(serverConfig.getSbApiKey()).thenReturn("api-key");
        when(serverConfig.getCourseServiceHost()).thenReturn("http://host");
        when(serverConfig.getProgressUpdateEndPoint()).thenReturn("/progress");
        when(serverConfig.getContentHost()).thenReturn("http://content");
        when(serverConfig.getHierarchyEndPoint()).thenReturn("/hierarchy");
        when(serverConfig.getContentReadEndPoint()).thenReturn("/read");
        when(serverConfig.getContentReadEndPointFields()).thenReturn("?fields=");
        when(serverConfig.getDefaultContentProperties()).thenReturn(Arrays.asList("field1", "field2"));
    }

    @Test
    void testUpdateContentProgress_Success() {
        Map<String, Object> reqBody = new HashMap<>();
        reqBody.put(Constants.IDENTIFIER, "id1");
        reqBody.put(Constants.COURSE_ID, "cid");
        reqBody.put(Constants.BATCH_ID, "bid");
        reqBody.put(Constants.LANGUAGE, "en");

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("responseCode", "OK");

        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any())).thenReturn(apiResponse);

        SBApiResponse outgoing = new SBApiResponse();
        String result = contentService.updateContentProgress("token", reqBody, "user1", outgoing);

        assertEquals(Constants.SUCCESS, result);
    }

    @Test
    void testReadChildCoursesFromCache() {
        Set<String> expected = new HashSet<>(Arrays.asList("c1", "c2"));
        when(redisCacheMgr.getSetFromCacheAsCommaSeparated(anyString())).thenReturn(expected);

        Set<String> result = contentService.readChildCoursesFromCache("parent");
        assertEquals(expected, result);
    }

    @Test
    void testGetContentType_OK() {
        Map<String, Object> contentMap = Map.of(Constants.CONTENT_TYPE_KEY, "Course");
        Map<String, Object> resultMap = Map.of(Constants.CONTENT, contentMap);
        Map<String, Object> response = Map.of(Constants.RESPONSE_CODE, Constants.OK, Constants.RESULT, resultMap);
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);

        String type = contentService.getContentType("res1");
        assertEquals("Course", type);
    }

    @Test
    void testGetContentType_NotOK() {
        Map<String, Object> response = Map.of(Constants.RESPONSE_CODE, "FAILED");
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);

        String type = contentService.getContentType("res1");
        assertEquals("", type);
    }

    @Test
    void testGetParentIdentifier_OK() {
        Map<String, Object> contentMap = Map.of(Constants.PARENT, "parent1");
        Map<String, Object> resultMap = Map.of(Constants.CONTENT, contentMap);
        Map<String, Object> response = Map.of(Constants.RESPONSE_CODE, Constants.OK, Constants.RESULT, resultMap);
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);

        String parent = contentService.getParentIdentifier("res1");
        assertEquals("parent1", parent);
    }

    @Test
    void testReadContentFromCache_DataCacheHit() {
        Map<String, Object> cacheData = Map.of("field1", "v1", "field2", "v2");
        when(dataCacheMgr.getContentFromCache(anyString())).thenReturn(cacheData);

        Map<String, Object> result = contentService.readContentFromCache("cid", Arrays.asList("field1", "field2"));
        assertEquals(cacheData, result);
    }

    @Test
    void testReadContentFromCache_RedisHit() {
        when(dataCacheMgr.getContentFromCache(anyString())).thenReturn(Collections.emptyMap());
        String json = "{\"field1\":\"v1\",\"field2\":\"v2\"}";
        when(redisCacheMgr.getContentFromCache(anyString())).thenReturn(json);

        Map<String, Object> result = contentService.readContentFromCache("cid", Arrays.asList("field1", "field2"));
        assertEquals("v1", result.get("field1"));
        assertEquals("v2", result.get("field2"));
    }

    @Test
    void testReadContent_OK() {
        Map<String, Object> content = Map.of("field1", "v1");
        Map<String, Object> resultMap = Map.of(Constants.CONTENT, content);
        Map<String, Object> response = Map.of(Constants.RESPONSE_CODE, Constants.OK, Constants.RESULT, resultMap);
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);

        Map<String, Object> result = contentService.readContent("cid", List.of("field1"));
        assertEquals(content, result);
    }

    @Test
    void testReadContent_NotOK() {
        Map<String, Object> response = Map.of(Constants.RESPONSE_CODE, "FAILED");
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);

        Map<String, Object> result = contentService.readContent("cid", List.of("field1"));
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetHierarchyResponseMap_Empty() {
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(null);

        Map<String, Object> result = contentService.getHierarchyResponseMap("cid");
        assertTrue(result.isEmpty());
    }

    @Test
    void testUpdateErrorDetails_Reflection() throws Exception {
        SBApiResponse resp = new SBApiResponse();
        java.lang.reflect.Method method = ContentServiceImpl.class.getDeclaredMethod(
                "updateErrorDetails", SBApiResponse.class, String.class, HttpStatus.class);
        method.setAccessible(true);
        method.invoke(contentService, resp, "err", HttpStatus.BAD_REQUEST);

        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals("err", resp.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
    }

    @Test
    void testReadContentFromCache_FieldsEmpty() {
        Map<String, Object> cacheData = Map.of("field1", "v1", "field2", "v2");
        when(dataCacheMgr.getContentFromCache(anyString())).thenReturn(cacheData);
        when(serverConfig.getDefaultContentProperties()).thenReturn(Arrays.asList("field1", "field2"));
        Map<String, Object> result = contentService.readContentFromCache("cid", Collections.emptyList());
        assertEquals(cacheData, result);
    }

    @Test
    void testReadContentFromCache_RedisBlankFallback() {
        when(dataCacheMgr.getContentFromCache(anyString())).thenReturn(Collections.emptyMap());
        when(redisCacheMgr.getContentFromCache(anyString())).thenReturn("");
        Map<String, Object> fallback = Map.of("field1", "v1");
        ContentServiceImpl spy = spy(contentService);
        doReturn(fallback).when(spy).readContent(anyString(), anyList());
        Map<String, Object> result = spy.readContentFromCache("cid", List.of("field1"));
        assertEquals(fallback, result);
    }

    @Test
    void testReadContentFromCache_RedisInvalidJsonFallback() {
        when(dataCacheMgr.getContentFromCache(anyString())).thenReturn(Collections.emptyMap());
        when(redisCacheMgr.getContentFromCache(anyString())).thenReturn("invalid_json");
        ContentServiceImpl spy = spy(contentService);
        doReturn(Map.of("field1", "v1")).when(spy).readContent(anyString());
        Map<String, Object> result = spy.readContentFromCache("cid", List.of("field1"));
        assertEquals("v1", result.get("field1"));
    }

    @Test
    void testReadContentFromCache_DataCacheHasMoreFields() {
        Map<String, Object> cacheData = Map.of("field1", "v1", "field2", "v2", "extra", "x");
        when(dataCacheMgr.getContentFromCache(anyString())).thenReturn(cacheData);
        Map<String, Object> result = contentService.readContentFromCache("cid", Arrays.asList("field1", "field2"));
        assertEquals(cacheData, result);
    }

    @Test
    void testReadContent_ResponseNotOK() {
        Map<String, Object> response = Map.of(Constants.RESPONSE_CODE, "ERROR");
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);
        Map<String, Object> result = contentService.readContent("cid", List.of("field1"));
        assertNotNull(result);
        assertTrue(result.isEmpty());
        // Additional assertion: verify fetchResult was called once
        verify(outboundRequestHandlerService, times(1)).fetchResult(anyString());
    }

    @Test
    void testReadContent_ResponseNull() {
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(null);
        Map<String, Object> result = contentService.readContent("cid", List.of("field1"));
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetHierarchyResponseMap_NotEmpty() {
        Map<String, Object> response = Map.of("key", "value");
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(response);
        Map<String, Object> result = contentService.getHierarchyResponseMap("cid");
        assertEquals(response, result);
    }

    @Test
    void testUpdateContentProgress_ApiResponseNotOK() {
        Map<String, Object> reqBody = new HashMap<>();
        reqBody.put(Constants.IDENTIFIER, "id1");
        reqBody.put(Constants.COURSE_ID, "cid");
        reqBody.put(Constants.BATCH_ID, "bid");
        reqBody.put(Constants.LANGUAGE, "en");
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("responseCode", "FAILED");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any())).thenReturn(apiResponse);
        SBApiResponse outgoing = new SBApiResponse();
        String result = contentService.updateContentProgress("token", reqBody, "user1", outgoing);
        assertEquals("", result);
        assertEquals(Constants.FAILED, outgoing.getParams().getStatus());
    }

    @Test
    void testUpdateContentProgress_Exception() {
        Map<String, Object> reqBody = new HashMap<>();
        reqBody.put(Constants.IDENTIFIER, "id1");
        reqBody.put(Constants.COURSE_ID, "cid");
        reqBody.put(Constants.BATCH_ID, "bid");
        reqBody.put(Constants.LANGUAGE, "en");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any()))
                .thenThrow(new RuntimeException("fail"));
        SBApiResponse outgoing = new SBApiResponse();
        String result = contentService.updateContentProgress("token", reqBody, "user1", outgoing);
        assertEquals("", result);
        assertEquals(Constants.FAILED, outgoing.getParams().getStatus());
    }

    private Map<String, Object> progressRequestBody() {
        Map<String, Object> reqBody = new HashMap<>();
        reqBody.put(Constants.IDENTIFIER, "id1");
        reqBody.put(Constants.COURSE_ID, "cid");
        reqBody.put(Constants.BATCH_ID, "bid");
        reqBody.put(Constants.LANGUAGE, "en");
        return reqBody;
    }

    private void disableInfoLogging() {
        Logger quietLogger = mock(Logger.class);
        when(quietLogger.isInfoEnabled()).thenReturn(false);
        ReflectionTestUtils.setField(contentService, "logger", quietLogger);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testUpdateContentProgress_BuildsExpectedRequest() {
        Map<String, Object> apiResponse = Map.of("responseCode", "OK");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any())).thenReturn(apiResponse);

        contentService.updateContentProgress("token", progressRequestBody(), "user1", new SBApiResponse());

        ArgumentCaptor<Object> requestCaptor = ArgumentCaptor.forClass(Object.class);
        ArgumentCaptor<Map<String, String>> headerCaptor = ArgumentCaptor.forClass(Map.class);
        verify(outboundRequestHandlerService).fetchResultUsingPatch(eq("http://host/progress"),
                requestCaptor.capture(), headerCaptor.capture());
        assertEquals("token", headerCaptor.getValue().get(Constants.X_AUTH_TOKEN));
        assertEquals("api-key", headerCaptor.getValue().get(Constants.AUTHORIZATION));
        Map<String, Object> request = (Map<String, Object>) ((Map<String, Object>) requestCaptor.getValue())
                .get(Constants.REQUEST);
        assertEquals("user1", request.get(Constants.USER_ID));
        List<Map<String, Object>> contents = (List<Map<String, Object>>) request.get("contents");
        assertEquals("id1", contents.get(0).get(Constants.CONTENT_ID_KEY));
        assertEquals(2, contents.get(0).get(Constants.STATUS));
    }

    @Test
    void testUpdateContentProgress_InfoLoggingDisabled() {
        disableInfoLogging();
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any()))
                .thenReturn(Map.of("responseCode", "OK"))
                .thenReturn(Map.of("responseCode", "FAILED"));

        assertEquals(Constants.SUCCESS,
                contentService.updateContentProgress("token", progressRequestBody(), "user1", new SBApiResponse()));
        SBApiResponse outgoing = new SBApiResponse();
        assertEquals("", contentService.updateContentProgress("token", progressRequestBody(), "user1", outgoing));
        assertEquals(Constants.FAILED, outgoing.getParams().getStatus());
        assertEquals(Constants.FAILED_TO_UPDATE_PROGRESS, outgoing.getParams().getErrmsg());
    }

    @Test
    void testUpdatePreEnrolledAssessment_Success() {
        when(serverConfig.getExtCourseServiceHost()).thenReturn("http://ext");
        when(serverConfig.getContentStateUpdate()).thenReturn("/state");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any()))
                .thenReturn(Map.of("responseCode", "OK"));

        SBApiResponse outgoing = new SBApiResponse();
        String result = contentService.updatePreEnrolledAssessment("token", progressRequestBody(), "user1", outgoing);

        assertEquals(Constants.SUCCESS, result);
        verify(outboundRequestHandlerService).fetchResultUsingPatch(eq("http://ext/state"), any(), any());
    }

    @Test
    void testUpdatePreEnrolledAssessment_NotOk() {
        when(serverConfig.getExtCourseServiceHost()).thenReturn("http://ext");
        when(serverConfig.getContentStateUpdate()).thenReturn("/state");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), any()))
                .thenReturn(Map.of("responseCode", "CLIENT_ERROR"));

        SBApiResponse outgoing = new SBApiResponse();
        String result = contentService.updatePreEnrolledAssessment("token", progressRequestBody(), "user1", outgoing);

        assertEquals("", result);
        assertNull(outgoing.getResult());
        assertEquals(Constants.FAILED, outgoing.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, outgoing.getResponseCode());
    }

    @Test
    void testUpdatePreEnrolledAssessment_InfoLoggingDisabled() {
        disableInfoLogging();
        when(outboundRequestHandlerService.fetchResultUsingPatch(any(), any(), any()))
                .thenReturn(Map.of("responseCode", "OK"))
                .thenReturn(Map.of());

        assertEquals(Constants.SUCCESS,
                contentService.updatePreEnrolledAssessment("token", progressRequestBody(), "user1", new SBApiResponse()));
        SBApiResponse outgoing = new SBApiResponse();
        assertEquals("", contentService.updatePreEnrolledAssessment("token", progressRequestBody(), "user1", outgoing));
        assertEquals(Constants.FAILED, outgoing.getParams().getStatus());
    }

    @Test
    void testUpdatePreEnrolledAssessment_Exception() {
        when(outboundRequestHandlerService.fetchResultUsingPatch(any(), any(), any()))
                .thenThrow(new RuntimeException("patch failed"));

        SBApiResponse outgoing = new SBApiResponse();
        String result = contentService.updatePreEnrolledAssessment("token", progressRequestBody(), "user1", outgoing);

        assertEquals("", result);
        assertEquals(Constants.FAILED, outgoing.getParams().getStatus());
        assertEquals(Constants.FAILED_TO_UPDATE_PROGRESS, outgoing.getParams().getErrmsg());
    }

    @Test
    void testGetContentType_EmptyResultAndEmptyContent() {
        when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenReturn(Map.of(Constants.RESPONSE_CODE, Constants.OK))
                .thenReturn(Map.of(Constants.RESPONSE_CODE, Constants.OK,
                        Constants.RESULT, Map.of(Constants.CONTENT, Map.of())));

        assertEquals("", contentService.getContentType("res1"));
        assertEquals("", contentService.getContentType("res1"));
    }

    @Test
    void testGetParentIdentifier_NotOkEmptyResultAndEmptyContent() {
        when(outboundRequestHandlerService.fetchResult(anyString()))
                .thenReturn(Map.of(Constants.RESPONSE_CODE, "FAILED"))
                .thenReturn(Map.of(Constants.RESPONSE_CODE, Constants.OK))
                .thenReturn(Map.of(Constants.RESPONSE_CODE, Constants.OK,
                        Constants.RESULT, Map.of(Constants.CONTENT, Map.of())));

        assertEquals("", contentService.getParentIdentifier("res1"));
        assertEquals("", contentService.getParentIdentifier("res1"));
        assertEquals("", contentService.getParentIdentifier("res1"));
        verify(outboundRequestHandlerService, times(3))
                .fetchResult("http://content/hierarchy/res1?hierarchyType=detail");
    }

    @Test
    void testReadContent_WithoutFieldsUsesBaseUrl() {
        Map<String, Object> content = Map.of("name", "course");
        when(outboundRequestHandlerService.fetchResult("http://content/read/cid?fields="))
                .thenReturn(Map.of(Constants.RESPONSE_CODE, Constants.OK,
                        Constants.RESULT, Map.of(Constants.CONTENT, content)));

        assertEquals(content, contentService.readContent("cid"));
    }

    @Test
    void testReadContent_WithFieldsAppendsFields() {
        when(outboundRequestHandlerService.fetchResult(anyString())).thenReturn(null);

        contentService.readContent("cid", List.of("f1", "f2"));

        verify(outboundRequestHandlerService).fetchResult("http://content/read/cid?fields=,f1,f2");
    }

    @Test
    void testReadContentFromCache_DataCacheHasFewerFields_ProjectsRedisContent() {
        when(dataCacheMgr.getContentFromCache("cid")).thenReturn(Map.of("field1", "v1"));
        when(redisCacheMgr.getContentFromCache("cid")).thenReturn("{\"field1\":\"v1\",\"other\":\"x\"}");

        Map<String, Object> result = contentService.readContentFromCache("cid", null);

        // field2 is not present in redis and "other" was not requested
        assertEquals(Map.of("field1", "v1"), result);
        verify(dataCacheMgr).putContentInCache("cid", Map.of("field1", "v1"));
    }

    @Test
    void testReadContentFromCache_RedisHasEmptyJson() {
        when(dataCacheMgr.getContentFromCache("cid")).thenReturn(null);
        when(redisCacheMgr.getContentFromCache("cid")).thenReturn("{}");

        Map<String, Object> result = contentService.readContentFromCache("cid", List.of("field1"));

        assertTrue(result.isEmpty());
        verify(dataCacheMgr, never()).putContentInCache(anyString(), any());
    }
}