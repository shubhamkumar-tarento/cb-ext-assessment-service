package com.igot.cb.common.service;

import com.igot.cb.cache.DataCacheMgr;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.http.HttpStatus;


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
}