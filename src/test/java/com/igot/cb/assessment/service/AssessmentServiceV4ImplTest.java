package com.igot.cb.assessment.service;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.assessment.repo.AssessmentRepository;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.service.ContentService;
import com.igot.cb.common.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.common.util.AccessTokenValidator;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import com.igot.cb.core.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;


import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;

class AssessmentServiceV4ImplTest {

    @InjectMocks
    AssessmentServiceV4Impl service;

    @Mock
    CbExtAssessmentServerProperties serverProperties;
    @Mock
    Producer kafkaProducer;
    @Mock
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    @Mock
    AssessmentUtilServiceV2 assessUtilServ;
    @Mock
    ObjectMapper mapper;
    @Mock
    AssessmentRepository assessmentRepository;
    @Mock
    AccessTokenValidator accessTokenValidator;
    @Mock
    ContentService contentService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(serverProperties.getAssessmentLevelParams()).thenReturn(Arrays.asList("primaryCategory", "expectedDuration", "children"));
        when(serverProperties.getAssessmentSectionParams()).thenReturn(Arrays.asList("identifier", "primaryCategory", "minimumPassPercentage", "objectType", "children"));
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("5");
    }

    @Test
    void testRetakeAssessment_Positive() {
        String token = "token";
        String userId = "user1";
        String assessmentId = "assess1";
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        // Use eq(token) because retakeAssessment delegates to retakeAssessmentByUserId which now passes the token
        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), eq(token))).thenReturn(hierarchy);
        when(serverProperties.isAssessmentRetakeCountVerificationEnabled()).thenReturn(false);

        SBApiResponse resp = service.retakeAssessment(assessmentId, token, false);
        assertEquals(3, resp.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
    }

    @Test
    void testRetakeAssessmentByUserId_Positive() {
        String userId = "user1";
        String assessmentId = "assess1";
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);

        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), isNull())).thenReturn(hierarchy);
        when(serverProperties.isAssessmentRetakeCountVerificationEnabled()).thenReturn(false);

        SBApiResponse resp = service.retakeAssessmentByUserId(assessmentId, userId, false, null);
        assertEquals(3, resp.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, resp.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void testRetakeAssessmentByUserId_Negative_BlankUserId() {
        String assessmentId = "assess1";
        SBApiResponse resp = service.retakeAssessmentByUserId(assessmentId, "", false, null);
        assertEquals(Constants.USER_ID_DOESNT_EXIST, resp.getParams().getErrmsg());
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void testRetakeAssessmentByUserId_Negative_EmptyHierarchy() {
        String userId = "user1";
        String assessmentId = "assess1";
        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), isNull())).thenReturn(Collections.emptyMap());

        SBApiResponse resp = service.retakeAssessmentByUserId(assessmentId, userId, false, null);
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, resp.getParams().getErrmsg());
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void testRetakeAssessmentByUserId_Positive_PreEnrolled() {
        String userId = "user1";
        String assessmentId = "assess1";
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.CONTEXT_CATEGORY_TAG, Constants.PRE_ENROLLED_ASSESSMENT_KEY);

        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), isNull())).thenReturn(hierarchy);

        SBApiResponse resp = service.retakeAssessmentByUserId(assessmentId, userId, false, null);
        assertEquals(1, resp.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, resp.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void testRetakeAssessmentByUserId_Exception() {
        String userId = "user1";
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), isNull())).thenThrow(new RuntimeException("fail"));
        SBApiResponse resp = service.retakeAssessmentByUserId("assess1", userId, false, null);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertTrue(resp.getParams().getErrmsg().contains("Error while calculating retake assessment"));
    }

    @Test
    void testReadAssessment_Negative_MissingExpectedDuration() {
        String token = "token";
        String userId = "user1";
        String assessmentId = "assess1";
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.CHILDREN, Arrays.asList());
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), eq(token))).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(Collections.emptyList());

        SBApiResponse resp = service.readAssessment(assessmentId, token, false, null);
        assertEquals(Constants.ASSESSMENT_INVALID, resp.getParams().getErrmsg());
    }

    @Test
    void testReadAssessment_Negative_EmptyHierarchy() {
        String token = "token";
        String userId = "user1";
        String assessmentId = "assess1";
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), eq(token))).thenReturn(Collections.emptyMap());

        SBApiResponse resp = service.readAssessment(assessmentId, token, false, null);
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, resp.getParams().getErrmsg());
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void testReadAssessment_Positive_ExistingData() {
        String token = "token";
        String userId = "user1";
        String assessmentId = "assess1";
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.EXPECTED_DURATION, 10);
        Map<String, Object> section = new HashMap<>();
        section.put("identifier", "section1");
        section.put("primaryCategory", "Section");
        section.put("children", Arrays.asList(Collections.singletonMap("identifier", "q1")));
        section.put("minimumPassPercentage", 50);
        section.put("objectType", "Section");
        hierarchy.put(Constants.CHILDREN, Arrays.asList(section));

        Map<String, Object> existingData = new HashMap<>();
        existingData.put(Constants.END_TIME, Date.from(Instant.now().plusSeconds(60)));
        existingData.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        existingData.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{}");

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), eq(token))).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(Arrays.asList(existingData));
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(true);

        SBApiResponse resp = service.readAssessment(assessmentId, token, false, null);
        assertNotNull(resp.getResult().get(Constants.QUESTION_SET));
    }

    @Test
    void testReadAssessment_Exception() {
        String token = "token";
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenThrow(new RuntimeException("fail"));
        SBApiResponse resp = service.readAssessment("assess1", token, false, null);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertTrue(resp.getParams().getErrmsg().contains("Error while reading assessment"));
    }

    @Test
    void testReadWheebox_Positive() {
        String token = "token";
        String userId = "user1";
        Map<String, Object> wheeboxData = new HashMap<>();
        wheeboxData.put("score", 90);

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        when(assessUtilServ.fetchWheebox(userId)).thenReturn(wheeboxData);

        SBApiResponse resp = service.readWheebox(token);
        assertTrue(resp.getResult().containsKey(Constants.RESPONSE));
    }

    @Test
    void testReadWheebox_Negative_BlankUserId() {
        String token = "token";
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn("");
        SBApiResponse resp = service.readWheebox(token);
        assertEquals(Constants.USER_ID_DOESNT_EXIST, resp.getParams().getErrmsg());
    }

    @Test
    void testReadWheebox_Exception() {
        String token = "token";
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenThrow(new RuntimeException("fail"));
        SBApiResponse resp = service.readWheebox(token);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void testSubmitAssessmentAsync_valid() {
        assertDoesNotThrow(() -> service.submitAssessmentAsync(new HashMap<>(), "user", true));
    }


    @Test
    void testHandleAssessmentSubmitRequest_valid() {
        assertDoesNotThrow(() -> service.handleAssessmentSubmitRequest(new HashMap<>(), true, "user"));
    }

    @Test
    void testSubmitAssessmentAsync_Success_PracticeAssessment() throws IOException {
        // Mock token to userId
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        // Setup submit request
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.COURSE_ID, "course123");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());

        // Setup assessmentHierarchy
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        assessmentHierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);

        // Setup course response
        Map<String, Object> courseMap = new HashMap<>();
        courseMap.put(Constants.COURSE_CATEGORY, "Practice");

        // Mocks
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.emptyList());
        when(assessUtilServ.readAssessmentRecord(anyString(), anyList()))
                .thenReturn("english");
        when(assessUtilServ.readContentRecord(eq("course123"), anyList()))
                .thenReturn("course123-baseLang");
        when(contentService.readContent("course123-baseLang"))
                .thenReturn(courseMap);
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(new HashMap<>());
        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap()))
                .thenReturn(new HashMap<>());
        when(contentService.updateContentProgress(anyString(), anyMap(), anyString(), any()))
                .thenReturn(Constants.SUCCESS);

        // Call method under test
        SBApiResponse resp = service.submitAssessmentAsync(submitRequest, "token", false);

        // Validate success
        assertEquals(Constants.SUCCESS, resp.getParams().getStatus());
    }


    @Test
    void testSubmitAssessmentAsync_Failed_InvalidUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn(null);
        SBApiResponse resp = service.submitAssessmentAsync(new HashMap<>(), "token", false);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, resp.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_Failed_MissingAssessmentId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> submitRequest = new HashMap<>();
        SBApiResponse resp = service.submitAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(Constants.INVALID_ASSESSMENT_ID, resp.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_FailedAssessmentSubmit() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenThrow(new RuntimeException("fail"));
        SBApiResponse resp = service.submitAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertTrue(resp.getParams().getErrmsg().contains("Failed to process assessment submit request"));
    }

    @Test
    void testHandleAssessmentSubmitRequest_HierarchyEmpty() {
        Map<String, Object> asyncRequest = new HashMap<>();
        asyncRequest.put(Constants.USER_ID_CONSTANT, "user1");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        asyncRequest.put(Constants.REQUEST, submitRequest);

        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Collections.emptyMap());

        assertDoesNotThrow(() -> service.handleAssessmentSubmitRequest(asyncRequest, false, "token"));
    }

    @Test
    void testHandleAssessmentSubmitRequest_UserAssessmentDataNotPresent() {
        Map<String, Object> asyncRequest = new HashMap<>();
        asyncRequest.put(Constants.USER_ID_CONSTANT, "user1");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        asyncRequest.put(Constants.REQUEST, submitRequest);

        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        hierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.emptyList());

        assertDoesNotThrow(() -> service.handleAssessmentSubmitRequest(asyncRequest, false, "token"));
    }

    @Test
    void testHandleAssessmentSubmitRequest_AlreadySubmitted() {
        Map<String, Object> asyncRequest = new HashMap<>();
        asyncRequest.put(Constants.USER_ID_CONSTANT, "user1");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        asyncRequest.put(Constants.REQUEST, submitRequest);

        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        hierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);

        Map<String, Object> existingData = new HashMap<>();
        existingData.put(Constants.STATUS, Constants.SUBMITTED);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(existingData));

        assertDoesNotThrow(() -> service.handleAssessmentSubmitRequest(asyncRequest, false, "token"));
    }

    @Test
    void testHandleAssessmentSubmitRequest_QuestionSetNull(){
        Map<String, Object> asyncRequest = new HashMap<>();
        asyncRequest.put(Constants.USER_ID_CONSTANT, "user1");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILDREN, new ArrayList<>());
        children.add(section);
        submitRequest.put(Constants.CHILDREN, children);
        asyncRequest.put(Constants.REQUEST, submitRequest);

        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CHILDREN, children);
        hierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);

        Map<String, Object> existingData = new HashMap<>();
        existingData.put(Constants.STATUS, "IN_PROGRESS");
        existingData.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(existingData));

        assertDoesNotThrow(() -> service.handleAssessmentSubmitRequest(asyncRequest, false, "token"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testHandleAssessmentSubmitRequest_NormalFlow() throws Exception {
        Map<String, Object> asyncRequest = new HashMap<>();
        asyncRequest.put(Constants.USER_ID_CONSTANT, "user1");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILDREN, new ArrayList<>());
        children.add(section);
        submitRequest.put(Constants.CHILDREN, children);
        asyncRequest.put(Constants.REQUEST, submitRequest);

        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CHILDREN, children);
        hierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);

        Map<String, Object> existingData = new HashMap<>();
        existingData.put(Constants.STATUS, "IN_PROGRESS");
        existingData.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"children\":[{\"identifier\":\"section1\",\"childNodes\":[\"q1\"]}]}");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(existingData));

        when(mapper.readValue(anyString(), any(TypeReference.class)))
                .thenReturn(Map.of("children", List.of(Map.of(Constants.IDENTIFIER, "section1", Constants.CHILD_NODES, List.of("q1")))));

        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap()))
                .thenReturn(Map.of("result", 100.0));
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of());

        assertDoesNotThrow(() -> service.handleAssessmentSubmitRequest(asyncRequest, false, "token"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testValidateQuestionListAPI_Negative() throws Exception {
        // Inject mocks
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);
        ReflectionTestUtils.setField(service, "assessUtilServ", assessUtilServ);
        ReflectionTestUtils.setField(service, "mapper", mapper);

        String token = "token";
        String userId = "user1";
        String assessmentId = "assess1";
        List<String> identifierList = new ArrayList<>();
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, assessmentId);

        // Mock getQuestionIdList to return identifiers
        List<String> questionIds = Arrays.asList("q1", "q2");
        AssessmentServiceV4Impl spyService = Mockito.spy(service);

        // Mock dependencies
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(assessmentId, false, token)).thenReturn(assessmentDetail);

        Map<String, Object> userAssessmentDetail = new HashMap<>();
        userAssessmentDetail.put(Constants.PRIMARY_CATEGORY, "Assessment");
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.CHILD_NODES, questionIds);
        userAssessmentDetail.put(Constants.CHILDREN, List.of(section));
        String assessmentReadResponse = "{\"primaryCategory\":\"Assessment\",\"children\":[{\"childNodes\":[\"q1\",\"q2\"]}]}";
        List<Map<String, Object>> existingDataList = List.of(Map.of(Constants.ASSESSMENT_READ_RESPONSE_KEY, assessmentReadResponse));
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(existingDataList);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(userAssessmentDetail);

        // Call private method via reflection
        Method method = AssessmentServiceV4Impl.class.getDeclaredMethod(
                "validateQuestionListAPI", Map.class, String.class, List.class, boolean.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> result = (Map<String, String>) method.invoke(spyService, requestBody, token, identifierList, false);

        assertEquals(Constants.IDENTIFIER_LIST_IS_EMPTY, result.get(Constants.ERROR_MESSAGE));
    }

    @Test
    void testValidateQuestionListAPI_BlankUserId() throws Exception {
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");

        Method method = AssessmentServiceV4Impl.class.getDeclaredMethod(
                "validateQuestionListAPI", Map.class, String.class, List.class, boolean.class);
        method.setAccessible(true);

        Map<String, Object> requestBody = new HashMap<>();
        List<String> identifierList = new ArrayList<>();
        @SuppressWarnings("unchecked")
        Map<String, String> result = (Map<String, String>) method.invoke(service, requestBody, "token", identifierList, false);

        assertEquals(Constants.USER_ID_DOESNT_EXIST, result.get(Constants.ERROR_MESSAGE));
    }

    @Test
    void testValidateQuestionListAPI_EmptyAssessmentId() throws Exception {
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        Method method = AssessmentServiceV4Impl.class.getDeclaredMethod(
                "validateQuestionListAPI", Map.class, String.class, List.class, boolean.class);
        method.setAccessible(true);

        Map<String, Object> requestBody = new HashMap<>();
        List<String> identifierList = new ArrayList<>();
        @SuppressWarnings("unchecked")
        Map<String, String> result = (Map<String, String>) method.invoke(service, requestBody, "token", identifierList, false);

        assertEquals(Constants.ASSESSMENT_ID_KEY_IS_NOT_PRESENT_IS_EMPTY, result.get(Constants.ERROR_MESSAGE));
    }

    @Test
    void testSubmitAssessmentAsync_Failed_ReadAssessment() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Collections.emptyMap());
        SBApiResponse resp = service.submitAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, resp.getParams().getStatus());
        assertEquals(Constants.READ_ASSESSMENT_FAILED, resp.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_PracticeAssessment_NoLanguage() throws IOException {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        // Setup submitRequest
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        submitRequest.put(Constants.COURSE_ID, "course123");

        // Mock: readAssessmentHierarchyFromCache
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenAnswer(invocation -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
                    map.put(Constants.CHILDREN, new ArrayList<>());
                    map.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);
                    return map;
                });

        // Mock: no previously submitted records
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.emptyList());

        // Mock: language from DB
        when(assessUtilServ.readAssessmentRecord(anyString(), anyList()))
                .thenReturn("english");

        // Mock: return course ID from language map
        when(assessUtilServ.readContentRecord(eq("course123"), anyList()))
                .thenReturn("course123-baseLang");

        // Mock: content service course category
        Map<String, Object> courseCategoryMap = new HashMap<>();
        courseCategoryMap.put(Constants.COURSE_CATEGORY, "General");
        when(contentService.readContent("course123-baseLang"))
                .thenReturn(courseCategoryMap);

        // Mock: validation logic
        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap()))
                .thenReturn(new HashMap<>());

        // Mock: question list from cache
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(new HashMap<>());

        // Mock: updateContentProgress for practice assessment
        when(contentService.updateContentProgress(anyString(), anyMap(), anyString(), any()))
                .thenReturn(Constants.SUCCESS);

        // Call method under test
        SBApiResponse resp = service.submitAssessmentAsync(submitRequest, "token", false);

        // Validate
        assertEquals(Constants.SUCCESS, resp.getParams().getStatus());
    }


    @Test
    void testSubmitAssessmentAsync_SectionLevelScoreCutoff() throws IOException {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        // Submit request setup
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.COURSE_ID, "course123"); // Required for readContentRecord

        // Section data
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILDREN, new ArrayList<>()); // Empty questions
        section.put(Constants.MINIMUM_PASS_PERCENTAGE, 40); // Valid value
        section.put(Constants.OBJECT_TYPE, "Section"); // Required by createResponseMapWithProperStructure
        section.put(Constants.PRIMARY_CATEGORY, "SectionCategory"); // Required
        children.add(section);
        submitRequest.put(Constants.CHILDREN, children);

        // Assessment hierarchy setup
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.CHILDREN, children);
        assessmentHierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.SECTION_LEVEL_SCORE_CUTOFF);
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 10);

        // Existing data
        Map<String, Object> existingData = new HashMap<>();
        existingData.put(Constants.START_TIME, new Date());
        existingData.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        existingData.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"children\":[{\"identifier\":\"section1\",\"childNodes\":[]}]}");

        // Mocks
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.singletonList(existingData));
        when(assessUtilServ.readAssessmentRecord(anyString(), anyList())).thenReturn("english");
        when(assessUtilServ.readContentRecord(eq("course123"), anyList()))
                .thenReturn("course123-baseLang");

        Map<String, Object> courseCategoryMap = new HashMap<>();
        courseCategoryMap.put(Constants.COURSE_CATEGORY, "General");
        when(contentService.readContent("course123-baseLang"))
                .thenReturn(courseCategoryMap);

        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(new HashMap<>());
        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap()))
                .thenReturn(new HashMap<>());

        // Method call
        SBApiResponse resp = service.submitAssessmentAsync(submitRequest, "token", false);

        // Assertion
        assertEquals(Constants.SUCCESS, resp.getParams().getStatus());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessmentResultV4_Positive_Submitted() throws Exception {
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);
        ReflectionTestUtils.setField(service, "assessUtilServ", assessUtilServ);
        ReflectionTestUtils.setField(service, "mapper", mapper);

        String token = "token";
        String userId = "user1";
        String assessmentId = "assess1";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, assessmentId);
        requestBody.put("batchId", "batch1");      // Add mandatory field
        requestBody.put("courseId", "course1");
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, requestBody);

        Map<String, Object> submittedData = new HashMap<>();
        submittedData.put(Constants.STATUS, Constants.SUBMITTED);
        submittedData.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "{\"score\":100}");
        List<Map<String, Object>> existingDataList = List.of(submittedData);

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(existingDataList);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(Map.of("score", 100));

        SBApiResponse resp = service.readAssessmentResultV4(request, token);
        assertEquals(100, resp.get("score"));
    }

    @Test
    void testReadAssessmentResultV4_Negative_BlankUserId() {
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");

        SBApiResponse resp = service.readAssessmentResultV4(new HashMap<>(), "token");
        assertEquals(Constants.USER_ID_DOESNT_EXIST, resp.getParams().getErrmsg());
    }

    @Test
    void testReadAssessmentResultV4_Negative_InvalidRequest() {
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        Map<String, Object> request = new HashMap<>();
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("batchId", "batch1");
        requestBody.put("courseId", "course1");
        request.put(Constants.REQUEST, requestBody);
        SBApiResponse resp = service.readAssessmentResultV4(request, "token");
        assertTrue(resp.getParams().getErrmsg().contains("One or more mandatory fields are missing in Request. Mandatory fields are : "));
    }

    @Test
    void testReadAssessmentResultV4_Negative_NoUserAssessmentData() {
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);
        ReflectionTestUtils.setField(service, "assessUtilServ", assessUtilServ);

        String token = "token";
        String userId = "user1";
        String assessmentId = "assess1";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, assessmentId);
        requestBody.put("batchId", "batch1");      // Add mandatory field
        requestBody.put("courseId", "course1");    // Add mandatory field
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, requestBody);

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(Collections.emptyList());

        SBApiResponse resp = service.readAssessmentResultV4(request, token);
        assertEquals(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, resp.getParams().getErrmsg());
    }

    @Test
    void testReadAssessmentResultV4_StatusInProgress() {
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);
        ReflectionTestUtils.setField(service, "assessUtilServ", assessUtilServ);

        String token = "token";
        String userId = "user1";
        String assessmentId = "assess1";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, assessmentId);
        requestBody.put("batchId", "batch1");      // Add mandatory field
        requestBody.put("courseId", "course1");
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, requestBody);

        Map<String, Object> inProgressData = new HashMap<>();
        inProgressData.put(Constants.STATUS, "IN_PROGRESS");
        List<Map<String, Object>> existingDataList = List.of(inProgressData);

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(existingDataList);

        SBApiResponse resp = service.readAssessmentResultV4(request, token);
        assertTrue((Boolean) resp.getResult().get(Constants.STATUS_IS_IN_PROGRESS));
    }

    @Test
    void testReadQuestionList_EmptyIdentifierList() {
        String token = "token";
        String assessmentId = "assess1";

        // request body with no question identifiers
        Map<String, Object> searchMap = new HashMap<>();
        Map<String, Object> reqMap = new HashMap<>();
        reqMap.put(Constants.SEARCH, searchMap);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, reqMap);
        request.put(Constants.ASSESSMENT_ID_KEY, assessmentId);

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn("user1");
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");

        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), eq(token)))
                .thenReturn(assessmentHierarchy);

        // Mock existing user assessment data with empty string (simulate empty response)
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), eq(assessmentId)))
                .thenReturn(List.of(Map.of(Constants.ASSESSMENT_READ_RESPONSE_KEY, "")));

        // Execute
        SBApiResponse response = service.readQuestionList(request, token, false);

        // Verify
        assertEquals(Constants.IDENTIFIER_LIST_IS_EMPTY, response.getParams().getErrmsg());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadQuestionList_Success() throws Exception {
        String token = "token";
        String assessmentId = "assess1";
        List<String> identifierList = List.of("q1");

        // Build requestBody
        Map<String, Object> search = Map.of(Constants.IDENTIFIER, identifierList);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, assessmentId);
        request.put(Constants.REQUEST, Map.of(Constants.SEARCH, search));

        // Setup mocks
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn("user1");

        Map<String, Object> hierarchy = Map.of(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(eq(assessmentId), anyBoolean(), eq(token)))
                .thenReturn(hierarchy);

        String assessmentJson = "{ \"primaryCategory\": \"Assessment\", \"children\": [{ \"childNodes\": [\"q1\"] }] }";
        Map<String, Object> userAssessmentDetail = new HashMap<>();
        userAssessmentDetail.put(Constants.PRIMARY_CATEGORY, "Assessment");
        userAssessmentDetail.put(Constants.CHILDREN, List.of(Map.of(Constants.CHILD_NODES, identifierList)));

        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), eq(assessmentId)))
                .thenReturn(List.of(Map.of(Constants.ASSESSMENT_READ_RESPONSE_KEY, assessmentJson)));

        when(mapper.readValue(anyString(), any(TypeReference.class)))
                .thenReturn(userAssessmentDetail);

        Map<String, Object> redisMap = Map.of("q1", Map.of("id", "q1", "text", "Q1"));
        when(assessUtilServ.readQListfromCache(identifierList, assessmentId, false, token))
                .thenReturn(redisMap);

        when(assessUtilServ.filterQuestionMapDetail(anyMap(), eq("Assessment"), anyBoolean()))
                .thenReturn(Map.of("id", "q1"));

        // Run the method
        SBApiResponse response = service.readQuestionList(request, token, false);

        // Assert
        List<?> questions = (List<?>) response.getResult().get(Constants.QUESTIONS);
        assertNotNull(questions);
        assertEquals(1, questions.size());
        assertEquals("q1", ((Map<?, ?>) questions.get(0)).get("id"));
    }

    @Test
    void testCalculateRetakeAttemptsConsumed_CyclicalMode_WithinCycleLimit() {
        String userId = "user1";
        String assessmentId = "assess1";
        int retakeAttemptsAllowed = 6;
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, retakeAttemptsAllowed);
        SBApiResponse response = new SBApiResponse();
        response.setResponseCode(HttpStatus.OK);
        List<Map<String, Object>> userAttempts = createMockAttempts(3);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(userAttempts);
        when(assessUtilServ.hasCoolOffPeriod(assessmentDetail)).thenReturn(true);
        when(assessUtilServ.calculateCyclicalRetakeAttempts(userId, assessmentId, assessmentDetail, userAttempts))
                .thenReturn(3);
        Integer result = ReflectionTestUtils.invokeMethod(service, "calculateRetakeAttemptsConsumed",
                userId, assessmentId, assessmentDetail, retakeAttemptsAllowed, response);
        assertNotNull(result);
        assertEquals(3, result.intValue());
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testCalculateRetakeAttemptsConsumed_CyclicalMode_CycleExhaustedCooloffActive() {
        String userId = "user1";
        String assessmentId = "assess1";
        int retakeAttemptsAllowed = 6;
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, retakeAttemptsAllowed);
        SBApiResponse response = new SBApiResponse();
        response.setResponseCode(HttpStatus.OK);
        List<Map<String, Object>> userAttempts = createMockAttempts(6);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(userAttempts);
        when(assessUtilServ.hasCoolOffPeriod(assessmentDetail)).thenReturn(true);
        when(assessUtilServ.calculateCyclicalRetakeAttempts(userId, assessmentId, assessmentDetail, userAttempts))
                .thenReturn(6);
        when(assessUtilServ.validateCoolOffPeriod(userId, assessmentId, assessmentDetail, userAttempts))
                .thenReturn("Please wait 1 days");
        Integer result = ReflectionTestUtils.invokeMethod(service, "calculateRetakeAttemptsConsumed",
                userId, assessmentId, assessmentDetail, retakeAttemptsAllowed, response);
        assertNotNull(result);
        assertEquals(6, result.intValue());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertNotNull(response.getParams().getErrmsg());
    }

    @Test
    void testCalculateRetakeAttemptsConsumed_CyclicalMode_CooloffExpired_NewCycle() {
        String userId = "user1";
        String assessmentId = "assess1";
        int retakeAttemptsAllowed = 6;
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, retakeAttemptsAllowed);
        SBApiResponse response = new SBApiResponse();
        response.setResponseCode(HttpStatus.OK);
        List<Map<String, Object>> userAttempts = createMockAttempts(6);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(userAttempts);
        when(assessUtilServ.hasCoolOffPeriod(assessmentDetail)).thenReturn(true);
        when(assessUtilServ.calculateCyclicalRetakeAttempts(userId, assessmentId, assessmentDetail, userAttempts))
                .thenReturn(6);
        when(assessUtilServ.validateCoolOffPeriod(userId, assessmentId, assessmentDetail, userAttempts))
                .thenReturn(Constants.EMPTY); // Cooloff expired
        Integer result = ReflectionTestUtils.invokeMethod(service, "calculateRetakeAttemptsConsumed",
                userId, assessmentId, assessmentDetail, retakeAttemptsAllowed, response);
        assertNotNull(result);
        assertEquals(0, result.intValue()); // New cycle starts
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testCalculateRetakeAttemptsConsumed_NonCyclicalMode_TotalAttempts() {
        String userId = "user1";
        String assessmentId = "assess1";
        int retakeAttemptsAllowed = 6;
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, retakeAttemptsAllowed);
        SBApiResponse response = new SBApiResponse();
        response.setResponseCode(HttpStatus.OK);
        List<Map<String, Object>> userAttempts = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Map<String, Object> attempt = new HashMap<>();
            attempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            userAttempts.add(attempt);
        }
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(userAttempts);
        when(assessUtilServ.hasCoolOffPeriod(assessmentDetail)).thenReturn(false);
        Integer result = ReflectionTestUtils.invokeMethod(service, "calculateRetakeAttemptsConsumed",
                userId, assessmentId, assessmentDetail, retakeAttemptsAllowed, response);
        assertNotNull(result);
        assertEquals(4, result.intValue());
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testCalculateRetakeAttemptsConsumed_NonCyclicalMode_NoAttempts() {
        String userId = "user1";
        String assessmentId = "assess1";
        int retakeAttemptsAllowed = 6;
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, retakeAttemptsAllowed);
        SBApiResponse response = new SBApiResponse();
        response.setResponseCode(HttpStatus.OK);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId))
                .thenReturn(new ArrayList<>());
        when(assessUtilServ.hasCoolOffPeriod(assessmentDetail)).thenReturn(false);
        Integer result = ReflectionTestUtils.invokeMethod(service, "calculateRetakeAttemptsConsumed",
                userId, assessmentId, assessmentDetail, retakeAttemptsAllowed, response);
        assertNotNull(result);
        assertEquals(0, result.intValue());
    }

    @Test
    void testCalculateRetakeAttemptsConsumed_CyclicalMode_FreshCycleAfterGap() {
        String userId = "user1";
        String assessmentId = "assess1";
        int retakeAttemptsAllowed = 6;
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, retakeAttemptsAllowed);
        SBApiResponse response = new SBApiResponse();
        response.setResponseCode(HttpStatus.OK);
        List<Map<String, Object>> userAttempts = createMockAttempts(7); // 1 new + 6 old
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(userAttempts);
        when(assessUtilServ.hasCoolOffPeriod(assessmentDetail)).thenReturn(true);
        when(assessUtilServ.calculateCyclicalRetakeAttempts(userId, assessmentId, assessmentDetail, userAttempts))
                .thenReturn(1); // Cycle boundary detected, only counting new attempt
        Integer result = ReflectionTestUtils.invokeMethod(service, "calculateRetakeAttemptsConsumed",
                userId, assessmentId, assessmentDetail, retakeAttemptsAllowed, response);
        assertNotNull(result);
        assertEquals(1, result.intValue());
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    private List<Map<String, Object>> createMockAttempts(int count) {
        List<Map<String, Object>> attempts = new ArrayList<>();
        Instant now = Instant.now();
        for (int i = 0; i < count; i++) {
            Map<String, Object> attempt = new HashMap<>();
            attempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempt.put(Constants.END_TIME, now.minusSeconds(i * 3600L));
            attempts.add(attempt);
        }
        return attempts;
    }

    @Test
    void testGetShuffleFlagFromHierarchy_ShuffleTrueForMatchingSection() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.SHUFFLE, true);
        hierarchy.put(Constants.CHILDREN, List.of(
                Map.of(Constants.IDENTIFIER, "q1"), Map.of(Constants.IDENTIFIER, "q2")
        ));
        Method method = AssessmentServiceV4Impl.class.getDeclaredMethod("getShuffleFlagFromHierarchy", Map.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(service, hierarchy);
        assertTrue(result);
    }

    @Test
    void testGetShuffleFlagFromHierarchy_ShuffleFalseForMatchingSection() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.SHUFFLE, false);
        hierarchy.put(Constants.CHILDREN, List.of(
                Map.of(Constants.IDENTIFIER, "q1"), Map.of(Constants.IDENTIFIER, "q2")
        ));
        Method method = AssessmentServiceV4Impl.class.getDeclaredMethod("getShuffleFlagFromHierarchy", Map.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(service, hierarchy);
        assertFalse(result);
    }

    @Test
    void testGetShuffleFlagFromHierarchy_EmptySections_ReturnsTrue() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CHILDREN, Collections.emptyList());
        Method method = AssessmentServiceV4Impl.class.getDeclaredMethod("getShuffleFlagFromHierarchy", Map.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(service, hierarchy);
        assertTrue(result);
    }

    @Test
    void testGetShuffleFlagFromHierarchy_NoMatchingSection_ReturnsTrue() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CHILDREN, List.of(
                Map.of(Constants.IDENTIFIER, "q1")
        ));
        Method method = AssessmentServiceV4Impl.class.getDeclaredMethod("getShuffleFlagFromHierarchy", Map.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(service, hierarchy);
        assertTrue(result);
    }

    @Test
    void testGetShuffleFlagFromHierarchy_NullChildren_ReturnsTrue() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        Method method = AssessmentServiceV4Impl.class.getDeclaredMethod("getShuffleFlagFromHierarchy", Map.class);
        method.setAccessible(true);
        boolean result = (boolean) method.invoke(service, hierarchy);
        assertTrue(result);
    }


}

