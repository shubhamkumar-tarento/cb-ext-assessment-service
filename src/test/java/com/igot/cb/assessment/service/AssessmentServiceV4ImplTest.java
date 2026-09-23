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
import java.time.temporal.ChronoUnit;
import java.util.*;

@SuppressWarnings("unchecked")
class AssessmentServiceV4ImplTest {

    private static final String TOKEN = "token";
    private static final String USER_ID = "user1";
    private static final String ASSESSMENT_ID = "assess1";
    private static final String TOPIC = "assessment-submit";

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
        when(serverProperties.getAssessmentSubmitTopic()).thenReturn(TOPIC);
        when(serverProperties.getMandatoryContextCategoriesForPassRequirement())
                .thenReturn(List.of("Final Milestone Assessment"));
        when(serverProperties.getMandatoryCourseCategoriesForCertificateGeneration())
                .thenReturn(List.of("Mandatory Course"));
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
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

    // ==================================================================
    // Additional branch-focused tests (merged from AssessmentServiceV4ImplCoverageTest)
    // ==================================================================

    // ---------------------------------------------------------------- helpers

    private Map<String, Object> hierarchyWithSection(String primaryCategory, Integer maxQuestions) {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, primaryCategory);
        hierarchy.put(Constants.EXPECTED_DURATION, 600);
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.PRIMARY_CATEGORY, "Section");
        section.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);
        section.put(Constants.OBJECT_TYPE, "QuestionSet");
        List<Map<String, Object>> questions = new ArrayList<>();
        questions.add(new HashMap<>(Map.of(Constants.IDENTIFIER, "q1")));
        questions.add(new HashMap<>(Map.of(Constants.IDENTIFIER, "q2")));
        questions.add(new HashMap<>(Map.of(Constants.IDENTIFIER, "q3")));
        section.put(Constants.CHILDREN, questions);
        if (maxQuestions != null) {
            section.put(Constants.MAX_QUESTIONS, maxQuestions);
        }
        hierarchy.put(Constants.CHILDREN, new ArrayList<>(List.of(section)));
        return hierarchy;
    }

    private Map<String, Object> hierarchySection(String id) {
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, id);
        section.put(Constants.PRIMARY_CATEGORY, "Section");
        section.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);
        section.put(Constants.OBJECT_TYPE, "QuestionSet");
        section.put(Constants.CHILDREN, new ArrayList<>(List.of("q1")));
        return section;
    }

    private Map<String, Object> submitSection(String id, String... questionIds) {
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, id);
        List<Map<String, Object>> questions = new ArrayList<>();
        for (String qid : questionIds) {
            questions.add(new HashMap<>(Map.of(Constants.IDENTIFIER, qid)));
        }
        section.put(Constants.CHILDREN, questions);
        return section;
    }

    private Map<String, Object> submitHierarchy(String primaryCategory, String cutOff, String contextCategory,
                                                List<Map<String, Object>> sections) {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, primaryCategory);
        hierarchy.put(Constants.SCORE_CUTOFF_TYPE, cutOff);
        hierarchy.put(Constants.EXPECTED_DURATION, 10);
        hierarchy.put(Constants.CHILDREN, sections);
        if (contextCategory != null) {
            hierarchy.put(Constants.CONTEXT_CATEGORY_TAG, contextCategory);
        }
        return hierarchy;
    }

    private Map<String, Object> storedQuestionSet(boolean withStartTime, String... sectionIds) {
        Map<String, Object> questionSet = new HashMap<>();
        List<Map<String, Object>> sections = new ArrayList<>();
        for (String id : sectionIds) {
            Map<String, Object> s = new HashMap<>();
            s.put(Constants.IDENTIFIER, id);
            s.put(Constants.CHILD_NODES, List.of("q1"));
            sections.add(s);
        }
        questionSet.put(Constants.CHILDREN, sections);
        if (withStartTime) {
            questionSet.put(Constants.START_TIME, Instant.now().toEpochMilli());
        }
        return questionSet;
    }

    private Map<String, Object> existingRecord(Object startTime, String readResponse) {
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        if (startTime != null) {
            existing.put(Constants.START_TIME, startTime);
        }
        if (readResponse != null) {
            existing.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, readResponse);
        }
        return existing;
    }

    private Map<String, Object> scoreResult(double result) {
        Map<String, Object> score = new HashMap<>();
        score.put(Constants.RESULT, result);
        score.put(Constants.TOTAL, 1);
        score.put(Constants.BLANK, 0);
        score.put(Constants.CORRECT, result > 0 ? 1 : 0);
        score.put(Constants.INCORRECT, result > 0 ? 0 : 1);
        score.put(Constants.CHILDREN, new ArrayList<>());
        return score;
    }

    private Map<String, Object> submitRequest(List<Map<String, Object>> sections) {
        Map<String, Object> req = new HashMap<>();
        req.put(Constants.IDENTIFIER, ASSESSMENT_ID);
        req.put(Constants.COURSE_ID, "course1");
        req.put(Constants.BATCH_ID, "batch1");
        req.put(Constants.CHILDREN, sections);
        return req;
    }

    private void stubSubmitFlow(Map<String, Object> hierarchy, Map<String, Object> existing,
                                Map<String, Object> storedQuestionSet, double score, Boolean dbUpdated)
            throws Exception {
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(existing == null ? Collections.emptyList() : List.of(existing));
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(storedQuestionSet);
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(new HashMap<>());
        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap())).thenReturn(scoreResult(score));
        when(assessUtilServ.parseStartTimeToInstant(any())).thenReturn(Instant.now());
        when(assessmentRepository.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(dbUpdated);
    }

    // ------------------------------------------------------------- retake

    @Test
    void retakeZeroAttemptsAllowedReturnsMinusOneConsumed() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 0);
        hierarchy.put(Constants.CONTEXT_CATEGORY_TAG, "Other");
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);

        SBApiResponse resp = service.retakeAssessmentByUserId(ASSESSMENT_ID, USER_ID, false, TOKEN);

        assertEquals(0, resp.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(-1, resp.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
    }

    @Test
    void retakeVerificationEnabledCountsSubmittedAttempts() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 5);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(serverProperties.isAssessmentRetakeCountVerificationEnabled()).thenReturn(true);
        Map<String, Object> submitted = new HashMap<>();
        submitted.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "{}");
        Map<String, Object> nullResponse = new HashMap<>();
        nullResponse.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, null);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID))
                .thenReturn(List.of(submitted, nullResponse, new HashMap<>()));
        when(assessUtilServ.hasCoolOffPeriod(hierarchy)).thenReturn(false);

        SBApiResponse resp = service.retakeAssessmentByUserId(ASSESSMENT_ID, USER_ID, false, TOKEN);

        assertEquals(5, resp.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(1, resp.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void retakeVerificationEnabledCoolOffActiveReturnsBadRequest() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 2);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(serverProperties.isAssessmentRetakeCountVerificationEnabled()).thenReturn(true);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(new ArrayList<>());
        when(assessUtilServ.hasCoolOffPeriod(hierarchy)).thenReturn(true);
        when(assessUtilServ.calculateCyclicalRetakeAttempts(eq(USER_ID), eq(ASSESSMENT_ID), eq(hierarchy), anyList()))
                .thenReturn(2);
        when(assessUtilServ.validateCoolOffPeriod(eq(USER_ID), eq(ASSESSMENT_ID), eq(hierarchy), anyList()))
                .thenReturn("Cool off active");

        SBApiResponse resp = service.retakeAssessmentByUserId(ASSESSMENT_ID, USER_ID, false, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
        assertEquals("Cool off active", resp.getParams().getErrmsg());
        assertFalse(resp.getResult().containsKey(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void retakeNoMaxAttemptsConfiguredDefaultsToZero() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);

        SBApiResponse resp = service.retakeAssessmentByUserId(ASSESSMENT_ID, USER_ID, false, TOKEN);

        assertEquals(0, resp.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, resp.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    // ------------------------------------------------------------- readAssessment

    @Test
    void readAssessmentBlankUserReturnsError() {
        when(accessTokenValidator.fetchUserIdFromAccessToken("other")).thenReturn(" ");
        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, "other", false, null);
        assertEquals(Constants.USER_ID_DOESNT_EXIST, resp.getParams().getErrmsg());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    @Test
    void readAssessmentPracticeQuestionSetReturnsFilteredQuestionSet() {
        Map<String, Object> hierarchy = hierarchyWithSection(Constants.PRACTICE_QUESTION_SET, 2);
        // params missing from the hierarchy / section must simply be left out of the filtered copy
        hierarchy.remove(Constants.EXPECTED_DURATION);
        ((List<Map<String, Object>>) hierarchy.get(Constants.CHILDREN)).get(0).remove(Constants.OBJECT_TYPE);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, null);

        Map<String, Object> questionSet = (Map<String, Object>) resp.getResult().get(Constants.QUESTION_SET);
        assertNotNull(questionSet);
        assertFalse(questionSet.containsKey(Constants.EXPECTED_DURATION));
        List<Map<String, Object>> sections = (List<Map<String, Object>>) questionSet.get(Constants.CHILDREN);
        assertEquals(1, sections.size());
        assertFalse(sections.get(0).containsKey(Constants.OBJECT_TYPE));
        assertEquals("section1", sections.get(0).get(Constants.IDENTIFIER));
        assertEquals(2, ((List<String>) sections.get(0).get(Constants.CHILD_NODES)).size());
        assertEquals(List.of("section1"), questionSet.get(Constants.CHILD_NODES));
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
    }

    @Test
    void readAssessmentEditModeReadsLiveHierarchy() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.fetchHierarchyFromAssessServc(ASSESSMENT_ID, TOKEN)).thenReturn(hierarchy);

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, true, null);

        Map<String, Object> questionSet = (Map<String, Object>) resp.getResult().get(Constants.QUESTION_SET);
        List<Map<String, Object>> sections = (List<Map<String, Object>>) questionSet.get(Constants.CHILDREN);
        assertEquals(3, ((List<String>) sections.get(0).get(Constants.CHILD_NODES)).size());
        verify(assessUtilServ, never()).readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString());
    }

    @Test
    void readAssessmentFirstAttemptSuccess() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(new ArrayList<>());
        when(assessUtilServ.validateContextLocking(any(), any(), any(), any(), any())).thenReturn("");
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(true);

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, "ctx1");

        Map<String, Object> questionSet = (Map<String, Object>) resp.getResult().get(Constants.QUESTION_SET);
        assertNotNull(questionSet.get(Constants.START_TIME));
        assertNotNull(questionSet.get(Constants.END_TIME));
        assertNotEquals(Constants.FAILED, resp.getParams().getStatus());
        verify(assessmentRepository).addUserAssesmentDataToDB(eq(USER_ID), eq(ASSESSMENT_ID), any(), any(), any(),
                eq(Constants.NOT_SUBMITTED));
    }

    @Test
    void readAssessmentFirstAttemptDbUpdateFails() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(new ArrayList<>());
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(false);

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, null);

        assertEquals(Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED, resp.getParams().getErrmsg());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
    }

    @Test
    void readAssessmentFirstAttemptContextLockFails() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(new ArrayList<>());
        when(assessUtilServ.validateContextLocking(any(), any(), any(), any(), any())).thenReturn("locked");

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, "ctx1");

        assertNull(resp.getResult().get(Constants.QUESTION_SET));
        assertNotEquals("locked", resp.getParams().getErrmsg());
        verify(assessmentRepository, never()).addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any());
    }

    @Test
    void readAssessmentExistingInProgressInstantEndTimeResumesAttempt() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Instant.now().plus(1, ChronoUnit.HOURS));
        existing.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        existing.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"primaryCategory\":\"Course Assessment\"}");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(existing));

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, null);

        Map<String, Object> questionSet = (Map<String, Object>) resp.getResult().get(Constants.QUESTION_SET);
        assertEquals("Course Assessment", questionSet.get(Constants.PRIMARY_CATEGORY));
        assertTrue(questionSet.get(Constants.START_TIME) instanceof Long);
        verify(assessmentRepository, never()).addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any());
    }

    @Test
    void readAssessmentExistingSubmittedWithinWindowRestartsAttempt() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().plus(1, ChronoUnit.HOURS)));
        existing.put(Constants.STATUS, Constants.SUBMITTED);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(existing));
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(true);

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, null);

        Map<String, Object> questionSet = (Map<String, Object>) resp.getResult().get(Constants.QUESTION_SET);
        assertTrue(questionSet.get(Constants.END_TIME) instanceof Long);
        assertNotEquals(Constants.FAILED, resp.getParams().getStatus());
    }

    @Test
    void readAssessmentExistingExpiredRestartFailsDbUpdate() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().minus(1, ChronoUnit.HOURS)));
        existing.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(existing));
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(false);

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, null);

        assertEquals(Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED, resp.getParams().getErrmsg());
    }

    @Test
    void readAssessmentExistingExpiredContextLockFails() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().minus(1, ChronoUnit.HOURS)));
        existing.put(Constants.STATUS, Constants.SUBMITTED);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(existing));
        when(assessUtilServ.validateContextLocking(any(), any(), any(), any(), any())).thenReturn("locked");

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, "ctx");

        assertNull(resp.getResult().get(Constants.QUESTION_SET));
        verify(assessmentRepository, never()).addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any());
    }

    @Test
    void readAssessmentExistingWithinWindowUnknownStatusReturnsNoQuestionSet() {
        Map<String, Object> hierarchy = hierarchyWithSection("Course Assessment", null);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().plus(1, ChronoUnit.HOURS)));
        existing.put(Constants.STATUS, "IN_REVIEW");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(existing));

        SBApiResponse resp = service.readAssessment(ASSESSMENT_ID, TOKEN, false, null);

        assertNull(resp.getResult().get(Constants.QUESTION_SET));
        assertNotEquals(Constants.FAILED, resp.getParams().getStatus());
        verify(assessUtilServ, never()).validateContextLocking(any(), any(), any(), any(), any());
    }

    // ------------------------------------------------------------- readQuestionList

    private Map<String, Object> questionListRequest(List<String> ids) {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, ASSESSMENT_ID);
        request.put(Constants.REQUEST, Map.of(Constants.SEARCH, Map.of(Constants.IDENTIFIER, ids)));
        return request;
    }

    @Test
    void readQuestionListBlankUserBadRequest() {
        when(accessTokenValidator.fetchUserIdFromAccessToken("bad")).thenReturn(null);
        SBApiResponse resp = service.readQuestionList(questionListRequest(List.of("q1")), "bad", false);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, resp.getParams().getErrmsg());
    }

    @Test
    void readQuestionListEmptyHierarchyBadRequest() {
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(new HashMap<>());
        SBApiResponse resp = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN, false);
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, resp.getParams().getErrmsg());
    }

    @Test
    void readQuestionListPracticeUsesHierarchyShuffleFlag() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        hierarchy.put(Constants.SHUFFLE, false);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        Map<String, Object> q1 = Map.of(Constants.IDENTIFIER, "q1");
        when(assessUtilServ.readQListfromCache(List.of("q1"), ASSESSMENT_ID, false, TOKEN)).thenReturn(Map.of("q1", q1));
        when(assessUtilServ.filterQuestionMapDetail(q1, Constants.PRACTICE_QUESTION_SET, false)).thenReturn(q1);

        SBApiResponse resp = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN, false);

        assertEquals(List.of(q1), resp.getResult().get(Constants.QUESTIONS));
        verify(assessUtilServ).filterQuestionMapDetail(q1, Constants.PRACTICE_QUESTION_SET, false);
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
    }

    @Test
    void readQuestionListEditModeSkipsUserRecordCheck() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Course Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, true, TOKEN)).thenReturn(hierarchy);
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(new HashMap<>());
        when(assessUtilServ.filterQuestionMapDetail(any(), anyString(), anyBoolean())).thenReturn(Map.of("id", "q1"));

        SBApiResponse resp = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN, true);

        assertEquals(1, ((List<?>) resp.getResult().get(Constants.QUESTIONS)).size());
        verify(assessUtilServ).filterQuestionMapDetail(null, "Course Assessment", true);
    }

    @Test
    void readQuestionListNoUserRecordsBadRequest() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Course Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(new ArrayList<>());

        SBApiResponse resp = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN, false);

        assertEquals(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, resp.getParams().getErrmsg());
    }

    @Test
    void readQuestionListEmptyUserAssessmentInvalidAssessmentId() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Course Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID))
                .thenReturn(List.of(Map.of(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{}")));
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(new HashMap<>());

        SBApiResponse resp = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN, false);

        assertEquals(Constants.ASSESSMENT_ID_INVALID, resp.getParams().getErrmsg());
    }

    @Test
    void readQuestionListQuestionIdsDontMatchBadRequest() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Course Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID))
                .thenReturn(List.of(Map.of(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{...}")));
        Map<String, Object> userDetail = new HashMap<>();
        userDetail.put(Constants.PRIMARY_CATEGORY, "Course Assessment");
        userDetail.put(Constants.CHILDREN, List.of(Map.of(Constants.CHILD_NODES, List.of("q1"))));
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(userDetail);

        SBApiResponse resp = service.readQuestionList(questionListRequest(List.of("q1", "q9")), TOKEN, false);

        assertEquals(Constants.THE_QUESTIONS_IDS_PROVIDED_DONT_MATCH, resp.getParams().getErrmsg());
        verify(assessUtilServ, never()).readQListfromCache(anyList(), anyString(), anyBoolean(), anyString());
    }

    @Test
    void readQuestionListCacheThrowsBadRequest() throws Exception {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        when(assessUtilServ.readAssessmentHierarchyFromCache(ASSESSMENT_ID, false, TOKEN)).thenReturn(hierarchy);
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenThrow(new RuntimeException("redis down"));

        SBApiResponse resp = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN, false);

        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
        assertTrue(resp.getParams().getErrmsg().contains("redis down"));
    }

    @Test
    void getQuestionIdListVariousMalformedRequestsReturnEmpty() {
        List<Map<String, Object>> requests = new ArrayList<>();
        requests.add(new HashMap<>());
        requests.add(Map.of(Constants.REQUEST, new HashMap<>()));
        requests.add(Map.of(Constants.REQUEST, Map.of("other", "x")));
        requests.add(Map.of(Constants.REQUEST, Map.of(Constants.SEARCH, new HashMap<>())));
        requests.add(Map.of(Constants.REQUEST, Map.of(Constants.SEARCH, Map.of("other", "x"))));
        requests.add(Map.of(Constants.REQUEST, Map.of(Constants.SEARCH, Map.of(Constants.IDENTIFIER, List.of()))));
        requests.add(Map.of(Constants.REQUEST, "not-a-map"));
        for (Map<String, Object> req : requests) {
            List<String> ids = ReflectionTestUtils.invokeMethod(service, "getQuestionIdList", req);
            assertNotNull(ids);
            assertTrue(ids.isEmpty(), "Expected empty ids for " + req);
        }
    }

    // ------------------------------------------------------------- submitAssessment / wheebox

    @Test
    void submitAssessmentNotImplemented() {
        SBApiResponse resp = service.submitAssessment(new HashMap<>(), TOKEN);
        assertEquals(HttpStatus.NOT_IMPLEMENTED, resp.getResponseCode());
        assertEquals("Method not supported", resp.getParams().getErrmsg());
    }

    @Test
    void readWheeboxNullAndEmptyResultNoResponseKey() {
        when(assessUtilServ.fetchWheebox(USER_ID)).thenReturn(null).thenReturn(new HashMap<>());
        assertFalse(service.readWheebox(TOKEN).getResult().containsKey(Constants.RESPONSE));
        assertFalse(service.readWheebox(TOKEN).getResult().containsKey(Constants.RESPONSE));
    }

    // ------------------------------------------------------------- readAssessmentResultV4

    private Map<String, Object> resultRequest() {
        Map<String, Object> body = new HashMap<>();
        body.put(Constants.ASSESSMENT_ID_KEY, ASSESSMENT_ID);
        body.put(Constants.COURSE_ID, "course1");
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, body);
        return request;
    }

    @Test
    void readAssessmentResultV4SubmittedWithBlankResponseReturnsEmptyResult() {
        Map<String, Object> submittedRecord = new HashMap<>();
        submittedRecord.put(Constants.STATUS, Constants.SUBMITTED);
        submittedRecord.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(submittedRecord));

        SBApiResponse resp = service.readAssessmentResultV4(resultRequest(), TOKEN);

        assertNotEquals(Constants.FAILED, resp.getParams().getStatus());
        verifyNoInteractions(mapper);
    }

    @Test
    void readAssessmentResultV4MapperFailsInternalError() throws Exception {
        Map<String, Object> submittedRecord = new HashMap<>();
        submittedRecord.put(Constants.STATUS, Constants.SUBMITTED);
        submittedRecord.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "{bad");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(submittedRecord));
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenThrow(new RuntimeException("parse"));

        SBApiResponse resp = service.readAssessmentResultV4(resultRequest(), TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
        assertTrue(resp.getParams().getErrmsg().contains("parse"));
    }

    @Test
    void readAssessmentResultV4InvalidRequests() {
        assertEquals(Constants.INVALID_REQUEST,
                service.readAssessmentResultV4(new HashMap<>(), TOKEN).getParams().getErrmsg());
        assertEquals(Constants.INVALID_REQUEST,
                service.readAssessmentResultV4(Map.of("x", "y"), TOKEN).getParams().getErrmsg());
        assertEquals(Constants.INVALID_REQUEST,
                service.readAssessmentResultV4(Map.of(Constants.REQUEST, new HashMap<>()), TOKEN)
                        .getParams().getErrmsg());

        Map<String, Object> body = new HashMap<>();
        body.put(Constants.ASSESSMENT_ID_KEY, " ");
        body.put(Constants.COURSE_ID, " ");
        SBApiResponse resp = service.readAssessmentResultV4(Map.of(Constants.REQUEST, body), TOKEN);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
        assertTrue(resp.getParams().getErrmsg().contains(Constants.ASSESSMENT_ID_KEY));
        assertTrue(resp.getParams().getErrmsg().contains(Constants.COURSE_ID));

        Map<String, Object> body2 = new HashMap<>();
        body2.put(Constants.ASSESSMENT_ID_KEY, ASSESSMENT_ID);
        SBApiResponse resp2 = service.readAssessmentResultV4(Map.of(Constants.REQUEST, body2), TOKEN);
        assertTrue(resp2.getParams().getErrmsg().contains(Constants.COURSE_ID));
    }

    // ------------------------------------------------------------- submitAssessmentAsync

    @Test
    void submitAsyncLanguageValidationFails() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy(Constants.PRACTICE_QUESTION_SET,
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>());
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        when(assessUtilServ.validateAssessmentLanguageAndNodes(anyMap())).thenReturn("bad language");

        SBApiResponse resp = service.submitAssessmentAsync(submitRequest(new ArrayList<>()), TOKEN, false);

        assertEquals("bad language", resp.getParams().getErrmsg());
        assertEquals(HttpStatus.BAD_REQUEST, resp.getResponseCode());
    }

    @Test
    void submitAsyncNoUserRecord() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>());
        stubSubmitFlow(hierarchy, null, null, 0, true);

        SBApiResponse resp = service.submitAssessmentAsync(submitRequest(new ArrayList<>()), TOKEN, false);

        assertEquals(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, resp.getParams().getErrmsg());
    }

    @Test
    void submitAsyncMissingStartTime() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>());
        stubSubmitFlow(hierarchy, existingRecord(null, "{}"), null, 0, true);

        SBApiResponse resp = service.submitAssessmentAsync(submitRequest(new ArrayList<>()), TOKEN, false);

        assertEquals(Constants.READ_ASSESSMENT_START_TIME_FAILED, resp.getParams().getErrmsg());
    }

    @Test
    void submitAsyncSubmissionExpiredStringStartTime() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>());
        String startTime = Instant.now().minus(2, ChronoUnit.DAYS).toString();
        stubSubmitFlow(hierarchy, existingRecord(startTime, "{}"), null, 0, true);

        SBApiResponse resp = service.submitAssessmentAsync(submitRequest(new ArrayList<>()), TOKEN, false);

        assertEquals(Constants.ASSESSMENT_SUBMIT_EXPIRED, resp.getParams().getErrmsg());
    }

    @Test
    void submitAsyncWrongSectionDetailsInstantStartTime() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(Instant.now(), "{}"), null, 0, true);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("unknown", "q1")))), TOKEN, false);

        assertEquals(Constants.WRONG_SECTION_DETAILS, resp.getParams().getErrmsg());
    }

    @Test
    void submitAsyncStoredQuestionSetBlankQuestionReadFailed() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), ""), null, 0, true);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN, false);

        assertEquals(Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED, resp.getParams().getErrmsg());
    }

    @Test
    void submitAsyncInvalidQuestionSubmitted() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 0, true);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1", "q99")))), TOKEN, false);

        assertEquals(Constants.ASSESSMENT_SUBMIT_INVALID_QUESTION, resp.getParams().getErrmsg());
    }

    @Test
    void submitAsyncAssessmentLevelPassesUpdatesProgressAndPublishes() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 80.0, true);
        when(contentService.readContent("course1")).thenReturn(Map.of(Constants.COURSE_CATEGORY, "Mandatory Course"));

        Map<String, Object> request = submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"))));
        SBApiResponse resp = service.submitAssessmentAsync(request, TOKEN, false);

        assertEquals(Boolean.TRUE, resp.getResult().get(Constants.PASS));
        assertEquals(80.0, resp.getResult().get(Constants.OVERALL_RESULT));
        assertEquals("Course Assessment", resp.getResult().get(Constants.PRIMARY_CATEGORY));
        verify(contentService).updateContentProgress(eq(TOKEN), eq(request), eq(USER_ID), any());
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer).push(eq(TOPIC), captor.capture());
        Map<String, Object> event = (Map<String, Object>) captor.getValue();
        assertEquals("course1", event.get(Constants.COURSE_ID));
        assertEquals("batch1", event.get(Constants.BATCH_ID));
        assertEquals(80.0, event.get(Constants.TOTAL_SCORE));
    }

    @Test
    void submitAsyncAssessmentLevelPreEnrolledContextUpdatesPreEnrolled() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, Constants.PRE_ENROLLED_ASSESSMENT_KEY,
                new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 20.0, true);

        Map<String, Object> request = submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"))));
        request.remove(Constants.COURSE_ID);
        request.remove(Constants.BATCH_ID);
        SBApiResponse resp = service.submitAssessmentAsync(request, TOKEN, false);

        assertEquals(Boolean.FALSE, resp.getResult().get(Constants.PASS));
        verify(contentService).updatePreEnrolledAssessment(eq(TOKEN), eq(request), eq(USER_ID), any());
        verify(contentService, never()).updateContentProgress(any(), any(), any(), any());
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer).push(eq(TOPIC), captor.capture());
        Map<String, Object> event = (Map<String, Object>) captor.getValue();
        assertEquals("", event.get(Constants.COURSE_ID));
        assertEquals("", event.get(Constants.BATCH_ID));
    }

    @Test
    void submitAsyncMandatoryContextCategoryFailedSkipsPublish() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, "Final Milestone Assessment",
                new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 10.0, true);

        service.submitAssessmentAsync(submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN,
                false);

        verify(assessmentRepository).updateUserAssesmentDataToDB(eq(USER_ID), eq(ASSESSMENT_ID), any(), any(),
                eq(Constants.SUBMITTED), any(), isNull());
        verifyNoInteractions(kafkaProducer);
        verify(contentService, never()).updateContentProgress(any(), any(), any(), any());
    }

    @Test
    void submitAsyncMandatoryContextCategoryPassedPublishes() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, "Final Milestone Assessment",
                new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 90.0, true);

        service.submitAssessmentAsync(submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN,
                false);

        verify(kafkaProducer).push(eq(TOPIC), any());
    }

    @Test
    void submitAsyncMandatoryCourseCategoryFailedSkipsPublish() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, "Some Context",
                new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 10.0, true);
        when(contentService.readContent("course1")).thenReturn(Map.of(Constants.COURSE_CATEGORY, "mandatory course"));

        service.submitAssessmentAsync(submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN,
                false);

        verifyNoInteractions(kafkaProducer);
    }

    @Test
    void submitAsyncDbUpdateFailsSkipsPublish() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 90.0, false);

        service.submitAssessmentAsync(submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN,
                false);

        verifyNoInteractions(kafkaProducer);
    }

    @Test
    void submitAsyncStoredQuestionSetWithoutStartTimeSkipsDbWrite() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(false, "s1"), 90.0, true);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN, false);

        assertEquals(Boolean.TRUE, resp.getResult().get(Constants.PASS));
        verify(assessmentRepository, never()).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(),
                any());
    }

    @Test
    void submitAsyncStoredQuestionSetWithoutChildrenSkipsQuestionValidation() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        Map<String, Object> stored = new HashMap<>();
        stored.put(Constants.START_TIME, 1L);
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), stored, 90.0, true);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1", "q2")))), TOKEN, false);

        assertEquals(Boolean.TRUE, resp.getResult().get(Constants.PASS));
        verify(assessmentRepository).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void submitAsyncStoredQuestionSetNullSkipsQuestionValidation() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), null, 90.0, true);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN, false);

        // persistSubmissionOutcome then fails on the null question set and is logged, response is still built
        assertEquals(Boolean.TRUE, resp.getResult().get(Constants.PASS));
        verifyNoInteractions(kafkaProducer);
    }

    @Test
    void submitAsyncCompetencyAssessmentAddsCompetencyToEvent() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Competency Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 90.0, true);
        Map<String, Object> request = submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"))));
        request.put(Constants.COMPETENCIES_V3, "[{\"competencyAreaName\":\"Behavioural\"}]");

        service.submitAssessmentAsync(request, TOKEN, false);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer).push(eq(TOPIC), captor.capture());
        Map<String, Object> event = (Map<String, Object>) captor.getValue();
        assertEquals(Map.of("competencyAreaName", "Behavioural"), event.get(Constants.COMPETENCY));
    }

    @Test
    void submitAsyncCompetencyAssessmentEmptyCompetencies() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Competency Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 90.0, true);
        Map<String, Object> request = submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"))));
        request.put(Constants.COMPETENCIES_V3, "[]");

        service.submitAssessmentAsync(request, TOKEN, false);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer).push(eq(TOPIC), captor.capture());
        assertEquals("", ((Map<String, Object>) captor.getValue()).get(Constants.COMPETENCY));
    }

    @Test
    void submitAsyncCompetencyAssessmentNullCompetenciesNoCompetencyKey() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Competency Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 90.0, true);
        Map<String, Object> request = submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"))));
        request.put(Constants.COMPETENCIES_V3, null);

        service.submitAssessmentAsync(request, TOKEN, false);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer).push(eq(TOPIC), captor.capture());
        assertFalse(((Map<String, Object>) captor.getValue()).containsKey(Constants.COMPETENCY));
    }

    @Test
    void submitAsyncEditModeAssessmentLevelDoesNotPersist() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, null, null, 90.0, true);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN, true);

        assertEquals(Boolean.TRUE, resp.getResult().get(Constants.PASS));
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
        verifyNoInteractions(assessmentRepository, kafkaProducer);
    }

    @Test
    void submitAsyncPracticeProgressUpdateFailsStillReturnsResult() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy(Constants.PRACTICE_QUESTION_SET,
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, null, null, 90.0, true);
        when(contentService.updateContentProgress(any(), any(), any(), any())).thenReturn(Constants.FAILED);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN, false);

        assertEquals(Constants.PRACTICE_QUESTION_SET, resp.getResult().get(Constants.PRIMARY_CATEGORY));
        verify(contentService).updateContentProgress(eq(TOKEN), anyMap(), eq(USER_ID), any());
        verifyNoInteractions(assessmentRepository);
    }

    @Test
    void submitAsyncSectionLevelMultipleSectionsAggregatesAndPublishes() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.SECTION_LEVEL_SCORE_CUTOFF, null,
                new ArrayList<>(List.of(hierarchySection("s1"), hierarchySection("s2"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1", "s2"), 60.0, true);
        Map<String, Object> s2 = new HashMap<>();
        s2.put(Constants.IDENTIFIER, "s2");
        s2.put(Constants.CHILDREN, new ArrayList<>());

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"), s2))), TOKEN, false);

        assertEquals(Constants.SUCCESS, resp.getParams().getStatus());
        assertEquals(60.0, resp.getResult().get(Constants.OVERALL_RESULT));
        assertEquals(2, resp.getResult().get(Constants.TOTAL));
        assertEquals(Boolean.TRUE, resp.getResult().get(Constants.PASS));
        verify(assessUtilServ, times(2)).validateQumlAssessment(anyList(), anyList(), anyMap());
        verify(kafkaProducer).push(eq(TOPIC), any());
    }

    @Test
    void submitAsyncSectionLevelEmptyScoresFallBackToHierarchyCounts() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.SECTION_LEVEL_SCORE_CUTOFF, null,
                new ArrayList<>(List.of(hierarchySection("s1"), hierarchySection("s2"), hierarchySection("s3"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1", "s2", "s3"), 30.0,
                true);
        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap())).thenReturn(new HashMap<>());
        // s2 is submitted without a children key; s3 is not submitted at all
        Map<String, Object> s2 = new HashMap<>();
        s2.put(Constants.IDENTIFIER, "s2");

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"), s2))), TOKEN, false);

        // Empty score maps fall back to the hierarchy's child count with a zero result
        assertEquals(0.0, resp.getResult().get(Constants.OVERALL_RESULT));
        assertEquals(Boolean.FALSE, resp.getResult().get(Constants.PASS));
        assertEquals(3, resp.getResult().get(Constants.BLANK));
    }

    @Test
    void submitAsyncCompetencyAssessmentWithoutCompetenciesKeyOmitsCompetency() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Competency Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 90.0, true);

        service.submitAssessmentAsync(submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN,
                false);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer).push(eq(TOPIC), captor.capture());
        Map<String, Object> event = (Map<String, Object>) captor.getValue();
        assertFalse(event.containsKey(Constants.COMPETENCY));
        assertEquals("Competency Assessment", event.get(Constants.PRIMARY_CATEGORY));
    }

    @Test
    void submitAsyncPracticeProgressUpdateSucceeds() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy(Constants.PRACTICE_QUESTION_SET,
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, null, null, 90.0, true);
        when(contentService.updateContentProgress(any(), any(), any(), any())).thenReturn(Constants.SUCCESS);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN, false);

        assertEquals(Boolean.TRUE, resp.getResult().get(Constants.PASS));
        verify(contentService).updateContentProgress(eq(TOKEN), anyMap(), eq(USER_ID), any());
    }

    @Test
    void handleSubmitEmptyCutOffTypeDoesNothing() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment", "", null,
                new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 70.0, true);

        service.handleAssessmentSubmitRequest(asyncRequest(submitRequest(
                new ArrayList<>(List.of(submitSection("s1", "q1"))))), false, TOKEN);

        verify(assessUtilServ, never()).validateQumlAssessment(anyList(), anyList(), anyMap());
        verifyNoInteractions(assessmentRepository, kafkaProducer);
    }

    @Test
    void submitAsyncUnknownCutOffTypeReturnsDefaultResponse() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy(Constants.PRACTICE_QUESTION_SET, "OtherLevel", null,
                new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, null, null, 90.0, true);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN, false);

        assertNotEquals(Constants.FAILED, resp.getParams().getStatus());
        assertFalse(resp.getResult().containsKey(Constants.PASS));
        verify(assessUtilServ, never()).validateQumlAssessment(anyList(), anyList(), anyMap());
    }

    @Test
    void submitAsyncScoreWithoutResultPublishesFailureAudit() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment",
                Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 90.0, true);
        Map<String, Object> badScore = scoreResult(90.0);
        badScore.put(Constants.RESULT, null);
        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap())).thenReturn(badScore);

        SBApiResponse resp = service.submitAssessmentAsync(
                submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1")))), TOKEN, false);

        // null result unboxing fails inside createResponseMapWithProperStructure -> handled as error
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resp.getResponseCode());
        verify(assessUtilServ).publishFailedAssessmentAuditEvent(eq(USER_ID), eq(ASSESSMENT_ID), anyMap(),
                anyString(), eq(Constants.METHOD_V4_SUBMIT_ASSESSMENT_ASYNC), anyMap());
    }

    // ------------------------------------------------------------- handleAssessmentSubmitRequest

    private Map<String, Object> asyncRequest(Map<String, Object> submitRequest) {
        Map<String, Object> async = new HashMap<>();
        async.put(Constants.USER_ID_CONSTANT, USER_ID);
        async.put(Constants.REQUEST, submitRequest);
        return async;
    }

    @Test
    void handleSubmitSectionLevelWritesAggregatedResult() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment", Constants.SECTION_LEVEL_SCORE_CUTOFF,
                null, new ArrayList<>(List.of(hierarchySection("s1"), hierarchySection("s2"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 70.0, true);
        Map<String, Object> request = submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"),
                submitSection("s2", "q1"))));

        service.handleAssessmentSubmitRequest(asyncRequest(request), false, TOKEN);

        ArgumentCaptor<Map<String, Object>> resultCaptor = ArgumentCaptor.forClass(Map.class);
        verify(assessmentRepository).updateUserAssesmentDataToDB(eq(USER_ID), eq(ASSESSMENT_ID), eq(request),
                resultCaptor.capture(), eq(Constants.SUBMITTED), any(), isNull());
        assertEquals(Boolean.FALSE, resultCaptor.getValue().get(Constants.STATUS_IS_IN_PROGRESS));
        assertEquals(70.0, resultCaptor.getValue().get(Constants.OVERALL_RESULT));
        verify(assessUtilServ, times(2)).validateQumlAssessment(anyList(), anyList(), anyMap());
        verify(kafkaProducer).push(eq(TOPIC), any());
        verify(contentService, never()).updateContentProgress(any(), any(), any(), any());
    }

    @Test
    void handleSubmitAssessmentLevelWritesFinalResult() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF,
                null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 70.0, true);
        Map<String, Object> request = submitRequest(new ArrayList<>(List.of(submitSection("s1", "q1"))));

        service.handleAssessmentSubmitRequest(asyncRequest(request), false, TOKEN);

        ArgumentCaptor<Map<String, Object>> resultCaptor = ArgumentCaptor.forClass(Map.class);
        verify(assessmentRepository).updateUserAssesmentDataToDB(eq(USER_ID), eq(ASSESSMENT_ID), eq(request),
                resultCaptor.capture(), eq(Constants.SUBMITTED), any(), isNull());
        assertEquals(Boolean.TRUE, resultCaptor.getValue().get(Constants.PASS));
        verify(kafkaProducer).push(eq(TOPIC), any());
    }

    @Test
    void handleSubmitPracticeSectionLevelSkipsScoring() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy(Constants.PRACTICE_QUESTION_SET,
                Constants.SECTION_LEVEL_SCORE_CUTOFF, null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 70.0, true);

        service.handleAssessmentSubmitRequest(asyncRequest(submitRequest(new ArrayList<>())), false, TOKEN);

        verify(assessUtilServ, never()).validateQumlAssessment(anyList(), anyList(), anyMap());
        // no sections scored -> no stored question set was loaded, so there is no start time to persist against
        verify(assessmentRepository, never()).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(),
                any());
    }

    @Test
    void handleSubmitNoMatchingStoredSectionKeepsPreviousIds() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment", Constants.SECTION_LEVEL_SCORE_CUTOFF,
                null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "other"), 70.0, true);

        service.handleAssessmentSubmitRequest(asyncRequest(submitRequest(
                new ArrayList<>(List.of(submitSection("s1", "q1"))))), false, TOKEN);

        verify(assessUtilServ).readQListfromCache(Collections.emptyList(), ASSESSMENT_ID, false, TOKEN);
    }

    @Test
    void handleSubmitStoredQuestionSetNullStopsScoring() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment", Constants.SECTION_LEVEL_SCORE_CUTOFF,
                null, new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), null, 70.0, true);

        service.handleAssessmentSubmitRequest(asyncRequest(submitRequest(
                new ArrayList<>(List.of(submitSection("s1", "q1"))))), false, TOKEN);

        verify(assessUtilServ, never()).validateQumlAssessment(anyList(), anyList(), anyMap());
        verifyNoInteractions(assessmentRepository);
    }

    @Test
    void handleSubmitUnknownCutOffDoesNothing() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment", "other", null,
                new ArrayList<>(List.of(hierarchySection("s1"))));
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 70.0, true);

        service.handleAssessmentSubmitRequest(asyncRequest(submitRequest(
                new ArrayList<>(List.of(submitSection("s1", "q1"))))), false, TOKEN);

        verify(assessUtilServ, never()).validateQumlAssessment(anyList(), anyList(), anyMap());
        verifyNoInteractions(assessmentRepository, kafkaProducer);
    }

    @Test
    void handleSubmitMissingCutOffTypeExceptionIsSwallowed() throws Exception {
        Map<String, Object> hierarchy = submitHierarchy("Course Assessment", null, null,
                new ArrayList<>(List.of(hierarchySection("s1"))));
        hierarchy.remove(Constants.SCORE_CUTOFF_TYPE);
        stubSubmitFlow(hierarchy, existingRecord(new Date(), "{}"), storedQuestionSet(true, "s1"), 70.0, true);

        assertDoesNotThrow(() -> service.handleAssessmentSubmitRequest(asyncRequest(submitRequest(
                new ArrayList<>(List.of(submitSection("s1", "q1"))))), false, TOKEN));
        verifyNoInteractions(assessmentRepository);
    }

    // ------------------------------------------------------------- private calculators

    @Test
    void calculateAssessmentFinalResultsMissingValuesReturnsPartialResult() {
        Map<String, Object> res = ReflectionTestUtils.invokeMethod(service, "calculateAssessmentFinalResults",
                new HashMap<String, Object>());
        assertNotNull(res);
        assertTrue(res.containsKey(Constants.CHILDREN));
        assertFalse(res.containsKey(Constants.PASS));
    }

    @Test
    void calculateSectionFinalResultsBadSectionReturnsPartialResult() {
        Map<String, Object> bad = new HashMap<>();
        bad.put(Constants.RESULT, 10.0);
        List<Map<String, Object>> sections = List.of(bad);
        Map<String, Object> res = ReflectionTestUtils.invokeMethod(service, "calculateSectionFinalResults",
                sections);
        assertNotNull(res);
        assertFalse(res.containsKey(Constants.OVERALL_RESULT));
    }

    @Test
    void createResponseMapWithProperStructureWithScores() {
        Map<String, Object> section = hierarchySection("s1");
        Map<String, Object> res = service.createResponseMapWithProperStructure(section, scoreResult(40.0));
        assertEquals(40.0, res.get(Constants.RESULT));
        assertEquals(Boolean.FALSE, res.get(Constants.PASS));
        assertEquals(50, res.get(Constants.PASS_PERCENTAGE));
        assertEquals("s1", res.get(Constants.IDENTIFIER));
    }

    @Test
    void calculateAssessmentSubmitTimeWithBufferAddsSubmissionDuration() {
        Instant start = Instant.parse("2024-01-01T00:00:00Z");
        Instant end = ReflectionTestUtils.invokeMethod(service, "calculateAssessmentSubmitTime", 60, start, 1);
        assertEquals(start.plusSeconds(65), end);
    }

    @Test
    void getShuffleFlagFromHierarchyNonBooleanReturnsTrue() {
        Boolean flag = ReflectionTestUtils.invokeMethod(service, "getShuffleFlagFromHierarchy",
                Map.of(Constants.SHUFFLE, "false"));
        assertEquals(Boolean.TRUE, flag);
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

