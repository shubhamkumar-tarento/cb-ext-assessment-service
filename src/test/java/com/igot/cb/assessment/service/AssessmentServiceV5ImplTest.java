package com.igot.cb.assessment.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.assessment.repo.AssessmentRepository;
import com.igot.cb.cassandra.utils.CassandraOperation;
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

class AssessmentServiceV5ImplTest {

    @InjectMocks
    private AssessmentServiceV5Impl service;

    @Mock
    private AccessTokenValidator accessTokenValidator;
    @Mock
    private AssessmentUtilServiceV2 assessUtilServ;
    @Mock
    private AssessmentRepository assessmentRepository;
    @Mock
    private CbExtAssessmentServerProperties serverProperties;
    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    @Mock
    private Producer producer;
    @Mock
    private ContentService contentService;
    @Mock
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        accessTokenValidator = mock(AccessTokenValidator.class);
        assessUtilServ = mock(AssessmentUtilServiceV2.class);
        mapper = new ObjectMapper();
        contentService = mock(ContentService.class);
        serverProperties = mock(CbExtAssessmentServerProperties.class);
        assessmentRepository = mock(AssessmentRepository.class);
        outboundRequestHandlerService = mock(OutboundRequestHandlerServiceImpl.class);
        cassandraOperation=mock(CassandraOperation.class);
        producer = mock(Producer.class);

        service = new AssessmentServiceV5Impl(serverProperties, producer, outboundRequestHandlerService, assessUtilServ, mapper, assessmentRepository, accessTokenValidator, contentService, cassandraOperation, producer);

        // Inject dependencies manually since no constructor is used
        ReflectionTestUtils.setField(service, "accessTokenValidator", accessTokenValidator);
        ReflectionTestUtils.setField(service, "assessUtilServ", assessUtilServ);
        ReflectionTestUtils.setField(service, "mapper", mapper);
        ReflectionTestUtils.setField(service, "contentService", contentService);
        ReflectionTestUtils.setField(service, "serverProperties", serverProperties);
        ReflectionTestUtils.setField(service, "assessmentRepository", assessmentRepository);
        ReflectionTestUtils.setField(service, "outboundRequestHandlerService", outboundRequestHandlerService);
        ReflectionTestUtils.setField(service, "cassandraOperation", cassandraOperation);
        ReflectionTestUtils.setField(service, "producer", producer);

        AssessmentUtilServiceV2Impl realUtil = new AssessmentUtilServiceV2Impl(serverProperties, outboundRequestHandlerService, mapper, cassandraOperation, null, contentService, producer) {
            @Override
            public Map<String, Object> readAssessmentHierarchyFromCache(String assessmentIdentifier, boolean editMode, String token) {
                return assessUtilServ.readAssessmentHierarchyFromCache(assessmentIdentifier, editMode, token);
            }
            @Override
            public List<Map<String, Object>> readUserSubmittedAssessmentRecords(String userId, String assessmentId) {
                return assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId);
            }
            @Override
            public boolean hasCoolOffPeriod(Map<String, Object> assessmentAllDetail) {
                return assessUtilServ.hasCoolOffPeriod(assessmentAllDetail);
            }
            @Override
            public int calculateCyclicalRetakeAttempts(String userId, String assessmentIdentifier,
                    Map<String, Object> assessmentAllDetail, List<Map<String, Object>> userAssessmentDataList) {
                return assessUtilServ.calculateCyclicalRetakeAttempts(userId, assessmentIdentifier, assessmentAllDetail, userAssessmentDataList);
            }
            @Override
            public String validateCoolOffPeriod(String userId, String assessmentIdentifier,
                    Map<String, Object> assessmentAllDetail, List<Map<String, Object>> userAssessmentDataList) {
                return assessUtilServ.validateCoolOffPeriod(userId, assessmentIdentifier, assessmentAllDetail, userAssessmentDataList);
            }
        };
        lenient().when(assessUtilServ.getShuffleFlagFromHierarchy(any())).thenAnswer(inv -> realUtil.getShuffleFlagFromHierarchy(inv.getArgument(0)));
        lenient().when(assessUtilServ.getQuestionIdList(any())).thenAnswer(inv -> realUtil.getQuestionIdList(inv.getArgument(0)));
        lenient().when(assessUtilServ.validateQuestionListRequest(any(), any())).thenAnswer(inv -> realUtil.validateQuestionListRequest(inv.getArgument(0), inv.getArgument(1)));
        lenient().doAnswer(inv -> { realUtil.applyQuestionIdMatch(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2)); return null; }).when(assessUtilServ).applyQuestionIdMatch(any(), any(), any());
        try {
            lenient().when(assessUtilServ.validateQuestionListAPI(any(), any(), any(), anyBoolean(), any())).thenAnswer(inv -> realUtil.validateQuestionListAPI(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2), inv.getArgument(3), inv.getArgument(4)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        lenient().when(assessUtilServ.isMandatoryPassContextCategory(any())).thenAnswer(inv -> realUtil.isMandatoryPassContextCategory(inv.getArgument(0)));
        lenient().when(assessUtilServ.proceedWithContentUpdate(any(), any(), anyBoolean())).thenAnswer(inv -> realUtil.proceedWithContentUpdate(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2)));
        lenient().when(assessUtilServ.validateAssessmentReadResult(any())).thenAnswer(inv -> realUtil.validateAssessmentReadResult(inv.getArgument(0)));
        lenient().when(assessUtilServ.resolveCourseCategory(any())).thenAnswer(inv -> realUtil.resolveCourseCategory(inv.getArgument(0)));
        lenient().when(assessUtilServ.calculateAssessmentSubmitTime(anyInt(), any(), anyInt())).thenAnswer(inv -> realUtil.calculateAssessmentSubmitTime(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2)));
        try {
            lenient().when(assessUtilServ.buildSubmitEvent(any(), any(), any())).thenAnswer(inv -> realUtil.buildSubmitEvent(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        lenient().doAnswer(inv -> { realUtil.updateContentProgressForContext(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2), inv.getArgument(3)); return null; }).when(assessUtilServ).updateContentProgressForContext(any(), any(), any(), any());
        lenient().when(assessUtilServ.calculateRetakeAttemptsConsumed(any(), any(), any(), anyInt(), any(), any(), any()))
                .thenAnswer(inv -> realUtil.calculateRetakeAttemptsConsumed(inv.getArgument(0), inv.getArgument(1),
                        inv.getArgument(2), inv.getArgument(3), inv.getArgument(4), inv.getArgument(5), inv.getArgument(6)));
        lenient().when(assessUtilServ.resolveAssessmentStartTimeAsInstant(any()))
                .thenAnswer(inv -> realUtil.resolveAssessmentStartTimeAsInstant(inv.getArgument(0)));
        lenient().when(assessUtilServ.readAssessmentResult(any(), any(), any()))
                .thenAnswer(inv -> realUtil.readAssessmentResult(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2)));
        try {
            lenient().when(assessUtilServ.validateIfQuestionIdsAreSame(any(), any(), any()))
                    .thenAnswer(inv -> realUtil.validateIfQuestionIdsAreSame(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        lenient().when(assessUtilServ.readAssessmentLevelData(any(), any()))
                .thenAnswer(inv -> realUtil.readAssessmentLevelData(inv.getArgument(0), inv.getArgument(1)));
        lenient().doAnswer(inv -> {
            realUtil.populateAssessmentFinalResults(inv.getArgument(0), inv.getArgument(1));
            return null;
        }).when(assessUtilServ).populateAssessmentFinalResults(any(), any());
    }


    @Test
    void testReadAssessment_NullUserId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        SBApiResponse response = service.readAssessment("id", "token", false, null);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testReadQuestionList_InvalidUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        Map<String, Object> requestBody = new HashMap<>();
        SBApiResponse response = service.readQuestionList(requestBody, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessmentResultV5_UserNotFound() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        Map<String, Object> request = new HashMap<>();
        SBApiResponse response = service.readAssessmentResultV5(request, "token");
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_InvalidUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn(null);
        Map<String, Object> submitRequest = new HashMap<>();
        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testSaveAssessmentAsync_UserNotFound() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        Map<String, Object> submitRequest = new HashMap<>();
        SBApiResponse response = service.saveAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessmentSavePoint_UserNotFound() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        SBApiResponse response = service.readAssessmentSavePoint("id", "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testAutoPublish_BlankAssessmentId() {
        SBApiResponse response = service.autoPublish("", "token");
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_ASSESSMENT_ID, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessment_UserIdBlank() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        SBApiResponse response = service.readAssessment("id", "token", false, "ctx");
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testReadQuestionList_ErrorMessage() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        Map<String, Object> requestBody = new HashMap<>();
        SBApiResponse response = service.readQuestionList(requestBody, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testSaveAssessmentAsync_UserIdBlank() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        SBApiResponse response = service.saveAssessmentAsync(new HashMap<>(), "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testAutoPublish_AssessmentIdentifierBlank() {
        SBApiResponse response = service.autoPublish("", "token");
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void testSubmitAssessmentAsync_UserIdEmpty() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn(null);
        SBApiResponse response = service.submitAssessmentAsync(new HashMap<>(), "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testSubmitAssessmentAsync_SuccessPracticeAssessment() throws IOException {
        // Mock user token
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");

        // Prepare submit request
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.COURSE_ID, "course123");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());

        // Prepare assessment hierarchy
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        assessmentHierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF); // Required
        assessmentHierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, "defaultType");

        // Course content response
        Map<String, Object> courseMap = new HashMap<>();
        courseMap.put(Constants.COURSE_CATEGORY, "Practice");

        // Mocks
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);

        when(assessUtilServ.readAssessmentRecord("assessmentId", List.of(Constants.LANGUAGE)))
                .thenReturn("english");

        when(assessUtilServ.readContentRecord(eq("course123"), anyList()))
                .thenReturn("course123-baseLang");

        when(contentService.readContent("course123-baseLang"))
                .thenReturn(courseMap);

        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.emptyList());

        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(new HashMap<>());

        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap()))
                .thenReturn(new HashMap<>());

        when(contentService.updateContentProgress(anyString(), anyMap(), anyString(), any()))
                .thenReturn(Constants.SUCCESS);

        // Execute method under test
        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);

        // Verify
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }


    @Test
    void testSubmitAssessmentAsync_InvalidSectionData() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "OtherCategory");
        assessmentHierarchy.put(Constants.CHILDREN, null); // Simulate missing children
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);
        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testSubmitAssessmentAsync_ExceptionHandling() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenThrow(new RuntimeException("Simulated error"));
        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Failed to process assessment submit request"));
    }

    @Test
    void testSubmitAssessmentAsync_NonPracticeAssessment_UserDataMissing(){
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "NonPractice");
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 1);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);
        // Simulate expired submission by mocking validateSubmitAssessmentRequest to return expired
        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_NonPracticeAssessment_AssessmentExpired() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");

        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());

        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "NonPractice");
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 1); // 1 second

        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);

        Map<String, Object> userAssessment = new HashMap<>();
        userAssessment.put(Constants.START_TIME, Instant.now().minusSeconds(120).toString()); // 2 mins ago
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(userAssessment));

        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Failed to process assessment submit request"));
    }


    @Test
    void testValidateSubmitAssessmentRequest_Reflection() throws Exception {
        // Prepare arguments as per the method signature
        Map<String, Object> submitRequest = new HashMap<>();
        String userId = "user";
        String assessmentId = "assessmentId";
        boolean isRetake = false;

        // Get the private method
        Class<?> submitDataClass = Class.forName(
                "com.igot.cb.assessment.service.AssessmentServiceV5Impl$SubmitAssessmentData");
        Constructor<?> submitDataCtor = submitDataClass.getDeclaredConstructor();
        submitDataCtor.setAccessible(true);
        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod(
                "validateSubmitAssessmentRequest",
                Map.class, String.class, submitDataClass, String.class, boolean.class
        );
        method.setAccessible(true);

        // Invoke the method
        Object result = method.invoke(service, submitRequest, userId, submitDataCtor.newInstance(), assessmentId, isRetake);

        // Assert result as needed
        assertNotNull(result);
    }

    @Test
    void testSubmitAssessmentAsync_FailedNonPracticeAssessment() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Competency Assessment");
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        assessmentHierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 2);
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, "defaultType");
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 60);
        List<Map<String, Object>> userAssessmentList = new ArrayList<>();
        Map<String, Object> userAssessment = new HashMap<>();
        userAssessment.put(Constants.START_TIME, java.time.Instant.now().toString()); // or new Date(), or Instant
        userAssessmentList.add(userAssessment);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(userAssessmentList);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);
        when(assessUtilServ.readAssessmentRecord("assessmentId", List.of(Constants.LANGUAGE)))
                .thenReturn("english");
        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testSubmitAssessmentAsync_SuccessWithDifferentAssessmentType() throws IOException {
        // Mock token -> user
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");

        // Prepare request
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.COURSE_ID, "course123");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());

        // Prepare assessment hierarchy
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        assessmentHierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 5);
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, "questionWeightage");
        assessmentHierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF); // Required

        // Course details
        Map<String, Object> courseMap = new HashMap<>();
        courseMap.put(Constants.COURSE_CATEGORY, "Practice");

        // Mocks
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);

        when(assessUtilServ.readAssessmentRecord("assessmentId", List.of(Constants.LANGUAGE)))
                .thenReturn("english");

        when(assessUtilServ.readContentRecord(eq("course123"), anyList()))
                .thenReturn("course123-baseLang");

        when(contentService.readContent("course123-baseLang"))
                .thenReturn(courseMap);

        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.emptyList());

        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(new HashMap<>());

        when(assessUtilServ.validateQumlAssessment(anyList(), anyList(), anyMap()))
                .thenReturn(new HashMap<>());

        when(contentService.updateContentProgress(anyString(), anyMap(), anyString(), any()))
                .thenReturn(Constants.SUCCESS);

        // Execute
        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);

        // Assert
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }


    @Test
    void testSubmitAssessmentAsyncV6_InvalidUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn(null);
        SBApiResponse response = service.submitAssessmentAsyncV6(new HashMap<>(), "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsyncV6_MissingAssessmentId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> submitRequest = new HashMap<>();
        SBApiResponse response = service.submitAssessmentAsyncV6(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.INVALID_ASSESSMENT_ID, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsyncV6_AssessmentHierarchyMissing() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Collections.emptyMap());
        SBApiResponse response = service.submitAssessmentAsyncV6(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }


    @Test
    void testSubmitAssessmentAsyncV6_SuccessPracticeAssessment() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        submitRequest.put(Constants.COURSE_ID, "courseId");
        when(contentService.readContent(any())).thenReturn(Collections.emptyMap());
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        assessmentHierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, "defaultType");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);
        when(assessUtilServ.readAssessmentRecord("assessmentId", List.of(Constants.LANGUAGE)))
                .thenReturn("english");
        SBApiResponse response = service.submitAssessmentAsyncV6(submitRequest, "token", false);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testSubmitAssessmentAsyncV6_ExceptionHandling() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenThrow(new RuntimeException("Simulated error"));
        SBApiResponse response = service.submitAssessmentAsyncV6(submitRequest, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Failed to process assessment submit request"));
    }

    @Test
    void testReadAssessment_HierarchyMissing() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Collections.emptyMap());
        SBApiResponse response = service.readAssessment("assess1", "token", false, null);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessment_PracticeAssessment() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        hierarchy.put(Constants.CHILDREN,  new ArrayList<>());
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        SBApiResponse response = service.readAssessment("assess1", "token", false, null);
        assertNotNull(response.getResult().get(Constants.QUESTION_SET));
    }

    @Test
    void testReadAssessment_FirstTimeRead_MissingDuration() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.emptyList());
        SBApiResponse response = service.readAssessment("assess1", "token", false, null);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_INVALID, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessment_FirstTimeRead_ContextLockError() {
        String assessmentIdentifier = "assess1";
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.EXPECTED_DURATION, 60);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.emptyList());
        when(assessUtilServ.validateContextLocking(anyMap(), any(), any(), anyString(), eq(assessmentIdentifier)))
                .thenReturn("LOCKED");
        SBApiResponse response = service.readAssessment(assessmentIdentifier, "token", false, null);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testReadAssessment_FirstTimeRead_DBUpdateFails() {
        String assessmentIdentifier = "assess1";
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.EXPECTED_DURATION, 60);
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(Collections.emptyList());
        when(assessUtilServ.validateContextLocking(anyMap(), any(), any(), anyString(), eq(assessmentIdentifier)))
                .thenReturn("");
        when(assessmentRepository.addUserAssesmentDataToDB(anyString(), anyString(), any(), any(), anyMap(), anyString()))
                .thenReturn(false);
        SBApiResponse response = service.readAssessment(assessmentIdentifier, "token", false, null);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessment_ExistingData_NotSubmitted() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.EXPECTED_DURATION, 60);
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().plusSeconds(60)));
        existing.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        existing.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"foo\":\"bar\"}");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(existing));
        SBApiResponse response = service.readAssessment("assess1", "token", false, null);
        assertNotNull(response.getResult().get(Constants.QUESTION_SET));
    }

    @Test
    void testReadAssessment_ExistingData_StartTimeNotUpdated() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.EXPECTED_DURATION, 60);
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 1);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().minusSeconds(60)));
        existing.put(Constants.STATUS, Constants.SUBMITTED);
        existing.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"foo\":\"bar\"}");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(existing));
        SBApiResponse response = service.readAssessment("assess1", "token", false, null);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessment_ExistingData_RetakeNormal() {
        String assessmentIdentifier = "assess1";
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.EXPECTED_DURATION, 60);
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 2);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().minusSeconds(60)));
        existing.put(Constants.STATUS, Constants.SUBMITTED);
        existing.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"foo\":\"bar\"}");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(existing));
        when(assessUtilServ.validateContextLocking(anyMap(), any(), any(), anyString(), eq(assessmentIdentifier)))
                .thenReturn("");
        when(assessmentRepository.addUserAssesmentDataToDB(anyString(), anyString(), any(), any(), anyMap(), anyString()))
                .thenReturn(true);
        SBApiResponse response = service.readAssessment(assessmentIdentifier, "token", false, null);
        assertNotNull(response.getResult().get(Constants.QUESTION_SET));
    }

    @Test
    void testSaveAssessmentAsync_Positive() {
        // Arrange
        String userId = "user1";
        String assessmentId = "assessment123";
        String token = "token";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, assessmentId);

        // Mock user ID
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);

        // Mock assessment details
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        assessmentAllDetail.put(Constants.PRIMARY_CATEGORY, "Assessment");
        when(assessUtilServ.readAssessmentHierarchyFromCache(assessmentId, false, token))
                .thenReturn(assessmentAllDetail);

        // Mock user assessment record
        Date startTime = new Date(System.currentTimeMillis() - 10000);
        Date endTime = new Date(System.currentTimeMillis() + 10000);
        Map<String, Object> userAssessment = new HashMap<>();
        userAssessment.put(Constants.START_TIME, startTime);
        userAssessment.put(Constants.END_TIME, endTime);
        userAssessment.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        userAssessment.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"foo\":\"bar\"}");
        List<Map<String, Object>> userAssessmentList = List.of(userAssessment);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(userAssessmentList);

        // Mock DB update
        when(assessmentRepository.updateUserAssesmentDataToDB(
                eq(userId), eq(assessmentId), any(), any(), any(), any(), eq(submitRequest)))
                .thenReturn(true);

        // Act
        SBApiResponse response = service.saveAssessmentAsync(submitRequest, token, false);

        // Assert
        assertNotNull(response);
        assertTrue(response.getResult().containsKey(Constants.QUESTION_SET));
        assertEquals(true, response.getResult().get("ASSESSMENT_UPDATE"));
    }

    @Test
    void testSaveAssessmentAsync_AssessmentHierarchyMissing() {
        // Arrange
        String userId = "user1";
        String assessmentId = "assessment123";
        String token = "token";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, assessmentId);

        // Mock user ID
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);

        // Mock assessment hierarchy as empty
        when(assessUtilServ.readAssessmentHierarchyFromCache(assessmentId, false, token))
                .thenReturn(Collections.emptyMap());

        // Act
        SBApiResponse response = service.saveAssessmentAsync(submitRequest, token, false);

        // Assert
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testGetParamDetailsForQTypes_QuestionWeightage() throws Exception {
        // Prepare mocks and data
        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.TOTAL_MARKS, 100);

        // Mock section-level definition
        Map<String, Object> proficiencyMap = new HashMap<>();
        proficiencyMap.put("marksForQuestion", 5);
        Map<String, Map<String, Object>> sectionLevelDefinition = new HashMap<>();
        sectionLevelDefinition.put("proficiency1", proficiencyMap);
        hierarchySection.put(Constants.SECTION_LEVEL_DEFINITION, sectionLevelDefinition);

        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, Constants.QUESTION_WEIGHTAGE);
        assessmentHierarchy.put(Constants.MINIMUM_PASS_PERCENTAGE, 60);
        assessmentHierarchy.put(Constants.NEGATIVE_MARKING_PERCENTAGE, 10);

        String hierarchySectionId = "section1";

        // Use reflection to access the private method
        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod(
                "getParamDetailsForQTypes",
                Map.class, Map.class, String.class
        );
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(
                service, hierarchySection, assessmentHierarchy, hierarchySectionId
        );

        // Assertions
        assertEquals(Constants.QUESTION_WEIGHTAGE, result.get(Constants.ASSESSMENT_TYPE));
        assertEquals(60, result.get(Constants.MINIMUM_PASS_PERCENTAGE));
        assertEquals(100, result.get(Constants.TOTAL_MARKS));
        assertEquals(10, result.get(Constants.NEGATIVE_MARKING_PERCENTAGE));
        assertEquals(hierarchySectionId, result.get("hierarchySectionId"));
        assertTrue(result.containsKey(Constants.QUESTION_SECTION_SCHEME));
    }

    @Test
    void testAutoPublish_Success() {
        String assessmentId = "assess123";
        String token = "token";
        String userId = "user1";
        String rootOrgId = "org123";

        // Mock user ID
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);

        // Mock Cassandra response for rootOrgId
        Map<String, Object> orgMap = new HashMap<>();
        orgMap.put(Constants.ROOT_ORG_ID, rootOrgId);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                anyString(), anyString(), anyMap(), anyList()))
                .thenReturn(List.of(orgMap));

        // Mock fetchResultUsingPost response from publish method
        Map<String, Object> publishResult = new HashMap<>();
        publishResult.put(Constants.RESULT, Map.of("published", true));
        publishResult.put(Constants.RESPONSE_CODE, Constants.OK);
        when(outboundRequestHandlerService.fetchResultUsingPost(
                anyString(), any(), anyMap()))
                .thenReturn(publishResult);

        // Mock update org patch call
        Map<String, Object> updateOrgResponse = new HashMap<>();
        updateOrgResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        when(outboundRequestHandlerService.fetchResultUsingPatch(
                anyString(), anyMap(), anyMap()))
                .thenReturn(updateOrgResponse);

        // Mock server properties
        when(serverProperties.getSbUrl()).thenReturn("http://example.com/");
        when(serverProperties.getUpdateOrgPath()).thenReturn("updateOrg");
        when(serverProperties.getCqfAssessmentPostPublishTopic()).thenReturn("topic");

        // Act
        SBApiResponse response = service.autoPublish(assessmentId, token);

        // Assert
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertNotNull(response.getResult());
        verify(producer).push("topic", assessmentId);
    }


    @Test
    void testUserIdBlank() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        Map<String, Object> request = new HashMap<>();
        List<String> identifierList = new ArrayList<>();
        Map<String, String> result = assessUtilServ.validateQuestionListAPI(request, "token", identifierList, false, accessTokenValidator);
        assertEquals(Constants.USER_ID_DOESNT_EXIST, result.get(Constants.ERROR_MESSAGE));
    }

    @Test
    void testAssessmentIdBlank() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> request = new HashMap<>();
        List<String> identifierList = new ArrayList<>();
        Map<String, String> result = assessUtilServ.validateQuestionListAPI(request, "token", identifierList, false, accessTokenValidator);
        assertEquals(Constants.ASSESSMENT_ID_KEY_IS_NOT_PRESENT_IS_EMPTY, result.get(Constants.ERROR_MESSAGE));
    }

    @Test
    void testIdentifierListEmpty() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        // getQuestionIdList returns empty
        List<String> identifierList = new ArrayList<>();
        Map<String, String> result = assessUtilServ.validateQuestionListAPI(request, "token", identifierList, false, accessTokenValidator);
        assertEquals(Constants.IDENTIFIER_LIST_IS_EMPTY, result.get(Constants.ERROR_MESSAGE));
    }

    @Test
    void testCreateResponseMapWithProperStructure_WithResultMap() {
        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "section1");
        hierarchySection.put(Constants.OBJECT_TYPE, "Section");
        hierarchySection.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchySection.put(Constants.MINIMUM_PASS_PERCENTAGE, 60);
        hierarchySection.put(Constants.NAME, "Section Name");

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(Constants.RESULT, 75.0);
        resultMap.put(Constants.BLANK, 2);
        resultMap.put(Constants.CORRECT, 5);
        resultMap.put(Constants.INCORRECT, 3);
        resultMap.put(Constants.CHILDREN, List.of("q1", "q2"));
        resultMap.put(Constants.SECTION_RESULT, "PASS");
        resultMap.put(Constants.TOTAL_MARKS, 100);
        resultMap.put(Constants.SECTION_MARKS, 75);

        Map<String, Object> result = service.createResponseMapWithProperStructure(hierarchySection, resultMap, 60);

        assertEquals("section1", result.get(Constants.IDENTIFIER));
        assertEquals("Section", result.get(Constants.OBJECT_TYPE));
        assertEquals("Assessment", result.get(Constants.PRIMARY_CATEGORY));
        assertEquals(60, result.get(Constants.PASS_PERCENTAGE));
        assertEquals("Section Name", result.get(Constants.NAME));
        assertEquals(75.0, result.get(Constants.RESULT));
        assertEquals(2, result.get(Constants.BLANK));
        assertEquals(5, result.get(Constants.CORRECT));
        assertEquals(3, result.get(Constants.INCORRECT));
        assertEquals(List.of("q1", "q2"), result.get(Constants.CHILDREN));
        assertEquals("PASS", result.get(Constants.SECTION_RESULT));
        assertEquals(100, result.get(Constants.TOTAL_MARKS));
        assertEquals(75, result.get(Constants.SECTION_MARKS));
        assertEquals(true, result.get(Constants.PASS));
        assertEquals(75.0, result.get(Constants.OVERALL_RESULT));
    }

    @Test
    void testCreateResponseMapWithProperStructure_EmptyResultMap() {
        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "section2");
        hierarchySection.put(Constants.OBJECT_TYPE, "Section");
        hierarchySection.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchySection.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);
        hierarchySection.put(Constants.NAME, "Section 2");
        hierarchySection.put(Constants.CHILDREN, List.of("q1", "q2", "q3"));

        Map<String, Object> result = service.createResponseMapWithProperStructure(hierarchySection, null, 50);

        assertEquals("section2", result.get(Constants.IDENTIFIER));
        assertEquals("Section", result.get(Constants.OBJECT_TYPE));
        assertEquals("Assessment", result.get(Constants.PRIMARY_CATEGORY));
        assertEquals(50, result.get(Constants.PASS_PERCENTAGE));
        assertEquals("Section 2", result.get(Constants.NAME));
        assertEquals(0.0, result.get(Constants.RESULT));
        assertEquals(3, result.get(Constants.TOTAL));
        assertEquals(3, result.get(Constants.BLANK));
        assertEquals(0, result.get(Constants.CORRECT));
        assertEquals(0, result.get(Constants.INCORRECT));
        assertEquals(false, result.get(Constants.PASS));
        assertEquals(0.0, result.get(Constants.OVERALL_RESULT));
    }

    @Test
    void testRetakeAssessment_BlankUser() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        SBApiResponse response = service.retakeAssessment("assess1", "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testRetakeAssessment_AssessmentDetailMissing() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Collections.emptyMap());
        SBApiResponse response = service.retakeAssessment("assess1", "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testRetakeAssessment_PreEnrolledContextss() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.CONTEXT_CATEGORY_TAG, Constants.PRE_ENROLLED_ASSESSMENT_KEY);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentDetail);
        SBApiResponse response = service.retakeAssessment("assess1", "token", false);
        assertEquals(1, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void testRetakeAssessment_NormalRetake() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentDetail);

        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        SBApiResponse response = spyService.retakeAssessment("assess1", "token", false);
        assertEquals(3, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED)); // 0 submissions
    }

    @Test
    void testCalculateAssessmentFinalResults_Pass() throws Exception {
        Map<String, Object> assessmentLevelResult = new HashMap<>();
        assessmentLevelResult.put(Constants.RESULT, 85.0);
        assessmentLevelResult.put(Constants.TOTAL, 10);
        assessmentLevelResult.put(Constants.BLANK, 1);
        assessmentLevelResult.put(Constants.CORRECT, 8);
        assessmentLevelResult.put(Constants.PASS_PERCENTAGE, 60);
        assessmentLevelResult.put(Constants.INCORRECT, 1);
        assessmentLevelResult.put(Constants.NAME, "Final Assessment");

        Method method = service.getClass().getDeclaredMethod("calculateAssessmentFinalResults", Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(service, assessmentLevelResult);

        assertEquals(85.0, result.get(Constants.OVERALL_RESULT));
        assertEquals(10, result.get(Constants.TOTAL));
        assertEquals(1, result.get(Constants.BLANK));
        assertEquals(8, result.get(Constants.CORRECT));
        assertEquals(60, result.get(Constants.PASS_PERCENTAGE));
        assertEquals(1, result.get(Constants.INCORRECT));
        assertEquals("Final Assessment", result.get(Constants.NAME));
        assertEquals(true, result.get(Constants.PASS));
        assertNotNull(result.get(Constants.CHILDREN));
    }

    @Test
    void testCalculateAssessmentFinalResults_NullInput() throws Exception {
        Method method = service.getClass().getDeclaredMethod("calculateAssessmentFinalResults", Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(service, (Map) null);

        assertNotNull(result);
        // Should be empty or handle gracefully
    }

    @Test
    void testRetakeAssessment_SuccessWithPreEnrolledContext() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CONTEXT_CATEGORY_TAG, Constants.PRE_ENROLLED_ASSESSMENT_KEY);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        SBApiResponse response = service.retakeAssessment("assessmentId", "token", false);
        assertEquals(1, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void testRetakeAssessment_SuccessWithGeneralContext() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);

        Map<String, Object> submission1 = Map.of(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "response1");
        Map<String, Object> submission2 = Map.of(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "response2");
        Map<String, Object> submission3 = Map.of(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "response3");

        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(submission1, submission2, submission3));

        SBApiResponse response = service.retakeAssessment("assessmentId", "token", false);

        assertEquals(3, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(3, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED)); // 3 submissions
    }


    @Test
    void testRetakeAssessment_Failure_BlankUserId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        SBApiResponse response = service.retakeAssessment("id", "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testRetakeAssessment_Failure_HierarchyNull() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Collections.emptyMap());
        SBApiResponse response = service.retakeAssessment("id", "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
    }

    // Additional test cases to cover all missed lines for 100% coverage

    @Test
    void testRetakeAssessment_PreEnrolledContexts() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> assessmentDetails = new HashMap<>();
        assessmentDetails.put(Constants.CONTEXT_CATEGORY_TAG, Constants.PRE_ENROLLED_ASSESSMENT_KEY);

        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentDetails);

        SBApiResponse response = service.retakeAssessment("id", "token", false);
        assertEquals(1, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void testRetakeAssessment_ExceptionHandlings() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenThrow(new RuntimeException("mocked exception"));

        SBApiResponse response = service.retakeAssessment("id", "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Error while calculating retake assessment"));
    }

    @Test
    void testReadAssessment_EditModeTrue_Practices() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        when(assessUtilServ.fetchHierarchyFromAssessServc(anyString(), anyString())).thenReturn(hierarchy);
        SBApiResponse response = service.readAssessment("id", "token", true, "ctx");
        assertNotNull(response.getResult().get(Constants.QUESTION_SET));
    }

    @Test
    void testReadAssessment_ValidationErrorAfterException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenThrow(new RuntimeException("Simulated error"));
        SBApiResponse response = service.readAssessment("id", "token", false, "ctx");
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Error while reading assessment"));
    }

    // Additional test cases to cover all missed lines for 100% coverage

    @Test
    void testReadAssessment_RetakeLimitExceeded() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.EXPECTED_DURATION, 60);
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 1);

        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().minusSeconds(60)));
        existing.put(Constants.STATUS, Constants.SUBMITTED);
        existing.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"foo\":\"bar\"}");
        existing.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "some response");

        // Return two attempts, 2 >= 1 → Should trigger retry limit block
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(existing, existing));

        SBApiResponse response = service.readAssessment("id", "token", false, "ctx");

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_RETRY_ATTEMPTS_CROSSED, response.getParams().getErrmsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPrivate_validateQuestionListRequest_invalidAssessmentId() throws Exception {
        List<String> idList = new ArrayList<>();
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, Collections.singletonMap(Constants.SEARCH, Collections.singletonMap(Constants.IDENTIFIER, List.of("q1"))));

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, String> result = assessUtilServ.validateQuestionListAPI(request, "token", idList, false, accessTokenValidator);
        assertTrue(result.containsKey(Constants.ERROR_MESSAGE));
    }

    @Test
    void testPrivate_createResponseMapWithProperStructure_NullResultMap() {
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "sec1");
        section.put(Constants.OBJECT_TYPE, "obj");
        section.put(Constants.PRIMARY_CATEGORY, "cat");
        section.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);
        section.put(Constants.NAME, "Section 1");
        section.put(Constants.CHILDREN, List.of("q1", "q2"));

        Map<String, Object> result = service.createResponseMapWithProperStructure(section, null, 50);
        assertEquals(0.0, result.get(Constants.RESULT));
        assertEquals(2, result.get(Constants.BLANK));
        assertEquals(false, result.get(Constants.PASS));  // Fixed here
    }


    @Test
    @SuppressWarnings("unchecked")
    void testPrivate_calculateAssessmentFinalResults_ValidData() throws Exception {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.RESULT, 85.0);
        input.put(Constants.TOTAL, 5);
        input.put(Constants.CORRECT, 4);
        input.put(Constants.INCORRECT, 1);
        input.put(Constants.BLANK, 0);
        input.put(Constants.NAME, "Assessment");
        input.put(Constants.PASS_PERCENTAGE, 70);

        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod("calculateAssessmentFinalResults", Map.class);
        method.setAccessible(true);
        Map<String, Object> output = (Map<String, Object>) method.invoke(service, input);
        assertEquals(true, output.get(Constants.PASS));
        assertEquals(85.0, output.get(Constants.OVERALL_RESULT));
    }

    @Test
    void testRetakeAssessment_SuccessPath() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);

        Map<String, Object> submission1 = Map.of(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "r1");
        Map<String, Object> submission2 = Map.of(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "r2");

        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(submission1, submission2));

        SBApiResponse response = service.retakeAssessment("assessmentId", "token", false);
        assertEquals(3, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(2, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED)); // 2 submissions
    }

    @Test
    void testReadAssessment_RetryLimitExceeded() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        hierarchy.put(Constants.EXPECTED_DURATION, 60);
        hierarchy.put(Constants.CHILDREN, new ArrayList<>());
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 1);

        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.END_TIME, Date.from(Instant.now().minusSeconds(60)));
        existing.put(Constants.STATUS, Constants.SUBMITTED);
        existing.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"foo\":\"bar\"}");
        existing.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "response");

        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString())).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString())).thenReturn(List.of(existing, existing));

        SBApiResponse response = service.readAssessment("id", "token", false, "ctx");
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_RETRY_ATTEMPTS_CROSSED, response.getParams().getErrmsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPrivate_calculateAssessmentFinalResults() throws Exception {
        Map<String, Object> input = new HashMap<>();
        input.put(Constants.RESULT, 90.0);
        input.put(Constants.TOTAL, 10);
        input.put(Constants.CORRECT, 9);
        input.put(Constants.INCORRECT, 1);
        input.put(Constants.BLANK, 0);
        input.put(Constants.NAME, "Test Section");
        input.put(Constants.PASS_PERCENTAGE, 70);

        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod("calculateAssessmentFinalResults", Map.class);
        method.setAccessible(true);

        Map<String, Object> result = (Map<String, Object>) method.invoke(service, input);
        assertEquals(true, result.get(Constants.PASS));
        assertEquals(90.0, result.get(Constants.OVERALL_RESULT));
        assertEquals("Test Section", result.get(Constants.NAME));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPrivate_validateQuestionListAPI_invalidAssessmentId() throws Exception {
        List<String> idList = new ArrayList<>();
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST,
                Map.of(Constants.SEARCH, Map.of(Constants.IDENTIFIER, List.of("q1", "q2"))));

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        Map<String, String> result = assessUtilServ.validateQuestionListAPI(request, "token", idList, false, accessTokenValidator);
        assertTrue(result.containsKey(Constants.ERROR_MESSAGE));
    }

    @Test
    void testReadQuestionList_ValidationError() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, "assess123");
        request.put(Constants.PRIMARY_CATEGORY, "Assessment");
        request.put(Constants.REQUEST, Map.of(Constants.SEARCH, Map.of(Constants.IDENTIFIER, List.of("wrongId"))));

        // Force validation error by skipping access token
        SBApiResponse response = service.readQuestionList(request, null, false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("User Id doesn't exist! Please supply a valid auth token", response.getParams().getErrmsg());
    }

    @Test
    void testReadQuestionList_ValidationError_AssessmentIdMismatch() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, "id123");
        request.put(Constants.REQUEST, Map.of(Constants.SEARCH, Map.of(Constants.IDENTIFIER, List.of("q123"))));

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");

        SBApiResponse response = service.readQuestionList(request, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    void testReadQuestionList_ExceptionThrown() {
        String matchingId = "q1";
        Map<String, Object> search = Map.of(Constants.IDENTIFIER, List.of(matchingId));
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, matchingId);
        request.put(Constants.REQUEST, Map.of(Constants.SEARCH, search));

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenThrow(new RuntimeException("Simulated failure"));

        SBApiResponse response = service.readQuestionList(request, "token", false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Failed to fetch the question list"));
    }

    @Test
    void testReadQuestionList_Success() throws Exception {
        String matchingId = "q1";

        Map<String, Object> search = Map.of(Constants.IDENTIFIER, List.of(matchingId));
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, matchingId);
        request.put(Constants.PRIMARY_CATEGORY, "Assessment");
        request.put(Constants.REQUEST, Map.of(Constants.SEARCH, search));

        // This is what mapper.readValue() expects
        String assessmentJson = "{ \"primaryCategory\": \"Assessment\", \"children\": [{ \"childNodes\": [\"q1\"] }] }";

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of(Constants.PRIMARY_CATEGORY, "Assessment"));
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(Map.of(Constants.ASSESSMENT_READ_RESPONSE_KEY, assessmentJson)));

        Map<String, Object> q1Map = Map.of(Constants.IDENTIFIER, matchingId);
        Map<String, Object> questionsMap = Map.of(matchingId, q1Map);

        when(assessUtilServ.readQListfromCache(anyList(), eq(matchingId), eq(false), anyString()))
                .thenReturn(questionsMap);
        when(assessUtilServ.filterQuestionMapDetailV2(any(), anyString(), anyBoolean())).thenReturn(q1Map);

        SBApiResponse response = service.readQuestionList(request, "token", false);
        List<?> questions = (List<?>) response.getResult().get(Constants.QUESTIONS);

        assertNotNull(questions, "Questions should not be null");
        assertEquals(1, questions.size());
        assertEquals("q1", ((Map<?, ?>) questions.get(0)).get(Constants.IDENTIFIER));
    }

    @Test
    void testReadAssessmentResultV5_FullCoverage() {
        String token = "validToken";
        String userId = "user123";
        String assessmentId = "assess123";
        String responseJson = "{\"score\": 85}";

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, assessmentId);
        requestBody.put(Constants.BATCH_ID, "batch1");
        requestBody.put(Constants.COURSE_ID, "course1");

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, requestBody);

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);

        // ========== 1. Test private validation failure ==========
        // Inject bad request (missing assessmentId) to fail validation
        Map<String, Object> badRequest = new HashMap<>();
        badRequest.put(Constants.REQUEST, new HashMap<>());

        SBApiResponse invalidResp = service.readAssessmentResultV5(badRequest, token);
        assertEquals(Constants.FAILED, invalidResp.getParams().getStatus());


        // ========== 2. Empty user assessment data ==========
        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(Collections.emptyList());

        SBApiResponse emptyListResp = service.readAssessmentResultV5(request, token);
        assertEquals(Constants.FAILED, emptyListResp.getParams().getStatus());
        assertEquals(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, emptyListResp.getParams().getErrmsg());

        // ========== 3. Assessment In-Progress ==========
        Map<String, Object> inProgress = new HashMap<>();
        inProgress.put(Constants.STATUS, "IN_PROGRESS");

        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(List.of(inProgress));

        SBApiResponse progressResp = service.readAssessmentResultV5(request, token);
        assertTrue((Boolean) progressResp.getResult().get(Constants.STATUS_IS_IN_PROGRESS));

        // ========== 4. Valid Submitted Response ==========
        Map<String, Object> submitted = new HashMap<>();
        submitted.put(Constants.STATUS, Constants.SUBMITTED);
        submitted.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, responseJson);

        when(assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentId)).thenReturn(List.of(submitted));

        SBApiResponse finalResponse = service.readAssessmentResultV5(request, token);
        assertEquals(Constants.SUCCESS, finalResponse.getParams().getStatus());
        assertEquals(85, finalResponse.getResult().get("score"));
    }

    @Test
    void testSubmitAssessmentAsync_PreEnrolledAssessment() throws Exception {
        // Setup request with valid CHILDREN
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, "q1");

        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "sec1");
        section.put(Constants.CHILDREN, List.of(question)); // Mandatory

        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess123");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.COURSE_ID, "course123");
        submitRequest.put(Constants.CHILDREN, List.of(section)); // For validation

        // Spy the service
        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        // Mock dependencies
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user123");
        when(contentService.readContent(anyString())).thenReturn(Map.of(Constants.COURSE_CATEGORY, "SomeCategory"));
        when(assessUtilServ.readQListfromCache(any(), any(), anyBoolean(), anyString())).thenReturn(Map.of("sec1", List.of(question)));
        when(assessUtilServ.validateQumlAssessmentV2(any(), any(), any(), any())).thenReturn(Map.of());
        when(assessUtilServ.readAssessmentRecord(eq("assess123"), anyList())).thenReturn("english");
        doReturn(Map.of()).when(spyService).createResponseMapWithProperStructure(any(), any(), any());

        // Build hierarchy manually (instead of mocking nonexistent method)
        List<Map<String, Object>> hierarchySectionList = new ArrayList<>();
        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "sec1");
        hierarchySection.put(Constants.CHILDREN, List.of(question));
        hierarchySectionList.add(hierarchySection);

        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CONTEXT_CATEGORY_TAG, Constants.PRE_ENROLLED_ASSESSMENT_KEY);
        hierarchy.put(Constants.ASSESSMENT_TYPE, "default");
        hierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        hierarchy.put(Constants.CHILDREN, hierarchySectionList); // ✅ must match structure
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(hierarchy);
        Map<String, Object> existingData = new HashMap<>();
        existingData.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, "");

        // Use reflection to call the private method
        Class<?> submitDataClass = Class.forName(
                "com.igot.cb.assessment.service.AssessmentServiceV5Impl$SubmitAssessmentData");
        Constructor<?> submitDataCtor = submitDataClass.getDeclaredConstructor();
        submitDataCtor.setAccessible(true);
        Method validateMethod = AssessmentServiceV5Impl.class.getDeclaredMethod(
                "validateSubmitAssessmentRequest",
                Map.class, String.class, submitDataClass, String.class, boolean.class
        );
        validateMethod.setAccessible(true);
        validateMethod.invoke(
                spyService,
                submitRequest, "user123", submitDataCtor.newInstance(), "token", false
        );

        // Act
        SBApiResponse response = spyService.submitAssessmentAsync(submitRequest, "token", false);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testSubmitAssessmentAsyncV6_CoversSectionScoreCutoffBlock() throws Exception {
        // Prepare request
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess123");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.COURSE_ID, "course123");

        // Mocks
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("900");

        // Assessment hierarchy setup
        Map<String, Object> question = Map.of(Constants.IDENTIFIER, "q1");
        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "sec1");
        hierarchySection.put(Constants.CHILDREN, List.of(question));

        Map<String, Map<String, Object>> sectionLevelDefinition = new HashMap<>();
        Map<String, Object> markingScheme = new HashMap<>();
        markingScheme.put("marksForQuestion", 5);
        sectionLevelDefinition.put("sec1", markingScheme);
        hierarchySection.put(Constants.SECTION_LEVEL_DEFINITION, sectionLevelDefinition);

        List<Map<String, Object>> hierarchySectionList = List.of(hierarchySection);
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.CONTEXT_CATEGORY_TAG, "other");
        assessmentHierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 2);
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, Constants.QUESTION_WEIGHTAGE);
        assessmentHierarchy.put(Constants.CHILDREN, hierarchySectionList);
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 10);
        when(assessUtilServ.readAssessmentRecord(any(), eq(List.of(Constants.LANGUAGE))))
                .thenReturn("english");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);
        when(contentService.readContent(anyString())).thenReturn(Map.of(Constants.COURSE_CATEGORY, "Science"));
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of("q1", question));
        when(assessUtilServ.validateQumlAssessmentV3(any(), any(), any(), any())).thenReturn(Map.of());
        when(assessUtilServ.parseStartTimeToLong(any())).thenReturn(123456789L);
        String questionSetMockJson = """
                {
                  "children": [
                    {
                      "identifier": "sec1",
                      "childNodes": ["q1"]
                    }
                  ]
                }
                """;

        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(Map.of(
                        Constants.STATUS, "started",
                        Constants.START_TIME, Instant.now().minus(Duration.ofMinutes(2)).toString(),
                        Constants.RETAKE_ATTEMPTS_CONSUMED, 0,
                        Constants.ASSESSMENT_READ_RESPONSE_KEY, questionSetMockJson
                )));

        // Section from submit request
        Map<String, Object> submitSection = new HashMap<>();
        submitSection.put(Constants.IDENTIFIER, "sec1");
        submitSection.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        submitRequest.put(Constants.CHILDREN, List.of(submitSection));

        // Spy the service
        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        // --- Manually call validateSubmitAssessmentRequest via Reflection ---
        Class<?> submitDataClass = Class.forName(
                "com.igot.cb.assessment.service.AssessmentServiceV5Impl$SubmitAssessmentData");
        Constructor<?> submitDataCtor = submitDataClass.getDeclaredConstructor();
        submitDataCtor.setAccessible(true);
        Method validateMethod = AssessmentServiceV5Impl.class.getDeclaredMethod(
                "validateSubmitAssessmentRequest",
                Map.class, String.class, submitDataClass, String.class, boolean.class
        );
        validateMethod.setAccessible(true);
        List<Map<String, Object>> hierarchySections = new ArrayList<>();
        List<Map<String, Object>> submitSections = new ArrayList<>();
        Map<String, Object> hierarchy = new HashMap<>();

        hierarchySections.add(hierarchySection);
        hierarchy.put(Constants.CHILDREN, hierarchySections);
        submitSections.add(submitSection);

        validateMethod.invoke(spyService, submitRequest, "user1", submitDataCtor.newInstance(), "token", false);

        // Stub response creator (if required internally)
        doReturn(Map.of()).when(spyService).createResponseMapWithProperStructure(any(), any(), any());

        // --- Act ---
        SBApiResponse response = spyService.submitAssessmentAsyncV6(submitRequest, "token", false);

        // --- Assert ---
        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals("Assessment", response.getResult().get(Constants.PRIMARY_CATEGORY));
    }

    @Test
    void testSubmitAssessmentAsyncV6_CoversPracticeQuestionSetBlock() throws Exception {
        // Prepare request
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess123");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.COURSE_ID, "course123");

        // Mocks
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("900");

        // Assessment hierarchy setup
        Map<String, Object> question = Map.of(Constants.IDENTIFIER, "q1");
        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "sec1");
        hierarchySection.put(Constants.CHILDREN, List.of(question));

        Map<String, Map<String, Object>> sectionLevelDefinition = new HashMap<>();
        Map<String, Object> markingScheme = new HashMap<>();
        markingScheme.put("marksForQuestion", 5);
        sectionLevelDefinition.put("sec1", markingScheme);
        hierarchySection.put(Constants.SECTION_LEVEL_DEFINITION, sectionLevelDefinition);

        List<Map<String, Object>> hierarchySectionList = List.of(hierarchySection);
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.CONTEXT_CATEGORY_TAG, "other");
        assessmentHierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 2);
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, Constants.QUESTION_WEIGHTAGE);
        assessmentHierarchy.put(Constants.CHILDREN, hierarchySectionList);
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 10);
        when(assessUtilServ.readAssessmentRecord(any(), eq(List.of(Constants.LANGUAGE))))
                .thenReturn("english");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(assessmentHierarchy);
        when(contentService.readContent(anyString())).thenReturn(Map.of(Constants.COURSE_CATEGORY, "Science"));
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of("q1", question));
        when(assessUtilServ.validateQumlAssessmentV3(any(), any(), any(), any())).thenReturn(Map.of());
        when(assessUtilServ.parseStartTimeToLong(any())).thenReturn(123456789L);
        String questionSetMockJson = """
                {
                  "children": [
                    {
                      "identifier": "sec1",
                      "childNodes": ["q1"]
                    }
                  ]
                }
                """;

        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(List.of(Map.of(
                        Constants.STATUS, "started",
                        Constants.START_TIME, Instant.now().minus(Duration.ofMinutes(2)).toString(),
                        Constants.RETAKE_ATTEMPTS_CONSUMED, 0,
                        Constants.ASSESSMENT_READ_RESPONSE_KEY, questionSetMockJson
                )));

        // Section from submit request
        Map<String, Object> submitSection = new HashMap<>();
        submitSection.put(Constants.IDENTIFIER, "sec1");
        submitSection.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        submitRequest.put(Constants.CHILDREN, List.of(submitSection));

        // Spy the service
        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        // --- Manually call validateSubmitAssessmentRequest via Reflection ---
        Class<?> submitDataClass = Class.forName(
                "com.igot.cb.assessment.service.AssessmentServiceV5Impl$SubmitAssessmentData");
        Constructor<?> submitDataCtor = submitDataClass.getDeclaredConstructor();
        submitDataCtor.setAccessible(true);
        Method validateMethod = AssessmentServiceV5Impl.class.getDeclaredMethod(
                "validateSubmitAssessmentRequest",
                Map.class, String.class, submitDataClass, String.class, boolean.class
        );
        validateMethod.setAccessible(true);
        List<Map<String, Object>> hierarchySections = new ArrayList<>();
        List<Map<String, Object>> submitSections = new ArrayList<>();
        Map<String, Object> hierarchy = new HashMap<>();

        hierarchySections.add(hierarchySection);
        hierarchy.put(Constants.CHILDREN, hierarchySections);
        submitSections.add(submitSection);

        validateMethod.invoke(spyService, submitRequest, "user1", submitDataCtor.newInstance(), "token", false);

        // Stub response creator (if required internally)
        doReturn(Map.of()).when(spyService).createResponseMapWithProperStructure(any(), any(), any());

        // --- Act ---
        SBApiResponse response = spyService.submitAssessmentAsyncV6(submitRequest, "token", false);

        // --- Assert ---
        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(Constants.PRACTICE_QUESTION_SET, response.getResult().get(Constants.PRIMARY_CATEGORY));
    }

    @Test
    void testSubmitAssessmentAsyncV6_CoversAssessmentLevelScoreCutoff_WithPracticeCategory() throws Exception {
        // Setup request
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess123");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.COURSE_ID, "course123");

        Map<String, Object> submitSection = new HashMap<>();
        submitSection.put(Constants.IDENTIFIER, "sec1");
        submitSection.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        submitRequest.put(Constants.CHILDREN, List.of(submitSection));

        // Setup hierarchy
        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "sec1");
        hierarchySection.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        hierarchySection.put(Constants.SECTION_LEVEL_DEFINITION, Map.of("sec1", Map.of("marksForQuestion", 5)));
        hierarchySection.put(Constants.MINIMUM_PASS_PERCENTAGE, 2);

        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.CONTEXT_CATEGORY_TAG, "other");
        assessmentHierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 2);
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET); // For else-if block
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, "default"); // triggers ASSESSMENT_LEVEL_SCORE_CUTOFF
        assessmentHierarchy.put(Constants.CHILDREN, List.of(hierarchySection));
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 10);

        Map<String, Object> existingAssessmentData = Map.of(
                Constants.STATUS, "started",
                Constants.START_TIME, Instant.now().minus(Duration.ofMinutes(2)).toString(),
                Constants.RETAKE_ATTEMPTS_CONSUMED, 0,
                Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"children\": [{\"childNodes\": [\"q1\"]}]}"
        );

        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        // Mocks
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("900");
        when(assessUtilServ.readAssessmentHierarchyFromCache(any(), anyBoolean(), any())).thenReturn(assessmentHierarchy);
        when(contentService.readContent(anyString())).thenReturn(Map.of(Constants.COURSE_CATEGORY, "Science"));
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of("q1", Map.of(Constants.IDENTIFIER, "q1")));
        when(assessUtilServ.validateQumlAssessmentV3(any(), any(), any(), any()))
                .thenReturn(Map.of("q1", Map.of(Constants.IDENTIFIER, "q1")));
        when(assessUtilServ.parseStartTimeToLong(any())).thenReturn(123456789L);
        when(assessUtilServ.readAssessmentRecord(any(), anyList())).thenReturn("english");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(any(), any())).thenReturn(List.of(existingAssessmentData));
        when(contentService.updateContentProgress(anyString(), anyMap(), anyString(), any(SBApiResponse.class)))
                .thenReturn("FAILED"); // triggers log error for !SUCCESS

        when(assessUtilServ.validateQumlAssessmentV3(any(), any(), any(), any()))
                .thenReturn(Map.of(
                        Constants.RESULT, 1.0,
                        Constants.CORRECT, 1,
                        Constants.INCORRECT, 0,
                        Constants.BLANK, 0,
                        Constants.CHILDREN, new ArrayList<>(),
                        Constants.SECTION_RESULT, "pass",
                        Constants.TOTAL_MARKS, 10,
                        Constants.SECTION_MARKS, 5
                ));
        // Allow real method for createResponseMap
        doCallRealMethod().when(spyService).createResponseMapWithProperStructure(any(), any(), any());

        // Stub calculateAssessmentFinalResults using reflection
        Method calcMethod = AssessmentServiceV5Impl.class.getDeclaredMethod("calculateAssessmentFinalResults", Map.class);
        calcMethod.setAccessible(true);
        Map<String, Object> fakeFinalRes = new HashMap<>();
        fakeFinalRes.put("score", 5);
        fakeFinalRes.put(Constants.TOTAL_SCORE, 10);

        // Act
        SBApiResponse response = spyService.submitAssessmentAsyncV6(submitRequest, "token", false);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testSubmitAssessmentAsyncV6_CoversAssessmentLevelScoreCutoff_WithoutPracticeCategory() throws Exception {
        // Setup request
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess123");
        submitRequest.put(Constants.LANGUAGE, "english");
        submitRequest.put(Constants.COURSE_ID, "course123");

        Map<String, Object> submitSection = new HashMap<>();
        submitSection.put(Constants.IDENTIFIER, "sec1");
        submitSection.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        submitRequest.put(Constants.CHILDREN, List.of(submitSection));

        // Setup hierarchy
        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "sec1");
        hierarchySection.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        hierarchySection.put(Constants.SECTION_LEVEL_DEFINITION, Map.of("sec1", Map.of("marksForQuestion", 5)));
        hierarchySection.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);

        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.CONTEXT_CATEGORY_TAG, "other");
        assessmentHierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 2);
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Practice Question Set1"); // For else-if block
        assessmentHierarchy.put(Constants.ASSESSMENT_TYPE, "default"); // triggers ASSESSMENT_LEVEL_SCORE_CUTOFF
        assessmentHierarchy.put(Constants.CHILDREN, List.of(hierarchySection));
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 10);

        Map<String, Object> existingAssessmentData = Map.of(
                Constants.STATUS, "started",
                Constants.START_TIME, Instant.now().minus(Duration.ofMinutes(2)).toString(),
                Constants.RETAKE_ATTEMPTS_CONSUMED, 0,
                Constants.ASSESSMENT_READ_RESPONSE_KEY, "{\"children\": [{\"childNodes\": [\"q1\"]}]}"
        );

        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        // Mocks
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("900");
        when(assessUtilServ.readAssessmentHierarchyFromCache(any(), anyBoolean(), any())).thenReturn(assessmentHierarchy);
        when(contentService.readContent(anyString())).thenReturn(Map.of(Constants.COURSE_CATEGORY, "Science"));
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of("q1", Map.of(Constants.IDENTIFIER, "q1")));
        when(assessUtilServ.validateQumlAssessmentV3(any(), any(), any(), any()))
                .thenReturn(Map.of("q1", Map.of(Constants.IDENTIFIER, "q1")));
        when(assessUtilServ.parseStartTimeToLong(any())).thenReturn(123456789L);
        when(assessUtilServ.readAssessmentRecord(any(), anyList())).thenReturn("english");
        when(assessUtilServ.readUserSubmittedAssessmentRecords(any(), any())).thenReturn(List.of(existingAssessmentData));
        when(contentService.updateContentProgress(anyString(), anyMap(), anyString(), any(SBApiResponse.class)))
                .thenReturn("FAILED"); // triggers log error for !SUCCESS

        when(assessUtilServ.validateQumlAssessmentV3(any(), any(), any(), any()))
                .thenReturn(Map.of(
                        Constants.RESULT, 1.0,
                        Constants.CORRECT, 1,
                        Constants.INCORRECT, 0,
                        Constants.BLANK, 0,
                        Constants.CHILDREN, new ArrayList<>(),
                        Constants.SECTION_RESULT, "pass",
                        Constants.TOTAL_MARKS, 10,
                        Constants.SECTION_MARKS, 5
                ));
        // Allow real method for createResponseMap
        doCallRealMethod().when(spyService).createResponseMapWithProperStructure(any(), any(), any());

        // Stub calculateAssessmentFinalResults using reflection
        Method calcMethod = AssessmentServiceV5Impl.class.getDeclaredMethod("calculateAssessmentFinalResults", Map.class);
        calcMethod.setAccessible(true);
        Map<String, Object> fakeFinalRes = new HashMap<>();
        fakeFinalRes.put("score", 5);
        fakeFinalRes.put(Constants.TOTAL_SCORE, 10);

        // Act
        SBApiResponse response = spyService.submitAssessmentAsyncV6(submitRequest, "token", false);

        // Assert
        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testAutoPublish_InvalidUserToken() {
        // Arrange
        String token = "dummyToken";
        String assessmentId = "assessment123";

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn("");

        // Act
        SBApiResponse response = service.autoPublish(assessmentId, token);

        // Assert
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.INVALID_USER_TOKEN, response.getParams().getErrmsg());
    }

    @Test
    void testAutoPublish_FailedToPublish() {
        // Arrange
        String token = "dummyToken";
        String assessmentId = "assessment123";

        // Create spy
        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        // Mock userId fetch
        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn("user123");

        // Mock publish method on spy
        doReturn(Collections.emptyMap()).when(spyService).publish(assessmentId, token);

        // Act
        SBApiResponse response = spyService.autoPublish(assessmentId, token);

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.PUBLISH_QUESTION_SET_FAILED, response.getParams().getErrmsg());
    }


    @Test
    void testShuffleQuestions_ShouldReturnShuffledCopy() {
        // Arrange
        Map<String, Object> q1 = Map.of("id", "q1");
        Map<String, Object> q2 = Map.of("id", "q2");
        Map<String, Object> q3 = Map.of("id", "q3");
        List<Map<String, Object>> originalList = List.of(q1, q2, q3);

        // Act
        List<Map<String, Object>> shuffledList = AssessmentServiceV5Impl.shuffleQuestions(originalList);

        // Assert
        assertNotNull(shuffledList);
        assertEquals(3, shuffledList.size());
        assertTrue(shuffledList.containsAll(originalList), "Shuffled list should contain all original elements");
        assertNotSame(originalList, shuffledList, "Returned list should be a new copy");

        // Optional: check if the list is actually shuffled (non-deterministic)
        // To avoid flaky tests, don't assert on order unless mocking randomness
    }

    @Test
    void testProcessRandomizationForQuestions_WithLimit() throws Exception {
        // Given
        Map<String, Map<String, Object>> sectionLevelDefinitionMap = new HashMap<>();
        Map<String, Object> easyLevelConfig = new HashMap<>();
        easyLevelConfig.put(Constants.NO_OF_QUESTIONS, 2);  // Set a limit
        sectionLevelDefinitionMap.put("easy", easyLevelConfig);

        // Questions with level "easy"
        List<Map<String, Object>> questions = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            Map<String, Object> q = new HashMap<>();
            q.put(Constants.IDENTIFIER, "q" + i);
            q.put(Constants.QUESTION_LEVEL, "easy");
            questions.add(q);
        }

        // Spy to access private method
        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        // Reflect to invoke private method
        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod("processRandomizationForQuestions",
                Map.class, List.class);
        method.setAccessible(true);

        // When
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(spyService, sectionLevelDefinitionMap, questions);

        // Then
        assertNotNull(result);
        assertEquals(2, result.size());  // Limited by NO_OF_QUESTIONS
        for (Map<String, Object> q : result) {
            assertEquals("easy", q.get(Constants.QUESTION_LEVEL));
        }
    }

    @Test
    void testProcessRandomizationForQuestions_NoLimitReturnsOriginal() throws Exception {
        // Given
        Map<String, Map<String, Object>> sectionLevelDefinitionMap = new HashMap<>();
        Map<String, Object> emptyConfig = new HashMap<>();
        sectionLevelDefinitionMap.put("easy", emptyConfig); // No NO_OF_QUESTIONS key

        List<Map<String, Object>> questions = new ArrayList<>();
        Map<String, Object> q = new HashMap<>();
        q.put(Constants.IDENTIFIER, "q1");
        q.put(Constants.QUESTION_LEVEL, "easy");
        questions.add(q);

        // Spy
        AssessmentServiceV5Impl spyService = Mockito.spy(service);

        // Reflect to invoke private method
        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod("processRandomizationForQuestions",
                Map.class, List.class);
        method.setAccessible(true);

        // When
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(spyService, sectionLevelDefinitionMap, questions);

        // Then
        assertEquals(1, result.size());
        assertEquals("q1", result.get(0).get(Constants.IDENTIFIER));
    }

    @Test
    void testReadAssessmentSavePoint_EditMode_FetchEmptyAssessment() {
        when(accessTokenValidator.fetchUserIdFromAccessToken("token")).thenReturn("user1");
        when(assessUtilServ.fetchHierarchyFromAssessServc("assessment123", "token")).thenReturn(Collections.emptyMap());

        SBApiResponse response = service.readAssessmentSavePoint("assessment123", "token", true);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
    }


    @Test
    void testReadAssessmentSavePoint_NoExistingUserData() {
        Map<String, Object> assessmentHierarchy = Map.of(Constants.PRIMARY_CATEGORY, "other");

        when(accessTokenValidator.fetchUserIdFromAccessToken("token")).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache("assessment123", false, "token")).thenReturn(assessmentHierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assessment123")).thenReturn(Collections.emptyList());

        SBApiResponse response = service.readAssessmentSavePoint("assessment123", "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_SAVE_NOT_AVBL, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessmentSavePoint_ExistingData_ButInvalidForSavePoint() {
        Map<String, Object> assessmentHierarchy = Map.of(Constants.PRIMARY_CATEGORY, "other");

        Date futureDate = new Date(System.currentTimeMillis() + 10 * 60 * 1000); // 10 mins later
        Map<String, Object> existingData = new HashMap<>();
        existingData.put(Constants.END_TIME, futureDate);
        existingData.put(Constants.STATUS, Constants.NOT_SUBMITTED);

        when(accessTokenValidator.fetchUserIdFromAccessToken("token")).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache("assessment123", false, "token")).thenReturn(assessmentHierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assessment123")).thenReturn(List.of(existingData));

        SBApiResponse response = service.readAssessmentSavePoint("assessment123", "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_SAVE_NOT_AVBL, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessmentSavePoint_ValidSavePointFound() {
        Map<String, Object> assessmentHierarchy = Map.of(Constants.PRIMARY_CATEGORY, "other");

        Date pastDate = new Date(System.currentTimeMillis() - 10 * 60 * 1000); // 10 mins ago
        Map<String, Object> existingData = new HashMap<>();
        existingData.put(Constants.END_TIME, pastDate);
        existingData.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        existingData.put(Constants.ASSESSMENT_SAVE_READ_RESPONSE_KEY, "{\"q1\":\"QuestionData\"}");

        when(accessTokenValidator.fetchUserIdFromAccessToken("token")).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache("assessment123", false, "token")).thenReturn(assessmentHierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assessment123")).thenReturn(List.of(existingData));

        SBApiResponse response = service.readAssessmentSavePoint("assessment123", "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertTrue(response.getResult().containsKey(Constants.QUESTION_SET));
    }

    @Test
    void testValidateAssessmentReadResult_MissingAssessmentId() {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.COURSE_ID, "course123");

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, requestBody);

        String result = assessUtilServ.validateAssessmentReadResult(request);
        assertTrue(result.contains(Constants.ASSESSMENT_ID_KEY));
        assertFalse(result.contains(Constants.COURSE_ID));
    }

    @Test
    void testValidateAssessmentReadResult_AllFieldsPresent() {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess123");
        requestBody.put(Constants.BATCH_ID, "batch456");
        requestBody.put(Constants.COURSE_ID, "course789");

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, requestBody);

        String result = assessUtilServ.validateAssessmentReadResult(request);
        assertEquals("", result);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCalculateSectionFinalResults_WithValidData() throws Exception {
        // Arrange
        Map<String, Object> section1 = new HashMap<>();
        section1.put(Constants.RESULT, 80.0); // Will be added to totalResult
        section1.put(Constants.BLANK, 1);
        section1.put(Constants.CORRECT, 4);
        section1.put(Constants.INCORRECT, 1);
        section1.put(Constants.PASS_PERCENTAGE, 60); // result >= pass percentage (pass++)
        section1.put(Constants.SECTION_MARKS, 40.0); // totalSectionMarks += 40.0
        section1.put(Constants.TOTAL_MARKS, 50);     // totalMarks += 50

        Map<String, Object> section2 = new HashMap<>();
        section2.put(Constants.RESULT, 70.0);
        section2.put(Constants.BLANK, 0);
        section2.put(Constants.CORRECT, 3);
        section2.put(Constants.INCORRECT, 2);
        section2.put(Constants.PASS_PERCENTAGE, 60); // pass++
        section2.put(Constants.SECTION_MARKS, 30.0);
        section2.put(Constants.TOTAL_MARKS, 50);

        List<Map<String, Object>> sectionResults = Arrays.asList(section1, section2);
        long startTime = 1000L;
        long endTime = 5000L;

        // Use reflection if method is private
        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod(
                "calculateSectionFinalResults", List.class, long.class, long.class, int.class, int.class);
        method.setAccessible(true);

        // Act
        Map<String, Object> result = (Map<String, Object>) method.invoke(
                service, sectionResults, startTime, endTime, 3, 1);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.get(Constants.RETAKE_ATTEMPT_CONSUMED));
        assertEquals(3, result.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS));
        assertEquals(70.0, result.get(Constants.TOTAL_PERCENTAGE)); // (70/100) * 100
        assertEquals(true, result.get(Constants.PASS)); // should be true (both sections passed)
        assertEquals(1, result.get(Constants.BLANK)); // 1 + 0 = 1
        assertEquals(7, result.get(Constants.CORRECT)); // 4 + 3
        assertEquals(3, result.get(Constants.INCORRECT)); // 1 + 2
        assertTrue(result.containsKey(Constants.OVERALL_RESULT)); // ((correct / (correct + incorrect)) * 100)
    }

    @Test
    void testWriteDataToDatabaseAndTriggerKafkaEvent_WhenPassIsTrueAndCategoryIsNotStandalone() throws Exception {
        // Arrange
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assessmentId123");
        submitRequest.put(Constants.COURSE_ID, "course123");
        submitRequest.put(Constants.BATCH_ID, "batch123");
        submitRequest.put(Constants.USER_ID, "user123");
        submitRequest.put("competencies_v3", "[{\"name\": \"Java\"}]");

        Map<String, Object> questionSetFromAssessment = new HashMap<>();
        questionSetFromAssessment.put(Constants.START_TIME, "2024-07-15T10:15:30Z");

        Map<String, Object> result = new HashMap<>();
        result.put(Constants.PASS, true);
        result.put(Constants.OVERALL_RESULT, 95.0);

        String userId = "user123";
        String primaryCategory = "Competency Assessment";
        String courseCategory = "Other"; // Not "Standalone Assessment"
        String token = "authToken";
        String contextCategory = ""; // Not PRE_ENROLLED_ASSESSMENT

        Instant fakeInstant = Instant.parse("2024-07-15T10:15:30Z");

        // Mocks
        when(assessUtilServ.parseStartTimeToInstant(any())).thenReturn(fakeInstant);
        when(assessmentRepository.updateUserAssesmentDataToDB(eq(userId), eq("assessmentId123"),
                anyMap(), eq(result), eq(Constants.SUBMITTED), eq(fakeInstant), isNull()))
                .thenReturn(true);
        when(contentService.updateContentProgress(eq(token), anyMap(), eq(userId), any(SBApiResponse.class)))
                .thenReturn(Constants.SUCCESS);
        when(serverProperties.getAssessmentSubmitTopic()).thenReturn("assessment-submit-topic");

        doNothing().when(producer).push(anyString(), any());

        ReflectionTestUtils.setField(service, "kafkaProducer", producer);

        // Reflectively invoke the updated private method
        Class<?> eventCtxClass = Class.forName(
                "com.igot.cb.assessment.service.AssessmentServiceV5Impl$KafkaEventContext");
        Constructor<?> eventCtxCtor = eventCtxClass.getDeclaredConstructor(
                Map.class, String.class, String.class, String.class, String.class, String.class);
        eventCtxCtor.setAccessible(true);
        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod(
                "writeDataToDatabaseAndTriggerKafkaEvent",
                eventCtxClass, Map.class, Map.class
        );
        method.setAccessible(true);
        method.invoke(service,
                eventCtxCtor.newInstance(submitRequest, userId, token, primaryCategory, courseCategory, contextCategory),
                questionSetFromAssessment, result);

        // Assert interactions
        verify(assessmentRepository, times(1)).updateUserAssesmentDataToDB(eq(userId), eq("assessmentId123"),
                anyMap(), eq(result), eq(Constants.SUBMITTED), eq(fakeInstant), isNull());

        verify(contentService, times(1)).updateContentProgress(eq(token), anyMap(), eq(userId), any(SBApiResponse.class));
        verify(producer, times(1)).push(eq("assessment-submit-topic"), any());
    }


    @Test
    @SuppressWarnings("unchecked")
    void testReadSectionLevelParams_WithShufflePath() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        // Arrange
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        Map<String, Object> assessmentFilteredDetail = new HashMap<>();

        // Section-level mock data
        Map<String, Object> question1 = Map.of(Constants.IDENTIFIER, "q2");
        List<Map<String, Object>> questions = List.of(question1);

        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put("name", "Section Name");
        section.put(Constants.CHILDREN, questions);
        section.put(Constants.MAX_QUESTIONS, 1); // limit

        assessmentAllDetail.put(Constants.CHILDREN, List.of(section));
        assessmentAllDetail.put(Constants.ASSESSMENT_TYPE, "Other"); // Not QUESTION_WEIGHTAGE

        // Section params to extract
        List<String> sectionParams = List.of("name", "description");
        when(serverProperties.getAssessmentSectionParams()).thenReturn(sectionParams);

        // Act
        Method method = AssessmentServiceV5Impl.class.getDeclaredMethod(
                "readSectionLevelParams", Map.class, Map.class);
        method.setAccessible(true);
        method.invoke(service, assessmentAllDetail, assessmentFilteredDetail);

        // Assert
        assertTrue(assessmentFilteredDetail.containsKey(Constants.CHILDREN));
        assertTrue(assessmentFilteredDetail.containsKey(Constants.CHILD_NODES));

        List<Map<String, Object>> filteredSections = (List<Map<String, Object>>) assessmentFilteredDetail.get(Constants.CHILDREN);
        assertEquals(1, filteredSections.size());
        Map<String, Object> filteredSection = filteredSections.get(0);

        assertTrue(filteredSection.containsKey(Constants.CHILD_NODES));
        List<String> childNodes = (List<String>) filteredSection.get(Constants.CHILD_NODES);
        assertEquals(1, childNodes.size());
        assertEquals("q2", childNodes.get(0));
    }

    @Test
    void testRetakeAssessment_WithRetakeVerificationEnabled() {
        // Arrange
        String token = "authToken";
        String userId = "user123";
        String assessmentIdentifier = "assess123";

        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3); // Allowed
        assessmentDetail.put(Constants.CONTEXT_CATEGORY_TAG, "SomeOtherContext"); // not pre-enrolled

        when(accessTokenValidator.fetchUserIdFromAccessToken(token)).thenReturn(userId);
        when(assessUtilServ.readAssessmentHierarchyFromCache(assessmentIdentifier, false, token)).thenReturn(assessmentDetail);
        when(serverProperties.isAssessmentRetakeCountVerificationEnabled()).thenReturn(true);

        // You must mock the private method using a spy
        AssessmentServiceV5Impl spyService = Mockito.spy(service);
        //doReturn(2).when(spyService).calculateAssessmentRetakeCount(userId, assessmentIdentifier); // returns 2

        // Act
        SBApiResponse response = spyService.retakeAssessment(assessmentIdentifier, token, false);

        // Assert
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(3, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED)); // 0 submissions
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
    void testGetShuffleFlagFromHierarchy_ShuffleTrueForMatchingSection() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.SHUFFLE, true);
        hierarchy.put(Constants.CHILDREN, List.of(
                Map.of(Constants.IDENTIFIER, "q1"), Map.of(Constants.IDENTIFIER, "q2")
        ));
        boolean result = assessUtilServ.getShuffleFlagFromHierarchy(hierarchy);
        assertTrue(result);
    }

    @Test
    void testGetShuffleFlagFromHierarchy_ShuffleFalseForMatchingSection() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.SHUFFLE, false);
        hierarchy.put(Constants.CHILDREN, List.of(
                Map.of(Constants.IDENTIFIER, "q1"), Map.of(Constants.IDENTIFIER, "q2")
        ));
        boolean result = assessUtilServ.getShuffleFlagFromHierarchy(hierarchy);
        assertFalse(result);
    }

    @Test
    void testGetShuffleFlagFromHierarchy_EmptySections_ReturnsTrue() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CHILDREN, Collections.emptyList());
        boolean result = assessUtilServ.getShuffleFlagFromHierarchy(hierarchy);
        assertTrue(result);
    }

    @Test
    void testGetShuffleFlagFromHierarchy_NoMatchingSection_ReturnsTrue() {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.CHILDREN, List.of(
                Map.of(Constants.IDENTIFIER, "q1")
        ));
        boolean result = assessUtilServ.getShuffleFlagFromHierarchy(hierarchy);
        assertTrue(result);
    }

    @Test
    void testGetShuffleFlagFromHierarchy_NullChildren_ReturnsTrue() {
        Map<String, Object> hierarchy = new HashMap<>();
        boolean result = assessUtilServ.getShuffleFlagFromHierarchy(hierarchy);
        assertTrue(result);
    }

    // ------------------------------------------------------------------
    // Shared fixtures for the submit / read flows below
    // ------------------------------------------------------------------

    private static final String STORED_QUESTION_SET =
            "{\"starttime\":1700000000000,\"children\":[{\"identifier\":\"sec1\",\"childNodes\":[\"q1\"]}]}";

    private Map<String, Object> newSection(String sectionId, String... questionIds) {
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, sectionId);
        List<Map<String, Object>> questions = new ArrayList<>();
        for (String questionId : questionIds) {
            Map<String, Object> question = new HashMap<>();
            question.put(Constants.IDENTIFIER, questionId);
            questions.add(question);
        }
        section.put(Constants.CHILDREN, questions);
        return section;
    }

    private Map<String, Object> newHierarchySection(String sectionId) {
        Map<String, Object> hierarchySection = newSection(sectionId, "q1");
        hierarchySection.put(Constants.PRIMARY_CATEGORY, "Section");
        hierarchySection.put(Constants.NAME, "Section " + sectionId);
        hierarchySection.put(Constants.TOTAL_MARKS, 10);
        Map<String, Map<String, Object>> sectionLevelDefinition = new HashMap<>();
        sectionLevelDefinition.put("easy", new HashMap<>(Map.of("marksForQuestion", 5, Constants.NO_OF_QUESTIONS, 1)));
        hierarchySection.put(Constants.SECTION_LEVEL_DEFINITION, sectionLevelDefinition);
        return hierarchySection;
    }

    private Map<String, Object> buildSubmitHierarchy(String assessmentType, String primaryCategory,
                                                     String contextCategory) {
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.ASSESSMENT_TYPE, assessmentType);
        hierarchy.put(Constants.PRIMARY_CATEGORY, primaryCategory);
        if (contextCategory != null) {
            hierarchy.put(Constants.CONTEXT_CATEGORY_TAG, contextCategory);
        }
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);
        hierarchy.put(Constants.EXPECTED_DURATION, 600);
        hierarchy.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);
        hierarchy.put(Constants.CHILDREN, new ArrayList<>(List.of(newHierarchySection("sec1"))));
        return hierarchy;
    }

    private Map<String, Object> buildSubmitRequest() {
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.COURSE_ID, "course1");
        submitRequest.put(Constants.BATCH_ID, "batch1");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>(List.of(newSection("sec1", "q1"))));
        return submitRequest;
    }

    private Map<String, Object> existingAttempt(Object startTime, String questionSet) {
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.START_TIME, startTime);
        attempt.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        attempt.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, questionSet);
        return attempt;
    }

    private Map<String, Object> scoreResult(double result, int correct, int incorrect) {
        Map<String, Object> score = new HashMap<>();
        score.put(Constants.RESULT, result);
        score.put(Constants.BLANK, 0);
        score.put(Constants.CORRECT, correct);
        score.put(Constants.INCORRECT, incorrect);
        score.put(Constants.SECTION_MARKS, correct * 5.0);
        score.put(Constants.TOTAL_MARKS, 10);
        return score;
    }

    private void stubSubmitFlow(Map<String, Object> hierarchy, List<Map<String, Object>> existing,
                                Map<String, Object> score) throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString())).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString())).thenReturn(existing);
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("120");
        when(contentService.readContent(anyString())).thenReturn(Map.of(Constants.COURSE_CATEGORY, "Course"));
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of("q1", Map.of(Constants.IDENTIFIER, "q1")));
        when(assessUtilServ.validateQumlAssessmentV3(any(), any(), any(), any())).thenReturn(score);
        when(assessUtilServ.parseStartTimeToInstant(any())).thenReturn(Instant.now());
        when(assessUtilServ.parseStartTimeToLong(any())).thenReturn(1000L);
        when(assessmentRepository.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Boolean.TRUE);
        when(serverProperties.getMandatoryContextCategoriesForPassRequirement())
                .thenReturn(List.of("Final Milestone Assessment"));
        when(serverProperties.getMandatoryCourseCategoriesForCertificateGeneration())
                .thenReturn(List.of("Mandatory Course"));
        when(serverProperties.getAssessmentSubmitTopic()).thenReturn("submit-topic");
    }

    // ------------------------------------------------------------------
    // submitAssessmentAsync / submitAssessmentAsyncV6
    // ------------------------------------------------------------------

    @Test
    void testSubmitAssessmentAsync_AssessmentLevel_CompetencyPass_PushesKafkaEvent() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Competency Assessment", "Other");
        Map<String, Object> submitRequest = buildSubmitRequest();
        submitRequest.put(Constants.COMPETENCIES_V3, "[{\"name\":\"c1\"}]");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));

        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(Boolean.TRUE, response.getResult().get(Constants.PASS));
        assertEquals("Competency Assessment", response.getResult().get(Constants.PRIMARY_CATEGORY));
        verify(contentService).updateContentProgress(eq("token"), eq(submitRequest), eq("user1"), any());
        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(producer).push(eq("submit-topic"), eventCaptor.capture());
        Map<String, Object> event = (Map<String, Object>) eventCaptor.getValue();
        assertEquals("course1", event.get(Constants.COURSE_ID));
        assertEquals("batch1", event.get(Constants.BATCH_ID));
        assertEquals(Map.of("name", "c1"), event.get(Constants.COMPETENCY));
    }

    @Test
    void testSubmitAssessmentAsync_PreEnrolled_DateStartTime_NoCourseOrBatch() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Competency Assessment",
                Constants.PRE_ENROLLED_ASSESSMENT_KEY);
        Map<String, Object> submitRequest = buildSubmitRequest();
        submitRequest.remove(Constants.COURSE_ID);
        submitRequest.remove(Constants.BATCH_ID);
        submitRequest.put(Constants.COMPETENCIES_V3, "[]");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Date.from(Instant.now().minusSeconds(60)), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));

        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(contentService).updatePreEnrolledAssessment(eq("token"), eq(submitRequest), eq("user1"), any());
        verify(contentService, never()).updateContentProgress(any(), any(), any(), any());
        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(producer).push(eq("submit-topic"), eventCaptor.capture());
        Map<String, Object> event = (Map<String, Object>) eventCaptor.getValue();
        assertEquals("", event.get(Constants.COURSE_ID));
        assertEquals("", event.get(Constants.BATCH_ID));
        assertEquals("", event.get(Constants.COMPETENCY));
    }

    @Test
    void testSubmitAssessmentAsync_MandatoryContextCategoryFailed_SkipsContentUpdate() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment",
                "Final Milestone Assessment");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(10.0, 1, 4));

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(Boolean.FALSE, response.getResult().get(Constants.PASS));
        verify(assessmentRepository).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
        verify(contentService, never()).updateContentProgress(any(), any(), any(), any());
        verify(producer, never()).push(anyString(), any());
    }

    @Test
    void testSubmitAssessmentAsync_MandatoryCourseCategoryFailed_SkipsContentUpdate() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", null);
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(10.0, 1, 4));
        when(contentService.readContent(anyString()))
                .thenReturn(Map.of(Constants.COURSE_CATEGORY, "Mandatory Course"));

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(contentService, never()).updateContentProgress(any(), any(), any(), any());
        verify(producer, never()).push(anyString(), any());
    }

    @Test
    void testSubmitAssessmentAsync_MandatoryCourseCategoryPassed_PushesEvent() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", null);
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(90.0, 9, 1));
        when(contentService.readContent(anyString()))
                .thenReturn(Map.of(Constants.COURSE_CATEGORY, "Mandatory Course"));

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(producer).push(eq("submit-topic"), any());
    }

    @Test
    void testSubmitAssessmentAsync_DbUpdateFails_NoKafkaEvent() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));
        when(assessmentRepository.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Boolean.FALSE);

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(contentService, never()).updateContentProgress(any(), any(), any(), any());
        verify(producer, never()).push(anyString(), any());
    }

    @Test
    void testSubmitAssessmentAsync_WriteDataThrows_IsSwallowed() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));
        when(assessUtilServ.parseStartTimeToInstant(any())).thenThrow(new RuntimeException("bad time"));

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(assessmentRepository, never())
                .updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
        verify(producer, never()).push(anyString(), any());
    }

    @Test
    void testSubmitAssessmentAsync_StoredQuestionSetWithoutChildren_SkipsPersistence() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), "{}")),
                scoreResult(80.0, 4, 1));

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        // no stored start time -> nothing is written
        verify(assessmentRepository, never())
                .updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void testSubmitAssessmentAsync_UnsupportedStartTimeType() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy, List.of(existingAttempt(12345L, STORED_QUESTION_SET)), scoreResult(80.0, 4, 1));

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.READ_ASSESSMENT_START_TIME_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_SubmissionExpired() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(7200), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_SUBMIT_EXPIRED, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_WrongSectionDetails() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));
        Map<String, Object> submitRequest = buildSubmitRequest();
        submitRequest.put(Constants.CHILDREN, new ArrayList<>(List.of(newSection("unknownSection", "q1"))));

        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.WRONG_SECTION_DETAILS, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_BlankStoredQuestionSet() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), "")),
                scoreResult(80.0, 4, 1));

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_InvalidQuestionSubmitted() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));
        Map<String, Object> submitRequest = buildSubmitRequest();
        Map<String, Object> sectionWithoutChildren = new HashMap<>();
        sectionWithoutChildren.put(Constants.IDENTIFIER, "sec1");
        Map<String, Object> sectionWithEmptyChildren = newSection("sec1");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>(List.of(
                newSection("sec1", "qNotInAssessment"), sectionWithoutChildren, sectionWithEmptyChildren)));

        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_SUBMIT_INVALID_QUESTION, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_LanguageValidationFails() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));
        when(assessUtilServ.validateAssessmentLanguageAndNodes(anyMap())).thenReturn("Invalid language");

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("Invalid language", response.getParams().getErrmsg());
        verify(assessUtilServ, never()).validateQumlAssessmentV3(any(), any(), any(), any());
    }

    @Test
    void testSubmitAssessmentAsyncV6_LanguageValidationFails() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy("default", "Course Assessment", "Other");
        stubSubmitFlow(hierarchy,
                List.of(existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET)),
                scoreResult(80.0, 4, 1));
        when(assessUtilServ.validateAssessmentLanguageAndNodes(anyMap())).thenReturn("Invalid language");

        SBApiResponse response = service.submitAssessmentAsyncV6(buildSubmitRequest(), "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("Invalid language", response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessmentAsync_SectionLevel_MultipleSections_PartialPass() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy(Constants.QUESTION_WEIGHTAGE, "Course Assessment", null);
        Map<String, Object> secondSection = newHierarchySection("sec2");
        secondSection.put(Constants.MINIMUM_PASS_PERCENTAGE, 40);
        ((List<Map<String, Object>>) hierarchy.get(Constants.CHILDREN)).add(secondSection);

        Map<String, Object> submittedWithNullResponse = new HashMap<>();
        submittedWithNullResponse.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, null);
        Map<String, Object> submittedWithResponse = new HashMap<>();
        submittedWithResponse.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "{}");
        List<Map<String, Object>> existing = List.of(
                existingAttempt(Instant.now().minusSeconds(60), STORED_QUESTION_SET),
                submittedWithNullResponse, submittedWithResponse);

        Map<String, Object> failingSection = scoreResult(20.0, 0, 0);
        failingSection.remove(Constants.SECTION_MARKS);
        failingSection.remove(Constants.TOTAL_MARKS);
        stubSubmitFlow(hierarchy, existing, scoreResult(80.0, 4, 1));
        when(assessUtilServ.validateQumlAssessmentV3(any(), any(), any(), any()))
                .thenReturn(scoreResult(80.0, 4, 1), failingSection);

        SBApiResponse response = service.submitAssessmentAsync(buildSubmitRequest(), "token", false);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(Boolean.FALSE, response.getResult().get(Constants.PASS));
        assertEquals(80.0, (Double) response.getResult().get(Constants.OVERALL_RESULT), 0.001);
        assertEquals(3, response.getResult().get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS));
        assertEquals(1, response.getResult().get(Constants.RETAKE_ATTEMPT_CONSUMED));
        assertEquals(2, ((List<?>) response.getResult().get(Constants.CHILDREN)).size());
        verify(producer).push(eq("submit-topic"), any());
    }

    @Test
    void testSubmitAssessmentAsync_SectionLevel_EditMode_SkipsPersistence() throws Exception {
        Map<String, Object> hierarchy = buildSubmitHierarchy(Constants.QUESTION_WEIGHTAGE, "Course Assessment", "Other");
        stubSubmitFlow(hierarchy, Collections.emptyList(), scoreResult(80.0, 4, 1));
        Map<String, Object> submitRequest = buildSubmitRequest();
        submitRequest.put(Constants.CHILDREN, new ArrayList<>(List.of(newSection("sec1"))));

        SBApiResponse response = service.submitAssessmentAsync(submitRequest, "token", true);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        verify(assessUtilServ, never()).parseStartTimeToLong(any());
        verify(assessmentRepository, never())
                .updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
        verify(contentService, never()).updateContentProgress(any(), any(), any(), any());
    }

    // ------------------------------------------------------------------
    // retakeAssessment
    // ------------------------------------------------------------------

    @Test
    void testRetakeAssessment_ZeroRetakeAttemptsAllowed() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(new HashMap<>(Map.of(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 0)));

        SBApiResponse response = service.retakeAssessment("assess1", "token", false);

        assertEquals(0, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(-1, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
    }

    @Test
    void testRetakeAssessment_NoMaxAttempts_NonCyclicalCountsSubmittedOnly() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>(Map.of(Constants.PRIMARY_CATEGORY, "Course Assessment"));
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString())).thenReturn(hierarchy);
        Map<String, Object> nullResponse = new HashMap<>();
        nullResponse.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, null);
        List<Map<String, Object>> attempts = List.of(nullResponse, new HashMap<>(),
                new HashMap<>(Map.of(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "{}")));
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1")).thenReturn(attempts);

        SBApiResponse response = service.retakeAssessment("assess1", "token", false);

        assertEquals(0, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(1, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    private Map<String, Object> stubCyclicalRetake(int cycleCount, String coolOffError) {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>(Map.of(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3));
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString())).thenReturn(hierarchy);
        when(assessUtilServ.readUserSubmittedAssessmentRecords(anyString(), anyString()))
                .thenReturn(createMockAttempts(3));
        when(assessUtilServ.hasCoolOffPeriod(hierarchy)).thenReturn(true);
        when(assessUtilServ.calculateCyclicalRetakeAttempts(anyString(), anyString(), anyMap(), anyList()))
                .thenReturn(cycleCount);
        when(assessUtilServ.validateCoolOffPeriod(anyString(), anyString(), anyMap(), anyList()))
                .thenReturn(coolOffError);
        return hierarchy;
    }

    @Test
    void testRetakeAssessment_Cyclical_WithinCycle() {
        stubCyclicalRetake(1, null);

        SBApiResponse response = service.retakeAssessment("assess1", "token", false);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(3, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(1, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
        verify(assessUtilServ, never()).validateCoolOffPeriod(anyString(), anyString(), anyMap(), anyList());
    }

    @Test
    void testRetakeAssessment_Cyclical_CoolOffActive() {
        stubCyclicalRetake(3, "Cool-off period active");

        SBApiResponse response = service.retakeAssessment("assess1", "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("Cool-off period active", response.getParams().getErrmsg());
        assertFalse(response.getResult().containsKey(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void testRetakeAssessment_Cyclical_CoolOffExpired_ResetsCount() {
        stubCyclicalRetake(3, "");

        SBApiResponse response = service.retakeAssessment("assess1", "token", false);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(0, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    // ------------------------------------------------------------------
    // readAssessment
    // ------------------------------------------------------------------

    private Map<String, Object> buildReadHierarchy(String primaryCategory, String assessmentType) {
        Map<String, Object> question1 = new HashMap<>(Map.of(Constants.IDENTIFIER, "q1", Constants.QUESTION_LEVEL, "easy"));
        Map<String, Object> question2 = new HashMap<>(Map.of(Constants.IDENTIFIER, "q2", Constants.QUESTION_LEVEL, "easy"));
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "sec1");
        section.put(Constants.NAME, "Section 1");
        section.put(Constants.CHILDREN, List.of(question1, question2));
        section.put(Constants.SECTION_LEVEL_DEFINITION,
                Map.of("easy", Map.of(Constants.NO_OF_QUESTIONS, 1, "marksForQuestion", 2)));
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, primaryCategory);
        hierarchy.put(Constants.ASSESSMENT_TYPE, assessmentType);
        hierarchy.put(Constants.EXPECTED_DURATION, 600);
        hierarchy.put(Constants.CHILDREN, List.of(section));
        return hierarchy;
    }

    @Test
    void testReadAssessment_EditMode_QuestionWeightage_FiltersParams() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.fetchHierarchyFromAssessServc("assess1", "token"))
                .thenReturn(buildReadHierarchy("Course Assessment", Constants.QUESTION_WEIGHTAGE));
        when(serverProperties.getAssessmentLevelParams()).thenReturn(List.of(Constants.PRIMARY_CATEGORY, "notPresent"));
        when(serverProperties.getAssessmentSectionParams()).thenReturn(List.of(Constants.IDENTIFIER, Constants.NAME));

        SBApiResponse response = service.readAssessment("assess1", "token", true, null);

        Map<String, Object> questionSet = (Map<String, Object>) response.getResult().get(Constants.QUESTION_SET);
        assertEquals("Course Assessment", questionSet.get(Constants.PRIMARY_CATEGORY));
        assertFalse(questionSet.containsKey("notPresent"));
        List<Map<String, Object>> sections = (List<Map<String, Object>>) questionSet.get(Constants.CHILDREN);
        assertEquals("Section 1", sections.get(0).get(Constants.NAME));
        assertEquals(1, ((List<?>) sections.get(0).get(Constants.CHILD_NODES)).size());
        verify(assessUtilServ, never()).readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString());
    }

    @Test
    void testReadAssessment_SubmittedBeforeEndTime_StartsRetakeAttempt() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.END_TIME, Instant.now().plusSeconds(3600));
        attempt.put(Constants.STATUS, Constants.SUBMITTED);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1")).thenReturn(List.of(attempt));
        when(assessUtilServ.validateContextLocking(anyMap(), any(), any(), anyString(), anyString())).thenReturn("");
        when(assessmentRepository.addUserAssesmentDataToDB(anyString(), anyString(), any(), any(), anyMap(), anyString()))
                .thenReturn(true);

        SBApiResponse response = service.readAssessment("assess1", "token", false, "parent");

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertNotNull(response.getResult().get(Constants.QUESTION_SET));
        verify(assessmentRepository).addUserAssesmentDataToDB(eq("user1"), eq("assess1"), any(), any(), anyMap(),
                eq(Constants.NOT_SUBMITTED));
    }

    @Test
    void testReadAssessment_UnknownStatusBeforeEndTime_ReturnsWithoutQuestionSet() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.END_TIME, Date.from(Instant.now().plusSeconds(3600)));
        attempt.put(Constants.STATUS, "IN_REVIEW");
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1")).thenReturn(List.of(attempt));

        SBApiResponse response = service.readAssessment("assess1", "token", false, "parent");

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertNull(response.getResult().get(Constants.QUESTION_SET));
        verify(assessmentRepository, never()).addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any());
    }

    @Test
    void testReadAssessment_RetakeWithNegativeLimit_ContextLocked() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = buildReadHierarchy("Course Assessment", "default");
        hierarchy.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, -1);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString())).thenReturn(hierarchy);
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.END_TIME, Date.from(Instant.now().minusSeconds(3600)));
        attempt.put(Constants.STATUS, Constants.SUBMITTED);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1")).thenReturn(List.of(attempt));
        when(assessUtilServ.validateContextLocking(anyMap(), any(), any(), anyString(), anyString()))
                .thenReturn("Context locked");

        SBApiResponse response = service.readAssessment("assess1", "token", false, "parent");

        assertNull(response.getResult().get(Constants.QUESTION_SET));
        verify(assessUtilServ, never()).hasCoolOffPeriod(anyMap());
        verify(assessmentRepository, never()).addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any());
    }

    // ------------------------------------------------------------------
    // readQuestionList
    // ------------------------------------------------------------------

    private Map<String, Object> questionListRequest(String assessmentId, List<String> ids) {
        Map<String, Object> search = new HashMap<>();
        search.put(Constants.IDENTIFIER, ids);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.SEARCH, search);
        Map<String, Object> body = new HashMap<>();
        body.put(Constants.ASSESSMENT_ID_KEY, assessmentId);
        body.put(Constants.REQUEST, request);
        return body;
    }

    @Test
    void testReadQuestionList_HierarchyMissing() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Collections.emptyMap());

        SBApiResponse response = service.readQuestionList(questionListRequest("assess1", List.of("q1")), "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testReadQuestionList_PracticeSet_UsesHierarchyShuffleFlag() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> hierarchy = new HashMap<>();
        hierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        hierarchy.put(Constants.SHUFFLE, false);
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString())).thenReturn(hierarchy);
        Map<String, Object> q1 = Map.of(Constants.IDENTIFIER, "q1");
        when(assessUtilServ.readQListfromCache(anyList(), anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of("q1", q1));
        when(assessUtilServ.filterQuestionMapDetailV2(q1, Constants.PRACTICE_QUESTION_SET, false)).thenReturn(q1);

        SBApiResponse response = service.readQuestionList(questionListRequest("assess1", List.of("q1")), "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(List.of(q1), response.getResult().get(Constants.QUESTIONS));
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
    }

    @Test
    void testReadQuestionList_NoUserAssessmentData() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of(Constants.PRIMARY_CATEGORY, "Course Assessment"));
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1")).thenReturn(Collections.emptyList());

        SBApiResponse response = service.readQuestionList(questionListRequest("assess1", List.of("q1")), "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, response.getParams().getErrmsg());
    }

    @Test
    void testReadQuestionList_EmptyStoredQuestionSet_InvalidAssessmentId() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of(Constants.PRIMARY_CATEGORY, "Course Assessment"));
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1"))
                .thenReturn(List.of(Map.of(Constants.ASSESSMENT_READ_RESPONSE_KEY, "{}")));

        SBApiResponse response = service.readQuestionList(questionListRequest("assess1", List.of("q1")), "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_ID_INVALID, response.getParams().getErrmsg());
    }

    @Test
    void testReadQuestionList_QuestionIdsDoNotMatch() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(Map.of(Constants.PRIMARY_CATEGORY, "Course Assessment"));
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1"))
                .thenReturn(List.of(Map.of(Constants.ASSESSMENT_READ_RESPONSE_KEY, STORED_QUESTION_SET)));

        SBApiResponse response = service.readQuestionList(questionListRequest("assess1", List.of("q1", "q99")),
                "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.THE_QUESTIONS_IDS_PROVIDED_DONT_MATCH, response.getParams().getErrmsg());
        verify(assessUtilServ, never()).readQListfromCache(anyList(), anyString(), anyBoolean(), anyString());
    }

    @Test
    void testGetQuestionIdList_InvalidRequestShapes_ReturnEmpty() {
        List<Map<String, Object>> invalidBodies = new ArrayList<>();
        invalidBodies.add(new HashMap<>());
        invalidBodies.add(new HashMap<>(Map.of(Constants.REQUEST, new HashMap<>())));
        invalidBodies.add(new HashMap<>(Map.of(Constants.REQUEST, Map.of("other", "x"))));
        invalidBodies.add(new HashMap<>(Map.of(Constants.REQUEST, Map.of(Constants.SEARCH, new HashMap<>()))));
        invalidBodies.add(new HashMap<>(Map.of(Constants.REQUEST, Map.of(Constants.SEARCH, Map.of("other", "x")))));
        invalidBodies.add(new HashMap<>(Map.of(Constants.REQUEST,
                Map.of(Constants.SEARCH, Map.of(Constants.IDENTIFIER, Collections.emptyList())))));
        invalidBodies.add(new HashMap<>(Map.of(Constants.REQUEST, "not-a-map")));

        for (Map<String, Object> body : invalidBodies) {
            List<String> ids = assessUtilServ.getQuestionIdList(body);
            assertNotNull(ids);
            assertTrue(ids.isEmpty(), "Expected empty id list for " + body);
        }
        List<String> ids = assessUtilServ.getQuestionIdList(
                questionListRequest("assess1", List.of("q1")));
        assertEquals(List.of("q1"), ids);
    }

    // ------------------------------------------------------------------
    // readAssessmentResultV5
    // ------------------------------------------------------------------

    @Test
    void testReadAssessmentResultV5_InvalidRequests() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        SBApiResponse noRequestKey = service.readAssessmentResultV5(new HashMap<>(Map.of("x", "y")), "token");
        assertEquals(HttpStatus.BAD_REQUEST, noRequestKey.getResponseCode());
        assertEquals(Constants.INVALID_REQUEST, noRequestKey.getParams().getErrmsg());

        SBApiResponse emptyBody = service.readAssessmentResultV5(
                new HashMap<>(Map.of(Constants.REQUEST, new HashMap<>())), "token");
        assertEquals(Constants.INVALID_REQUEST, emptyBody.getParams().getErrmsg());

        SBApiResponse missingCourse = service.readAssessmentResultV5(
                new HashMap<>(Map.of(Constants.REQUEST, Map.of(Constants.ASSESSMENT_ID_KEY, "a1"))), "token");
        assertEquals(HttpStatus.BAD_REQUEST, missingCourse.getResponseCode());
        assertTrue(missingCourse.getParams().getErrmsg().contains(Constants.COURSE_ID));
        assertFalse(missingCourse.getParams().getErrmsg().contains(Constants.ASSESSMENT_ID_KEY));

        SBApiResponse blankFields = service.readAssessmentResultV5(new HashMap<>(Map.of(Constants.REQUEST,
                Map.of(Constants.ASSESSMENT_ID_KEY, " ", Constants.COURSE_ID, " "))), "token");
        assertTrue(blankFields.getParams().getErrmsg().contains(Constants.COURSE_ID));
        assertTrue(blankFields.getParams().getErrmsg().contains(Constants.ASSESSMENT_ID_KEY));
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
    }

    private Map<String, Object> validResultRequest() {
        return new HashMap<>(Map.of(Constants.REQUEST,
                Map.of(Constants.ASSESSMENT_ID_KEY, "a1", Constants.COURSE_ID, "c1")));
    }

    @Test
    void testReadAssessmentResultV5_SubmittedWithBlankResponse() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "a1"))
                .thenReturn(List.of(Map.of(Constants.STATUS, Constants.SUBMITTED,
                        Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "")));

        SBApiResponse response = service.readAssessmentResultV5(validResultRequest(), "token");

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testReadAssessmentResultV5_InvalidStoredJson() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "a1"))
                .thenReturn(List.of(Map.of(Constants.STATUS, Constants.SUBMITTED,
                        Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "{not-json")));

        SBApiResponse response = service.readAssessmentResultV5(validResultRequest(), "token");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertTrue(response.getParams().getErrmsg().startsWith("Failed to process Assessment read response"));
    }

    // ------------------------------------------------------------------
    // saveAssessmentAsync
    // ------------------------------------------------------------------

    private Map<String, Object> saveRequest() {
        return new HashMap<>(Map.of(Constants.IDENTIFIER, "assess1"));
    }

    private Map<String, Object> savedAttempt(Object start, Object end, String status) {
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.START_TIME, start);
        attempt.put(Constants.END_TIME, end);
        attempt.put(Constants.STATUS, status);
        attempt.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, STORED_QUESTION_SET);
        return attempt;
    }

    @Test
    void testSaveAssessmentAsync_EditMode_ReturnsQuestionSet() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.fetchHierarchyFromAssessServc("assess1", "token"))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));

        SBApiResponse response = service.saveAssessmentAsync(saveRequest(), "token", true);

        assertNotNull(response.getResult().get(Constants.QUESTION_SET));
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
    }

    @Test
    void testSaveAssessmentAsync_PracticeSet_ReturnsQuestionSet() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy(Constants.PRACTICE_QUESTION_SET, "default"));

        SBApiResponse response = service.saveAssessmentAsync(saveRequest(), "token", false);

        assertNotNull(response.getResult().get(Constants.QUESTION_SET));
        verify(assessmentRepository, never()).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void testSaveAssessmentAsync_NoExistingData() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1")).thenReturn(Collections.emptyList());

        SBApiResponse response = service.saveAssessmentAsync(saveRequest(), "token", false);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testSaveAssessmentAsync_AlreadySubmitted() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        Date start = new Date(System.currentTimeMillis() - 60_000);
        Date end = new Date(System.currentTimeMillis() + 60_000);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1"))
                .thenReturn(List.of(savedAttempt(start, end, Constants.SUBMITTED)));

        SBApiResponse response = service.saveAssessmentAsync(saveRequest(), "token", false);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
        verify(assessmentRepository, never()).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void testSaveAssessmentAsync_StartNotBeforeEnd() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        Date start = new Date(System.currentTimeMillis());
        Date end = new Date(start.getTime() - 1000);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1"))
                .thenReturn(List.of(savedAttempt(start, end, Constants.NOT_SUBMITTED)));

        SBApiResponse response = service.saveAssessmentAsync(saveRequest(), "token", false);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testSaveAssessmentAsync_DbUpdateFails() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        Date start = new Date(System.currentTimeMillis() - 60_000);
        Date end = new Date(System.currentTimeMillis() + 60_000);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1"))
                .thenReturn(List.of(savedAttempt(start, end, Constants.NOT_SUBMITTED)));
        when(assessmentRepository.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Boolean.FALSE);

        SBApiResponse response = service.saveAssessmentAsync(saveRequest(), "token", false);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED, response.getParams().getErrmsg());
        assertEquals(false, response.getResult().get("ASSESSMENT_UPDATE"));
    }

    @Test
    void testSaveAssessmentAsync_InvalidStoredTimes_HandlesException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1"))
                .thenReturn(List.of(savedAttempt("not-a-date", "not-a-date", Constants.NOT_SUBMITTED)));

        SBApiResponse response = service.saveAssessmentAsync(saveRequest(), "token", false);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertTrue(response.getParams().getErrmsg().startsWith("Error while reading assessment"));
    }

    // ------------------------------------------------------------------
    // readAssessmentSavePoint
    // ------------------------------------------------------------------

    @Test
    void testReadAssessmentSavePoint_EditMode_ReturnsQuestionSet() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.fetchHierarchyFromAssessServc("assess1", "token"))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));

        SBApiResponse response = service.readAssessmentSavePoint("assess1", "token", true);

        assertNotNull(response.getResult().get(Constants.QUESTION_SET));
        verify(assessUtilServ, never()).readUserSubmittedAssessmentRecords(anyString(), anyString());
    }

    @Test
    void testReadAssessmentSavePoint_EndTimePassedButSubmitted() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.END_TIME, new Date(System.currentTimeMillis() - 60_000));
        attempt.put(Constants.STATUS, Constants.SUBMITTED);
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1")).thenReturn(List.of(attempt));

        SBApiResponse response = service.readAssessmentSavePoint("assess1", "token", false);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ASSESSMENT_HIERARCHY_SAVE_NOT_AVBL, response.getParams().getErrmsg());
    }

    @Test
    void testReadAssessmentSavePoint_InvalidEndTime_HandlesException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(assessUtilServ.readAssessmentHierarchyFromCache(anyString(), anyBoolean(), anyString()))
                .thenReturn(buildReadHierarchy("Course Assessment", "default"));
        when(assessUtilServ.readUserSubmittedAssessmentRecords("user1", "assess1"))
                .thenReturn(List.of(Map.of(Constants.END_TIME, "not-a-date")));

        SBApiResponse response = service.readAssessmentSavePoint("assess1", "token", false);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertTrue(response.getParams().getErrmsg().startsWith("Error while reading assessment"));
    }

    // ------------------------------------------------------------------
    // Randomization / mark map helpers
    // ------------------------------------------------------------------

    @Test
    void testProcessRandomizationForQuestions_NoPositiveIntegerLimit_ReturnsOriginal() {
        List<Map<String, Object>> questions = List.of(
                Map.of(Constants.IDENTIFIER, "q1", Constants.QUESTION_LEVEL, "easy"),
                Map.of(Constants.IDENTIFIER, "q2", Constants.QUESTION_LEVEL, "hard"));
        Map<String, Map<String, Object>> definition = new HashMap<>();
        definition.put("easy", Map.of(Constants.NO_OF_QUESTIONS, 0));
        definition.put("hard", Map.of(Constants.NO_OF_QUESTIONS, "2"));

        List<Map<String, Object>> result = ReflectionTestUtils.invokeMethod(service,
                "processRandomizationForQuestions", definition, questions);

        assertSame(questions, result);
    }

    @Test
    void testProcessRandomizationForQuestions_IgnoresOtherKeysAndUnknownLevels() {
        List<Map<String, Object>> questions = List.of(
                Map.of(Constants.IDENTIFIER, "q1", Constants.QUESTION_LEVEL, "easy"),
                Map.of(Constants.IDENTIFIER, "q2", Constants.QUESTION_LEVEL, "easy"),
                Map.of(Constants.IDENTIFIER, "q3", Constants.QUESTION_LEVEL, "unknown"));
        Map<String, Map<String, Object>> definition = new HashMap<>();
        definition.put("easy", Map.of(Constants.NO_OF_QUESTIONS, 1, "marksForQuestion", 3));

        List<Map<String, Object>> result = ReflectionTestUtils.invokeMethod(service,
                "processRandomizationForQuestions", definition, questions);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("easy", result.get(0).get(Constants.QUESTION_LEVEL));
    }

    @Test
    void testGenerateMarkMap_IgnoresNonMarkKeys() {
        Map<String, Map<String, Object>> scheme = new HashMap<>();
        scheme.put("easy", Map.of("marksForQuestion", 2, Constants.NO_OF_QUESTIONS, 5));
        scheme.put("hard", Map.of(Constants.NO_OF_QUESTIONS, 1));

        Map<String, Integer> markMap = service.generateMarkMap(scheme);

        assertEquals(Map.of("easy", 2), markMap);
    }

    // ------------------------------------------------------------------
    // autoPublish
    // ------------------------------------------------------------------

    private void stubPublishFlow(Map<String, Object> publishResponse, Map<String, Object> updateOrgResponse) {
        when(accessTokenValidator.fetchUserIdFromAccessToken("token")).thenReturn("user1");
        when(serverProperties.getQuestionSetPublish()).thenReturn("/publish");
        when(serverProperties.getAssessmentHost()).thenReturn("http://host");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), any(), anyMap())).thenReturn(publishResponse);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(anyString(), anyString(), anyMap(), anyList()))
                .thenReturn(List.of(Map.of(Constants.ROOT_ORG_ID, "org1")));
        when(serverProperties.getSbUrl()).thenReturn("http://sb/");
        when(serverProperties.getUpdateOrgPath()).thenReturn("org/update");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), anyMap(), anyMap()))
                .thenReturn(updateOrgResponse);
    }

    @Test
    void testAutoPublish_PublishResponseNotOk() {
        stubPublishFlow(Map.of(Constants.RESPONSE_CODE, "SERVER_ERROR"), Map.of(Constants.RESPONSE_CODE, Constants.OK));

        SBApiResponse response = service.autoPublish("assess1", "token");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.PUBLISH_QUESTION_SET_FAILED, response.getParams().getErrmsg());
        verify(outboundRequestHandlerService).fetchResultUsingPost(eq("http://host/publish/assess1"), any(), anyMap());
        verify(cassandraOperation, never()).getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any());
    }

    @Test
    void testAutoPublish_UpdateOrgNotOk() {
        stubPublishFlow(Map.of(Constants.RESPONSE_CODE, Constants.OK), Map.of(Constants.RESPONSE_CODE, "CLIENT_ERROR"));

        SBApiResponse response = service.autoPublish("assess1", "token");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.UPDATE_ORG_WITH_CQF_ID_FAILED, response.getParams().getErrmsg());
        verify(outboundRequestHandlerService).fetchResultUsingPatch(eq("http://sb/org/update"), anyMap(), anyMap());
        verify(producer, never()).push(anyString(), any());
    }

    @Test
    void testAutoPublish_UpdateOrgEmptyResponse() {
        stubPublishFlow(Map.of(Constants.RESPONSE_CODE, Constants.OK), Collections.emptyMap());

        SBApiResponse response = service.autoPublish("assess1", "token");

        assertEquals(Constants.UPDATE_ORG_WITH_CQF_ID_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testAutoPublish_UserLookupFails_HandlesException() {
        stubPublishFlow(Map.of(Constants.RESPONSE_CODE, Constants.OK), Map.of(Constants.RESPONSE_CODE, Constants.OK));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(anyString(), anyString(), anyMap(), anyList()))
                .thenReturn(Collections.emptyList());

        SBApiResponse response = service.autoPublish("assess1", "token");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.AUTO_PUBLISH_FAILED, response.getParams().getErrmsg());
        verify(producer, never()).push(anyString(), any());
    }
}
