package com.igot.cb.assessment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.assessment.repo.AssessmentRepository;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.common.util.AccessTokenValidator;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import com.igot.cb.core.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import com.fasterxml.jackson.core.type.TypeReference;


import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class AssessmentServiceV2ImplTest {

    @InjectMocks
    private AssessmentServiceV2Impl assessmentServiceV2;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private AssessmentRepository assessmentRepository;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private AssessmentUtilServiceV2 assessUtilServ;

    @Mock
    private ObjectMapper mapper;

    @Mock
    private CbExtAssessmentServerProperties serverProperties;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Mock
    private Producer producer;

    private static final String TOKEN = "dummyToken";
    private static final String ASSESSMENT_ID = "assess123";
    private static final String USER_ID = "user123";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        AssessmentUtilServiceV2Impl realUtil = new AssessmentUtilServiceV2Impl(serverProperties,
                outboundRequestHandlerService, mapper, null, redisCacheMgr, null, producer);
        lenient().when(assessUtilServ.readAssessmentLevelData(any(), any()))
                .thenAnswer(inv -> realUtil.readAssessmentLevelData(inv.getArgument(0), inv.getArgument(1)));
        lenient().doAnswer(inv -> {
            realUtil.populateAssessmentFinalResults(inv.getArgument(0), inv.getArgument(1));
            return null;
        }).when(assessUtilServ).populateAssessmentFinalResults(any(), any());
        lenient().doAnswer(inv -> {
            realUtil.populateSectionFinalResults(inv.getArgument(0), inv.getArgument(1));
            return null;
        }).when(assessUtilServ).populateSectionFinalResults(any(), any());
        lenient().when(assessUtilServ.createResponseMapWithProperStructure(any(), any()))
                .thenAnswer(inv -> realUtil.createResponseMapWithProperStructure(inv.getArgument(0), inv.getArgument(1)));
    }

    // +ve: readAssessment returns success
    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessment_success() throws JsonProcessingException {
        when(serverProperties.getAssessmentLevelParams()).thenReturn(Collections.emptyList());
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        String assessmentHierarchyJson = new ObjectMapper().writeValueAsString(assessmentHierarchy);
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + ASSESSMENT_ID)).thenReturn(assessmentHierarchyJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentHierarchy);
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(new ArrayList<>());
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(true);
        SBApiResponse response = assessmentServiceV2.readAssessment(ASSESSMENT_ID, TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    // -ve: readAssessment with invalid token
    @Test
    void testReadAssessment_invalidToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(null);
        SBApiResponse response = assessmentServiceV2.readAssessment(ASSESSMENT_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(Constants.USER_ID_DOESNT_EXIST));
    }

    // -ve: readAssessment throws exception
    @Test
    void testReadAssessment_exception() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenThrow(new RuntimeException("DB error"));

        SBApiResponse response = assessmentServiceV2.readAssessment(ASSESSMENT_ID, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Failed to read Assessment"));
    }

    // +ve: submitAssessment returns success
    @Test
    @SuppressWarnings("unchecked")
    void testSubmitAssessment_success() throws Exception {
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("30");

        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILD_NODES, List.of("q1", "q2"));
        section.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);
        section.put(Constants.OBJECT_TYPE, "Section");
        section.put(Constants.PRIMARY_CATEGORY, "Section");
        section.put(Constants.CHILDREN, new ArrayList<>()); // questions

        List<Map<String, Object>> children = List.of(section);

        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.SCORE_CUTOFF_TYPE, "assessmentLevelScoreCutoff");
        assessmentHierarchy.put(Constants.CHILDREN, children);

        String assessmentHierarchyJson = new ObjectMapper().writeValueAsString(assessmentHierarchy);
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + ASSESSMENT_ID)).thenReturn(assessmentHierarchyJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentHierarchy);

        Map<String, Object> dbSection = new HashMap<>(section);
        dbSection.put(Constants.CHILD_NODES, List.of("q1", "q2"));
        List<Map<String, Object>> dbChildren = List.of(dbSection);

        Map<String, Object> dbAssessment = new HashMap<>(assessmentHierarchy);
        dbAssessment.put(Constants.CHILDREN, dbChildren);

        Map<String, Object> dbData = new HashMap<>();
        dbData.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        dbData.put(Constants.START_TIME, new Date());
        dbData.put(Constants.ASSESSMENT_READ_RESPONSE_KEY, new ObjectMapper().writeValueAsString(dbAssessment));

        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(dbData));
        Map<String, Object> submitSection = new HashMap<>();
        submitSection.put(Constants.IDENTIFIER, "section1");
        submitSection.put(Constants.CHILDREN, new ArrayList<>()); // questions

        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, ASSESSMENT_ID);
        submitRequest.put(Constants.CHILDREN, List.of(submitSection));

        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);

        SBApiResponse response = assessmentServiceV2.submitAssessment(submitRequest, TOKEN, false);
        System.out.println("Error: " + response.getParams().getErrmsg());
        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    // -ve: submitAssessment with missing user
    @Test
    void testSubmitAssessment_userNotFound() throws Exception {
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, ASSESSMENT_ID);
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(null);

        SBApiResponse response = assessmentServiceV2.submitAssessment(submitRequest, TOKEN, false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(Constants.USER_ID_DOESNT_EXIST));
    }

    // -ve: submitAssessment with invalid assessment id
    @Test
    void testSubmitAssessment_invalidAssessmentId() throws Exception {
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "");
        submitRequest.put(Constants.CHILDREN, new ArrayList<>());
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);

        SBApiResponse response = assessmentServiceV2.submitAssessment(submitRequest, TOKEN, false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(Constants.INVALID_ASSESSMENT_ID));
    }

    // +ve: readQuestionList returns success
    @Test
    @SuppressWarnings("unchecked")
    void testReadQuestionList_success() throws JsonProcessingException {
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILD_NODES, List.of("q1", "q2"));
        section.put(Constants.OBJECT_TYPE, "Section");
        section.put(Constants.PRIMARY_CATEGORY, "Section");
        section.put(Constants.CHILDREN, new ArrayList<>());

        List<Map<String, Object>> children = List.of(section);

        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.IDENTIFIER, ASSESSMENT_ID);
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, children);
        String assessmentHierarchyJson =
                "{\"identifier\":\"assess123\",\"primaryCategory\":\"Assessment\",\"children\":[{\"identifier\":\"section1\",\"primaryCategory\":\"Section\",\"children\":[],\"childNodes\":[\"q1\",\"q2\"],\"objectType\":\"Section\"}],\"expectedDuration\":120}";

        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + ASSESSMENT_ID)).thenReturn(assessmentHierarchyJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentHierarchy);

        Map<String, Object> dbData = new HashMap<>();
        dbData.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        dbData.put(Constants.START_TIME, new Date());
        dbData.put(Constants.ASSESSMENT_READ_RESPONSE, assessmentHierarchyJson);
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(dbData));
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);

        Map<String, Object> questionSet = new HashMap<>(assessmentHierarchy);
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.QUESTION_SET, questionSet);
        Map<String, Object> readHierarchyApiResponse = new HashMap<>();
        readHierarchyApiResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        readHierarchyApiResponse.put(Constants.RESULT, result);

        Map<String, Object> search = new HashMap<>();
        search.put(Constants.IDENTIFIER, List.of("q1", "q2"));
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.SEARCH, search);
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.REQUEST, request);
        requestBody.put(Constants.ASSESSMENT_ID_KEY, ASSESSMENT_ID);

        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);

        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    // -ve: readQuestionList with missing assessment id
    @Test
    void testReadQuestionList_missingAssessmentId() {
        Map<String, Object> requestBody = new HashMap<>();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);

        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(Constants.ASSESSMENT_ID_KEY_IS_NOT_PRESENT_IS_EMPTY));
    }

    // -ve: readQuestionList with invalid token
    @Test
    void testReadQuestionList_invalidToken() {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, ASSESSMENT_ID);
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(null);

        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(Constants.USER_ID_DOESNT_EXIST));
    }

    // +ve: retakeAssessment returns success
    @Test
    @SuppressWarnings("unchecked")
    void testRetakeAssessment_success() throws Exception {
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        String assessmentHierarchyJson = new ObjectMapper().writeValueAsString(assessmentHierarchy);

        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(new ArrayList<>());
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + ASSESSMENT_ID)).thenReturn(assessmentHierarchyJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentHierarchy);

        SBApiResponse response = assessmentServiceV2.retakeAssessment(ASSESSMENT_ID, TOKEN);

        assertNotNull(response);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    // -ve: retakeAssessment with invalid token
    @Test
    void testRetakeAssessment_invalidToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(null);

        SBApiResponse response = assessmentServiceV2.retakeAssessment(ASSESSMENT_ID, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(Constants.USER_ID_DOESNT_EXIST));
    }

    // -ve: retakeAssessment throws exception
    @Test
    void testRetakeAssessment_exception() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenThrow(new RuntimeException("DB error"));

        SBApiResponse response = assessmentServiceV2.retakeAssessment(ASSESSMENT_ID, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Failed to read Assessment"));
    }

    @Test
    void testCalculateSectionFinalResults() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        // Prepare dummy section results
        Map<String, Object> section1 = new HashMap<>();
        section1.put(Constants.RESULT, 80.0);
        section1.put(Constants.TOTAL, 10);
        section1.put(Constants.BLANK, 1);
        section1.put(Constants.CORRECT, 8);
        section1.put(Constants.INCORRECT, 1);
        section1.put(Constants.PASS_PERCENTAGE, 50);

        Map<String, Object> section2 = new HashMap<>();
        section2.put(Constants.RESULT, 70.0);
        section2.put(Constants.TOTAL, 10);
        section2.put(Constants.BLANK, 2);
        section2.put(Constants.CORRECT, 7);
        section2.put(Constants.INCORRECT, 1);
        section2.put(Constants.PASS_PERCENTAGE, 60);

        List<Map<String, Object>> sectionLevelResults = Arrays.asList(section1, section2);

        // Use reflection to access the private method
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "calculateSectionFinalResults", List.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(service, sectionLevelResults);

        assertNotNull(result);
        assertEquals(75.0, result.get(Constants.OVERALL_RESULT));
        assertEquals(20, result.get(Constants.TOTAL));
        assertEquals(3, result.get(Constants.BLANK));
        assertEquals(15, result.get(Constants.CORRECT));
        assertEquals(2, result.get(Constants.INCORRECT));
        assertEquals(true, result.get(Constants.PASS)); // Only one section passes
    }

    @Test
    void testWriteDataToDatabaseAndTriggerKafkaEvent() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        // Mock dependencies
        AssessmentRepository mockRepo = mock(AssessmentRepository.class);
        Producer mockProducer = mock(Producer.class);
        AssessmentUtilServiceV2 mockUtil = mock(AssessmentUtilServiceV2.class);
        CbExtAssessmentServerProperties mockProps = mock(CbExtAssessmentServerProperties.class);

        // Inject mocks
        Field repoField = AssessmentServiceV2Impl.class.getDeclaredField("assessmentRepository");
        repoField.setAccessible(true);
        repoField.set(service, mockRepo);

        Field producerField = AssessmentServiceV2Impl.class.getDeclaredField("kafkaProducer");
        producerField.setAccessible(true);
        producerField.set(service, mockProducer);

        Field utilField = AssessmentServiceV2Impl.class.getDeclaredField("assessUtilServ");
        utilField.setAccessible(true);
        utilField.set(service, mockUtil);

        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, mockProps);

        // Prepare input data
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.USER_ID, "user1");

        Map<String, Object> questionSetFromAssessment = new HashMap<>();
        questionSetFromAssessment.put(Constants.START_TIME, Instant.now());

        Map<String, Object> result = new HashMap<>();
        result.put(Constants.OVERALL_RESULT, 80.0);

        when(mockUtil.parseStartTimeToInstant(any())).thenReturn(Instant.now());
        when(mockRepo.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any())).thenReturn(true);
        when(mockProps.getAssessmentSubmitTopic()).thenReturn("topic");

        // Call private method via reflection
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "writeDataToDatabaseAndTriggerKafkaEvent",
                Map.class, String.class, Map.class, Map.class, String.class
        );
        method.setAccessible(true);
        method.invoke(service, submitRequest, "user1", questionSetFromAssessment, result, "Assessment");

        // Verify interactions
        verify(mockRepo, times(1)).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
        verify(mockProducer, times(1)).push(eq("topic"), any());
    }

    @Test
    void testCalculateAssessmentRetakeCount() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        Map<String, Object> entry1 = new HashMap<>();
        entry1.put(Constants.SUBMIT_ASSESSMENT_RESPONSE, "response1");
        Map<String, Object> entry2 = new HashMap<>();
        entry2.put(Constants.SUBMIT_ASSESSMENT_RESPONSE, null);
        Map<String, Object> entry3 = new HashMap<>();
        entry3.put(Constants.SUBMIT_ASSESSMENT_RESPONSE, "response2");

        List<Map<String, Object>> userAssessmentData = Arrays.asList(entry1, entry2, entry3);

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "calculateAssessmentRetakeCount", List.class);
        method.setAccessible(true);

        int count = (int) method.invoke(service, userAssessmentData);

        assertEquals(2, count);
    }

    @Test
    void testCreateResponseMapWithProperStructure_WithResultMap() {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "section1");
        hierarchySection.put(Constants.OBJECT_TYPE, "Section");
        hierarchySection.put(Constants.PRIMARY_CATEGORY, "Section");
        hierarchySection.put(Constants.MINIMUM_PASS_PERCENTAGE, 60);

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(Constants.RESULT, 75.0);
        resultMap.put(Constants.TOTAL, 10);
        resultMap.put(Constants.BLANK, 2);
        resultMap.put(Constants.CORRECT, 7);
        resultMap.put(Constants.INCORRECT, 1);

        Map<String, Object> result = service.createResponseMapWithProperStructure(hierarchySection, resultMap);

        assertEquals("section1", result.get(Constants.IDENTIFIER));
        assertEquals("Section", result.get(Constants.OBJECT_TYPE));
        assertEquals("Section", result.get(Constants.PRIMARY_CATEGORY));
        assertEquals(60, result.get(Constants.PASS_PERCENTAGE));
        assertEquals(75.0, result.get(Constants.RESULT));
        assertEquals(10, result.get(Constants.TOTAL));
        assertEquals(2, result.get(Constants.BLANK));
        assertEquals(7, result.get(Constants.CORRECT));
        assertEquals(1, result.get(Constants.INCORRECT));
        assertEquals(true, result.get(Constants.PASS));
        assertEquals(75.0, result.get(Constants.OVERALL_RESULT));
    }

    @Test
    void testCreateResponseMapWithProperStructure_EmptyResultMap() {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        Map<String, Object> hierarchySection = new HashMap<>();
        hierarchySection.put(Constants.IDENTIFIER, "section2");
        hierarchySection.put(Constants.OBJECT_TYPE, "Section");
        hierarchySection.put(Constants.PRIMARY_CATEGORY, "Section");
        hierarchySection.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);
        hierarchySection.put(Constants.CHILDREN, Arrays.asList("q1", "q2", "q3"));

        Map<String, Object> result = service.createResponseMapWithProperStructure(hierarchySection, null);

        assertEquals("section2", result.get(Constants.IDENTIFIER));
        assertEquals("Section", result.get(Constants.OBJECT_TYPE));
        assertEquals("Section", result.get(Constants.PRIMARY_CATEGORY));
        assertEquals(50, result.get(Constants.PASS_PERCENTAGE));
        assertEquals(0.0, result.get(Constants.RESULT));
        assertEquals(3, result.get(Constants.TOTAL));
        assertEquals(3, result.get(Constants.BLANK));
        assertEquals(0, result.get(Constants.CORRECT));
        assertEquals(0, result.get(Constants.INCORRECT));
        assertEquals(false, result.get(Constants.PASS));
        assertEquals(0.0, result.get(Constants.OVERALL_RESULT));
    }

    @Test
    void testCalculateAssessmentFinalResults() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        Map<String, Object> assessmentLevelResult = new HashMap<>();
        assessmentLevelResult.put(Constants.RESULT, 85.0);
        assessmentLevelResult.put(Constants.TOTAL, 20);
        assessmentLevelResult.put(Constants.BLANK, 2);
        assessmentLevelResult.put(Constants.CORRECT, 17);
        assessmentLevelResult.put(Constants.PASS_PERCENTAGE, 60);
        assessmentLevelResult.put(Constants.INCORRECT, 1);

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "calculateAssessmentFinalResults", Map.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(service, assessmentLevelResult);

        assertNotNull(result);
        assertEquals(85.0, result.get(Constants.OVERALL_RESULT));
        assertEquals(20, result.get(Constants.TOTAL));
        assertEquals(2, result.get(Constants.BLANK));
        assertEquals(17, result.get(Constants.CORRECT));
        assertEquals(60, result.get(Constants.PASS_PERCENTAGE));
        assertEquals(1, result.get(Constants.INCORRECT));
        assertEquals(true, result.get(Constants.PASS));
        assertTrue(result.get(Constants.CHILDREN) instanceof List);
        assertEquals(assessmentLevelResult, ((List<?>) result.get(Constants.CHILDREN)).get(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadSectionLevelParams_PopulatesSectionDetailsCorrectly() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        CbExtAssessmentServerProperties mockProps = mock(CbExtAssessmentServerProperties.class);
        List<String> sectionParams = List.of(Constants.IDENTIFIER, Constants.MINIMUM_PASS_PERCENTAGE, Constants.MAX_QUESTIONS);
        when(mockProps.getAssessmentSectionParams()).thenReturn(sectionParams);

        // Inject mock
        try {
            Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
            propsField.setAccessible(true);
            propsField.set(service, mockProps);
        } catch (Exception e) {
            fail("Failed to inject mock serverProperties");
        }

        // Prepare input assessmentAllDetail
        Map<String, Object> section1 = new HashMap<>();
        section1.put(Constants.IDENTIFIER, "section1");
        section1.put(Constants.MINIMUM_PASS_PERCENTAGE, 60);
        section1.put(Constants.MAX_QUESTIONS, 2);
        Map<String, Object> q1 = new HashMap<>();
        q1.put(Constants.IDENTIFIER, "q1");
        Map<String, Object> q2 = new HashMap<>();
        q2.put(Constants.IDENTIFIER, "q2");
        section1.put(Constants.CHILDREN, List.of(q1, q2));

        Map<String, Object> assessmentAllDetail = new HashMap<>();
        assessmentAllDetail.put(Constants.CHILDREN, List.of(section1));

        Map<String, Object> assessmentFilteredDetail = new HashMap<>();
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "readSectionLevelParams", Map.class, Map.class);
        method.setAccessible(true);
        method.invoke(service, assessmentAllDetail, assessmentFilteredDetail);


        // Assertions
        assertTrue(assessmentFilteredDetail.containsKey(Constants.CHILDREN));
        List<Map<String, Object>> sectionResponse = (List<Map<String, Object>>) assessmentFilteredDetail.get(Constants.CHILDREN);
        assertEquals(1, sectionResponse.size());
        Map<String, Object> newSection = sectionResponse.get(0);
        assertEquals("section1", newSection.get(Constants.IDENTIFIER));
        assertEquals(60, newSection.get(Constants.MINIMUM_PASS_PERCENTAGE));
        assertTrue(newSection.containsKey(Constants.CHILD_NODES));
        List<String> childNodes = (List<String>) newSection.get(Constants.CHILD_NODES);
        assertEquals(2, childNodes.size());
        assertTrue(childNodes.contains("q1") && childNodes.contains("q2"));

        assertTrue(assessmentFilteredDetail.containsKey(Constants.CHILD_NODES));
        List<String> sectionIdList = (List<String>) assessmentFilteredDetail.get(Constants.CHILD_NODES);
        assertEquals(List.of("section1"), sectionIdList);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessmentLevelData_withValidParams() throws Exception {
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, "q1");

        Map<String, Object> section = new HashMap<>();
        section.put(Constants.CHILDREN, List.of(question));

        Map<String, Object> assessmentAllDetail = new HashMap<>();
        assessmentAllDetail.put(Constants.IDENTIFIER, "assess1");
        assessmentAllDetail.put(Constants.CHILDREN, List.of(section));

        when(serverProperties.getAssessmentLevelParams()).thenReturn(List.of(Constants.IDENTIFIER));
        when(serverProperties.getAssessmentSectionParams()).thenReturn(Collections.emptyList());

        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);
        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, serverProperties);

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("readAssessmentLevelData", Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(service, assessmentAllDetail);
        assertTrue(result.containsKey(Constants.IDENTIFIER));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessmentLevelData_withEmptyParams() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        // Inject mock serverProperties
        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, serverProperties);

        when(serverProperties.getAssessmentLevelParams()).thenReturn(Collections.emptyList());
        when(serverProperties.getAssessmentSectionParams()).thenReturn(Collections.emptyList());

        Map<String, Object> assessmentAllDetail = new HashMap<>();
        assessmentAllDetail.put(Constants.CHILDREN, Collections.emptyList()); // Prevent NPE

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("readAssessmentLevelData", Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(service, assessmentAllDetail);
        assertNotNull(result);
    }
    // submitAssessment(Map, String, boolean)
    @Test
    void testSubmitAssessment_withNullUserId() throws Exception {
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn(null);
        SBApiResponse response = assessmentServiceV2.submitAssessment(submitRequest, TOKEN, false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testSubmitAssessment_withEmptyAssessmentId() throws Exception {
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "");
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn(USER_ID);
        SBApiResponse response = assessmentServiceV2.submitAssessment(submitRequest, TOKEN, false);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // readAssessment(String, String)
    @Test
    void testReadAssessment_withInvalidToken() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn(null);
        SBApiResponse response = assessmentServiceV2.readAssessment("assess1", TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testReadAssessment_withException() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenThrow(new RuntimeException("error"));
        SBApiResponse response = assessmentServiceV2.readAssessment("assess1", TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // readQuestionList(Map, String)
    @Test
    void testReadQuestionList_withEmptyRequestBody() {
        Map<String, Object> requestBody = new HashMap<>();
        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testReadQuestionList_withInvalidToken() {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn(null);
        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testFetchReadHierarchyDetails_withException() throws Exception {
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        when(redisCacheMgr.getCache(anyString())).thenThrow(new RuntimeException("error"));
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "fetchReadHierarchyDetails", Map.class, String.class, String.class);
        method.setAccessible(true);
        String errMsg = (String) method.invoke(assessmentServiceV2, assessmentAllDetail, TOKEN, "assess1");
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, errMsg);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testValidateQuestionListAPI_validAndInvalidCases() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        // Inject mocks
        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, serverProperties);

        Field repoField = AssessmentServiceV2Impl.class.getDeclaredField("assessmentRepository");
        repoField.setAccessible(true);
        repoField.set(service, assessmentRepository);

        Field redisField = AssessmentServiceV2Impl.class.getDeclaredField("redisCacheMgr");
        redisField.setAccessible(true);
        redisField.set(service, redisCacheMgr);

        Field mapperField = AssessmentServiceV2Impl.class.getDeclaredField("mapper");
        mapperField.setAccessible(true);
        mapperField.set(service, mapper);

        Field accessTokenField = AssessmentServiceV2Impl.class.getDeclaredField("accessTokenValidator");
        accessTokenField.setAccessible(true);
        accessTokenField.set(service, accessTokenValidator);

        // Prepare input
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> search = new HashMap<>();
        search.put(Constants.IDENTIFIER, List.of("q1", "q2"));
        request.put(Constants.SEARCH, search);
        requestBody.put(Constants.REQUEST, request);

        List<String> identifierList = new ArrayList<>();

        // Mock dependencies
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        when(redisCacheMgr.getCache(anyString())).thenReturn("{\"primaryCategory\":\"Assessment\",\"identifier\":\"assess1\",\"children\":[]}");
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(Map.of(
                Constants.PRIMARY_CATEGORY, "Assessment",
                Constants.IDENTIFIER, "assess1",
                Constants.CHILDREN, List.of(Map.of(Constants.CHILD_NODES, List.of("q1", "q2")))
        ));
        lenient().when(assessmentRepository.fetchUserAssessmentDataFromDB(anyString(), anyString())).thenReturn(List.of(
                Map.of(Constants.ASSESSMENT_READ_RESPONSE, "{\"primaryCategory\":\"Assessment\",\"identifier\":\"assess1\",\"children\":[{\"childNodes\":[\"q1\",\"q2\"]}]}")
        ));
        // Call private method via reflection
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("validateQuestionListAPI", Map.class, String.class, List.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> result = (Map<String, String>) method.invoke(service, requestBody, "token", identifierList);

        assertTrue(result.containsKey(Constants.ERROR_MESSAGE));
        assertEquals("", result.get(Constants.ERROR_MESSAGE));
        // Negative case: missing token
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("");
        identifierList.clear();
        result = (Map<String, String>) method.invoke(service, requestBody, "token", identifierList);
        assertEquals(Constants.USER_ID_DOESNT_EXIST, result.get(Constants.ERROR_MESSAGE));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetQuestionIdList_variousCases() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);

        // Case 1: Valid request with identifiers
        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> search = new HashMap<>();
        search.put(Constants.IDENTIFIER, List.of("q1", "q2", "q3"));
        request.put(Constants.SEARCH, search);
        requestBody.put(Constants.REQUEST, request);

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("getQuestionIdList", Map.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(service, requestBody);
        assertEquals(List.of("q1", "q2", "q3"), result);

        // Case 2: Missing REQUEST key
        Map<String, Object> emptyRequestBody = new HashMap<>();
        result = (List<String>) method.invoke(service, emptyRequestBody);
        assertTrue(result.isEmpty());

        // Case 3: SEARCH key missing
        Map<String, Object> noSearchRequestBody = new HashMap<>();
        noSearchRequestBody.put(Constants.REQUEST, new HashMap<>());
        result = (List<String>) method.invoke(service, noSearchRequestBody);
        assertTrue(result.isEmpty());

        // Case 4: IDENTIFIER is empty list
        Map<String, Object> emptyIdentifierRequestBody = new HashMap<>();
        Map<String, Object> req = new HashMap<>();
        Map<String, Object> srch = new HashMap<>();
        srch.put(Constants.IDENTIFIER, Collections.emptyList());
        req.put(Constants.SEARCH, srch);
        emptyIdentifierRequestBody.put(Constants.REQUEST, req);
        result = (List<String>) method.invoke(service, emptyIdentifierRequestBody);
        assertTrue(result.isEmpty());
    }

    @Test
    void testSubmitAssessment_FailedSubmission() throws Exception {
        // Prepare mocks and inject them
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);
        Field repoField = AssessmentServiceV2Impl.class.getDeclaredField("assessmentRepository");
        repoField.setAccessible(true);
        repoField.set(service, assessmentRepository);

        Field utilField = AssessmentServiceV2Impl.class.getDeclaredField("assessUtilServ");
        utilField.setAccessible(true);
        utilField.set(service, assessUtilServ);

        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, serverProperties);

        Field redisField = AssessmentServiceV2Impl.class.getDeclaredField("redisCacheMgr");
        redisField.setAccessible(true);
        redisField.set(service, redisCacheMgr);

        Field mapperField = AssessmentServiceV2Impl.class.getDeclaredField("mapper");
        mapperField.setAccessible(true);
        mapperField.set(service, mapper);

        Field accessTokenField = AssessmentServiceV2Impl.class.getDeclaredField("accessTokenValidator");
        accessTokenField.setAccessible(true);
        accessTokenField.set(service, accessTokenValidator);

        // Prepare input data
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        children.add(section);
        submitRequest.put(Constants.CHILDREN, children);

        // Mock dependencies
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, children);
        lenient().when(serverProperties.getAssessmentLevelParams()).thenReturn(List.of(Constants.IDENTIFIER));
        lenient().when(serverProperties.getAssessmentSectionParams()).thenReturn(List.of(Constants.IDENTIFIER));
        lenient().when(assessUtilServ.validateQumlAssessment(
                ArgumentMatchers.<List<String>>any(),
                ArgumentMatchers.<List<Map<String, Object>>>any(),
                ArgumentMatchers.<Map<String, Object>>any()
        )).thenReturn(
                Map.of(Constants.RESULT, 80.0, Constants.TOTAL, 1, Constants.BLANK, 0, Constants.CORRECT, 1, Constants.INCORRECT, 0)
        );
        lenient().when(assessmentRepository.fetchUserAssessmentDataFromDB(anyString(), anyString())).thenReturn(List.of(
                Map.of(Constants.STATUS, Constants.NOT_SUBMITTED, Constants.START_TIME, Date.from(Instant.now()), Constants.ASSESSMENT_READ_RESPONSE_KEY, "{}")
        ));
        lenient().when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        lenient().when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("120");
        lenient().doNothing().when(redisCacheMgr).putCache(anyString(), any());
        lenient().when(assessmentRepository.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any())).thenReturn(true);
        lenient().when(serverProperties.getAssessmentSubmitTopic()).thenReturn("topic");
        lenient().doNothing().when(producer).push(anyString(), any());

        // Call method
        SBApiResponse response = service.submitAssessment(submitRequest, "token", false);

        // Assert
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // Empty assessment data in cache: should return empty string
    @Test
    void testFetchReadHierarchyDetails_withEmptyAssessmentData() throws Exception {
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        when(redisCacheMgr.getCache(anyString())).thenReturn("");
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "fetchReadHierarchyDetails", Map.class, String.class, String.class);
        method.setAccessible(true);
        String errMsg = (String) method.invoke(assessmentServiceV2, assessmentAllDetail, TOKEN, "assess1");
        assertEquals("Assessment hierarchy read failed, failed to process request", errMsg);
        assertTrue(assessmentAllDetail.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFetchReadHierarchyDetails_withCacheHit() throws Exception {
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        String assessmentJson = "{\"primaryCategory\":\"Assessment\"}";
        when(redisCacheMgr.getCache(anyString())).thenReturn(assessmentJson);
        when(mapper.readValue(anyString(), any(TypeReference.class)))
                .thenReturn(Map.of(Constants.PRIMARY_CATEGORY, "Assessment"));

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "fetchReadHierarchyDetails", Map.class, String.class, String.class);
        method.setAccessible(true);

        String errMsg = (String) method.invoke(assessmentServiceV2, assessmentAllDetail, TOKEN, "assess1");
        assertEquals("", errMsg);
        assertEquals("Assessment", assessmentAllDetail.get(Constants.PRIMARY_CATEGORY));
    }

    @Test
    void testReadAssessment_hierarchyFetchFails() {
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        when(redisCacheMgr.getCache(anyString())).thenReturn("");
        SBApiResponse response = assessmentServiceV2.readAssessment(ASSESSMENT_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Assessment hierarchy read failed"));
    }

    // +ve: PRACTICE_QUESTION_SET category
    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessment_practiceQuestionSet() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        String assessmentHierarchyJson = new ObjectMapper().writeValueAsString(assessmentHierarchy);
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + ASSESSMENT_ID)).thenReturn(assessmentHierarchyJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentHierarchy);
        SBApiResponse response = assessmentServiceV2.readAssessment(ASSESSMENT_ID, TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    // -ve: DB update fails on first read
    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessment_dbUpdateFails() throws Exception {
        when(serverProperties.getAssessmentLevelParams()).thenReturn(Collections.emptyList());
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        String assessmentHierarchyJson = new ObjectMapper().writeValueAsString(assessmentHierarchy);
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + ASSESSMENT_ID)).thenReturn(assessmentHierarchyJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentHierarchy);
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(new ArrayList<>());
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(false);
        SBApiResponse response = assessmentServiceV2.readAssessment(ASSESSMENT_ID, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains(Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED));
    }

    // +ve: Existing assessment, NOT_SUBMITTED, within time
    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessment_existingNotSubmittedWithinTime() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        String assessmentHierarchyJson = new ObjectMapper().writeValueAsString(assessmentHierarchy);
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + ASSESSMENT_ID)).thenReturn(assessmentHierarchyJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentHierarchy);
        Date endTime = Date.from(Instant.now().plusSeconds(300));
        Map<String, Object> dbData = new HashMap<>();
        dbData.put(Constants.STATUS, Constants.NOT_SUBMITTED);
        dbData.put(Constants.END_TIME, endTime);
        dbData.put(Constants.ASSESSMENT_READ_RESPONSE, assessmentHierarchyJson);
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(dbData));
        SBApiResponse response = assessmentServiceV2.readAssessment(ASSESSMENT_ID, TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    // +ve: Existing assessment, SUBMITTED or end time exceeded
    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessment_existingSubmittedOrEndTimeExceeded() throws Exception {
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, new ArrayList<>());
        String assessmentHierarchyJson = new ObjectMapper().writeValueAsString(assessmentHierarchy);
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + ASSESSMENT_ID)).thenReturn(assessmentHierarchyJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentHierarchy);
        Date endTime = Date.from(Instant.now().minusSeconds(300));
        Map<String, Object> dbData = new HashMap<>();
        dbData.put(Constants.STATUS, Constants.SUBMITTED);
        dbData.put(Constants.END_TIME, endTime);
        dbData.put(Constants.ASSESSMENT_READ_RESPONSE, assessmentHierarchyJson);
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(dbData));
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(true);
        SBApiResponse response = assessmentServiceV2.readAssessment(ASSESSMENT_ID, TOKEN);
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testWriteDataToDatabaseAndTriggerKafkaEvent_dbUpdateFails() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);
        AssessmentRepository mockRepo = mock(AssessmentRepository.class);
        Producer mockProducer = mock(Producer.class);
        AssessmentUtilServiceV2 mockUtil = mock(AssessmentUtilServiceV2.class);
        CbExtAssessmentServerProperties mockProps = mock(CbExtAssessmentServerProperties.class);

        Field repoField = AssessmentServiceV2Impl.class.getDeclaredField("assessmentRepository");
        repoField.setAccessible(true);
        repoField.set(service, mockRepo);
        Field producerField = AssessmentServiceV2Impl.class.getDeclaredField("kafkaProducer");
        producerField.setAccessible(true);
        producerField.set(service, mockProducer);
        Field utilField = AssessmentServiceV2Impl.class.getDeclaredField("assessUtilServ");
        utilField.setAccessible(true);
        utilField.set(service, mockUtil);
        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, mockProps);

        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.USER_ID, "user1");
        Map<String, Object> questionSetFromAssessment = new HashMap<>();
        questionSetFromAssessment.put(Constants.START_TIME, Instant.now());
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.OVERALL_RESULT, 80.0);

        when(mockUtil.parseStartTimeToInstant(any())).thenReturn(Instant.now());
        when(mockRepo.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any())).thenReturn(false);

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "writeDataToDatabaseAndTriggerKafkaEvent", Map.class, String.class, Map.class, Map.class, String.class);
        method.setAccessible(true);
        method.invoke(service, submitRequest, "user1", questionSetFromAssessment, result, "Assessment");

        verify(mockProducer, never()).push(anyString(), any());
    }

    // -ve: Null start time, should not update DB or push Kafka
    @Test
    void testWriteDataToDatabaseAndTriggerKafkaEvent_nullStartTime() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);
        AssessmentRepository mockRepo = mock(AssessmentRepository.class);
        Producer mockProducer = mock(Producer.class);
        AssessmentUtilServiceV2 mockUtil = mock(AssessmentUtilServiceV2.class);
        CbExtAssessmentServerProperties mockProps = mock(CbExtAssessmentServerProperties.class);

        Field repoField = AssessmentServiceV2Impl.class.getDeclaredField("assessmentRepository");
        repoField.setAccessible(true);
        repoField.set(service, mockRepo);
        Field producerField = AssessmentServiceV2Impl.class.getDeclaredField("kafkaProducer");
        producerField.setAccessible(true);
        producerField.set(service, mockProducer);
        Field utilField = AssessmentServiceV2Impl.class.getDeclaredField("assessUtilServ");
        utilField.setAccessible(true);
        utilField.set(service, mockUtil);
        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, mockProps);

        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.USER_ID, "user1");
        Map<String, Object> questionSetFromAssessment = new HashMap<>(); // No START_TIME
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.OVERALL_RESULT, 80.0);

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "writeDataToDatabaseAndTriggerKafkaEvent", Map.class, String.class, Map.class, Map.class, String.class);
        method.setAccessible(true);
        method.invoke(service, submitRequest, "user1", questionSetFromAssessment, result, "Assessment");

        verify(mockRepo, never()).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
        verify(mockProducer, never()).push(anyString(), any());
    }

    // -ve: Exception thrown by repository
    @Test
    void testWriteDataToDatabaseAndTriggerKafkaEvent_repoThrowsException() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);
        AssessmentRepository mockRepo = mock(AssessmentRepository.class);
        Producer mockProducer = mock(Producer.class);
        AssessmentUtilServiceV2 mockUtil = mock(AssessmentUtilServiceV2.class);
        CbExtAssessmentServerProperties mockProps = mock(CbExtAssessmentServerProperties.class);

        Field repoField = AssessmentServiceV2Impl.class.getDeclaredField("assessmentRepository");
        repoField.setAccessible(true);
        repoField.set(service, mockRepo);
        Field producerField = AssessmentServiceV2Impl.class.getDeclaredField("kafkaProducer");
        producerField.setAccessible(true);
        producerField.set(service, mockProducer);
        Field utilField = AssessmentServiceV2Impl.class.getDeclaredField("assessUtilServ");
        utilField.setAccessible(true);
        utilField.set(service, mockUtil);
        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, mockProps);

        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        submitRequest.put(Constants.USER_ID, "user1");
        Map<String, Object> questionSetFromAssessment = new HashMap<>();
        questionSetFromAssessment.put(Constants.START_TIME, Instant.now());
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.OVERALL_RESULT, 80.0);

        when(mockUtil.parseStartTimeToInstant(any())).thenReturn(Instant.now());
        when(mockRepo.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("DB error"));

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "writeDataToDatabaseAndTriggerKafkaEvent", Map.class, String.class, Map.class, Map.class, String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(service, submitRequest, "user1", questionSetFromAssessment, result, "Assessment"));
        verify(mockProducer, never()).push(anyString(), any());
    }

    @Test
    void testFetchReadHierarchyDetails_withEmptyReadHierarchyApiResponse() throws Exception {
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        when(redisCacheMgr.getCache(anyString())).thenReturn("");
        when(assessUtilServ.getReadHierarchyApiResponse(anyString(), anyString())).thenReturn(Collections.emptyMap());

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "fetchReadHierarchyDetails", Map.class, String.class, String.class);
        method.setAccessible(true);

        String errMsg = (String) method.invoke(assessmentServiceV2, assessmentAllDetail, TOKEN, ASSESSMENT_ID);
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, errMsg);
        assertTrue(assessmentAllDetail.isEmpty());
    }

    @Test
    void testFetchReadHierarchyDetails_withNonOkResponseCode() throws Exception {
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        when(redisCacheMgr.getCache(anyString())).thenReturn("");
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESPONSE_CODE, "ERROR");
        apiResponse.put(Constants.RESULT, Map.of(Constants.QUESTION_SET, Map.of(Constants.PRIMARY_CATEGORY, "Assessment")));
        when(assessUtilServ.getReadHierarchyApiResponse(anyString(), anyString())).thenReturn(apiResponse);

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "fetchReadHierarchyDetails", Map.class, String.class, String.class);
        method.setAccessible(true);

        String errMsg = (String) method.invoke(assessmentServiceV2, assessmentAllDetail, TOKEN, ASSESSMENT_ID);
        assertEquals(Constants.ASSESSMENT_HIERARCHY_READ_FAILED, errMsg);
        assertTrue(assessmentAllDetail.isEmpty());
    }

    @Test
    void testFetchReadHierarchyDetails_withMissingResultOrQuestionSet() throws Exception {
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        when(redisCacheMgr.getCache(anyString())).thenReturn("");
        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        apiResponse.put(Constants.RESULT, Collections.emptyMap()); // Missing QUESTION_SET
        when(assessUtilServ.getReadHierarchyApiResponse(anyString(), anyString())).thenReturn(apiResponse);

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "fetchReadHierarchyDetails", Map.class, String.class, String.class);
        method.setAccessible(true);

        String errMsg = (String) method.invoke(assessmentServiceV2, assessmentAllDetail, TOKEN, ASSESSMENT_ID);
        // Should not throw, but assessmentAllDetail remains empty
        assertEquals("Assessment hierarchy read failed, failed to process request", errMsg);
        assertTrue(assessmentAllDetail.isEmpty());
    }

    @Test
    void testSubmitAssessment_withAssessmentLevelScoreCutoff() throws Exception {
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        section.put(Constants.MINIMUM_PASS_PERCENTAGE, -1);
        children.add(section);
        submitRequest.put(Constants.CHILDREN, children);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        // Dummy API response for hierarchy with scoreCutOffType
        Map<String, Object> questionSet = new HashMap<>();
        questionSet.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        questionSet.put(Constants.IDENTIFIER, "assess1");
        questionSet.put(Constants.CHILDREN, children);
        questionSet.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);


        Map<String, Object> result = new HashMap<>();
        result.put(Constants.QUESTION_SET, questionSet);

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        apiResponse.put(Constants.RESULT, result);

        when(assessUtilServ.getReadHierarchyApiResponse(anyString(), anyString())).thenReturn(apiResponse);

        // Mock required service calls
        lenient().when(serverProperties.getAssessmentLevelParams()).thenReturn(List.of(Constants.IDENTIFIER));
        lenient().when(serverProperties.getAssessmentSectionParams()).thenReturn(List.of(Constants.IDENTIFIER));
        lenient().when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        lenient().when(assessUtilServ.validateQumlAssessment(any(), any(), any())).thenReturn(new HashMap<>());
        lenient().when(assessUtilServ.readQListfromCache(any(), any(), anyBoolean(), anyString())).thenReturn(new HashMap<>());

        SBApiResponse response = assessmentServiceV2.submitAssessment(submitRequest, "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testSubmitAssessment_withSectionLevelScoreCutoff() throws Exception {
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        section.put(Constants.MINIMUM_PASS_PERCENTAGE, -1);
        children.add(section);
        submitRequest.put(Constants.CHILDREN, children);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        // Dummy API response for hierarchy with scoreCutOffType
        Map<String, Object> questionSet = new HashMap<>();
        questionSet.put(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET);
        questionSet.put(Constants.IDENTIFIER, "assess1");
        questionSet.put(Constants.CHILDREN, children);
        questionSet.put(Constants.SCORE_CUTOFF_TYPE, Constants.SECTION_LEVEL_SCORE_CUTOFF);


        Map<String, Object> result = new HashMap<>();
        result.put(Constants.QUESTION_SET, questionSet);

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        apiResponse.put(Constants.RESULT, result);

        when(assessUtilServ.getReadHierarchyApiResponse(anyString(), anyString())).thenReturn(apiResponse);

        // Mock required service calls
        lenient().when(serverProperties.getAssessmentLevelParams()).thenReturn(List.of(Constants.IDENTIFIER));
        lenient().when(serverProperties.getAssessmentSectionParams()).thenReturn(List.of(Constants.IDENTIFIER));
        lenient().when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        lenient().when(assessUtilServ.validateQumlAssessment(any(), any(), any())).thenReturn(new HashMap<>());
        lenient().when(assessUtilServ.readQListfromCache(any(), any(), anyBoolean(), anyString())).thenReturn(new HashMap<>());

        SBApiResponse response = assessmentServiceV2.submitAssessment(submitRequest, "token", false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    // Question set from DB/cache is null
    @Test
    void testSubmitAssessment_questionSetFromDbIsNull() throws Exception {
        AssessmentServiceV2Impl service = new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository, redisCacheMgr, outboundRequestHandlerService, mapper, accessTokenValidator);
        Field repoField = AssessmentServiceV2Impl.class.getDeclaredField("assessmentRepository");
        repoField.setAccessible(true);
        repoField.set(service, assessmentRepository);
        Field utilField = AssessmentServiceV2Impl.class.getDeclaredField("assessUtilServ");
        utilField.setAccessible(true);
        utilField.set(service, assessUtilServ);
        Field propsField = AssessmentServiceV2Impl.class.getDeclaredField("serverProperties");
        propsField.setAccessible(true);
        propsField.set(service, serverProperties);
        Field redisField = AssessmentServiceV2Impl.class.getDeclaredField("redisCacheMgr");
        redisField.setAccessible(true);
        redisField.set(service, redisCacheMgr);
        Field mapperField = AssessmentServiceV2Impl.class.getDeclaredField("mapper");
        mapperField.setAccessible(true);
        mapperField.set(service, mapper);
        Field accessTokenField = AssessmentServiceV2Impl.class.getDeclaredField("accessTokenValidator");
        accessTokenField.setAccessible(true);
        accessTokenField.set(service, accessTokenValidator);

        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.IDENTIFIER, "assess1");
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILDREN, List.of(Map.of(Constants.IDENTIFIER, "q1")));
        children.add(section);
        submitRequest.put(Constants.CHILDREN, children);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        assessmentHierarchy.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentHierarchy.put(Constants.SCORE_CUTOFF_TYPE, Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF);
        assessmentHierarchy.put(Constants.EXPECTED_DURATION, 120);
        assessmentHierarchy.put(Constants.CHILDREN, children);
        lenient().when(serverProperties.getAssessmentLevelParams()).thenReturn(List.of(Constants.IDENTIFIER));
        lenient().when(serverProperties.getAssessmentSectionParams()).thenReturn(List.of(Constants.IDENTIFIER));
        lenient().when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        lenient().when(assessmentRepository.fetchUserAssessmentDataFromDB(anyString(), anyString())).thenReturn(new ArrayList<>());

        SBApiResponse response = service.submitAssessment(submitRequest, "token", false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void testReadQuestionList_exceptionThrown() {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        lenient().when(redisCacheMgr.mget(anyList())).thenThrow(new RuntimeException("Redis error"));

        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Identifier List is Empty"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadQuestionList_cacheHitReturnsAssessmentData() throws Exception {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> search = new HashMap<>();
        search.put(Constants.IDENTIFIER, List.of("q1"));
        request.put(Constants.SEARCH, search);
        requestBody.put(Constants.REQUEST, request);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        // Add a section with children and childNodes
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.IDENTIFIER, "section1");
        section.put(Constants.CHILD_NODES, List.of("q1"));
        List<Map<String, Object>> childrenSections = List.of(section);

        // Update JSON to include childNodes
        String assessmentDataJson = "{\"primaryCategory\":\"Assessment\",\"identifier\":\"assess1\",\"children\":[{\"identifier\":\"section1\",\"children\":[{\"identifier\":\"q1\"}],\"childNodes\":[\"q1\"]}]}";
        Map<String, Object> assessmentData = new HashMap<>();
        assessmentData.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentData.put(Constants.IDENTIFIER, "assess1");
        assessmentData.put(Constants.CHILDREN, childrenSections);

        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + "assess1")).thenReturn(assessmentDataJson);

        Map<String, Object> dbData = new HashMap<>();
        dbData.put(Constants.ASSESSMENT_READ_RESPONSE, assessmentDataJson);
        List<Map<String, Object>> existingDataList = List.of(dbData);
        when(assessmentRepository.fetchUserAssessmentDataFromDB("user1", "assess1")).thenReturn(existingDataList);

        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentData);

        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void testReadQuestionList_identifierListEmpty() {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        // Simulate validateQuestionListAPI returning empty identifier list
        AssessmentServiceV2Impl service = spy(assessmentServiceV2);
        SBApiResponse response = service.readQuestionList(requestBody, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.IDENTIFIER_LIST_IS_EMPTY, response.getParams().getErrmsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadQuestionList_allQuestionsMissingInCacheAndDb() throws JsonProcessingException {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> search = new HashMap<>();
        search.put(Constants.IDENTIFIER, List.of("q1", "q2"));
        request.put(Constants.SEARCH, search);
        requestBody.put(Constants.REQUEST, request);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        // Mock assessment hierarchy in cache
        String assessmentDataJson = "{\"primaryCategory\":\"Assessment\",\"identifier\":\"assess1\",\"children\":[{\"identifier\":\"section1\",\"children\":[{\"identifier\":\"q1\"},{\"identifier\":\"q2\"}],\"childNodes\":[\"q1\",\"q2\"]}]}";
        Map<String, Object> assessmentData = new HashMap<>();
        assessmentData.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentData.put(Constants.IDENTIFIER, "assess1");
        assessmentData.put(Constants.CHILDREN, List.of(
                Map.of(Constants.IDENTIFIER, "section1", Constants.CHILD_NODES, List.of("q1", "q2"))
        ));

        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + "assess1")).thenReturn(assessmentDataJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentData);

        // Simulate cache miss for both questions
        List<String> identifierList = List.of("q1", "q2");
        List<String> cachedQuestions = List.of("");
        when(redisCacheMgr.mget(identifierList)).thenReturn(cachedQuestions);

        // Simulate DB miss for both questions
        when(assessUtilServ.readQuestionDetails(anyList())).thenReturn(Collections.emptyList());

        // Simulate DB assessment data
        Map<String, Object> dbData = new HashMap<>();
        dbData.put(Constants.ASSESSMENT_READ_RESPONSE, assessmentDataJson);
        List<Map<String, Object>> existingDataList = List.of(dbData);
        when(assessmentRepository.fetchUserAssessmentDataFromDB("user1", "assess1")).thenReturn(existingDataList);

        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.FAILED_TO_GET_QUESTION_DETAILS, response.getParams().getErrmsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadQuestionList_partialQuestionsFound() throws Exception {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> search = new HashMap<>();
        search.put(Constants.IDENTIFIER, List.of("q1", "q2"));
        request.put(Constants.SEARCH, search);
        requestBody.put(Constants.REQUEST, request);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");

        // Mock assessment hierarchy in cache
        String assessmentDataJson = "{\"primaryCategory\":\"Assessment\",\"identifier\":\"assess1\",\"children\":[{\"identifier\":\"section1\",\"children\":[{\"identifier\":\"q1\"},{\"identifier\":\"q2\"}],\"childNodes\":[\"q1\",\"q2\"]}]}";
        Map<String, Object> assessmentData = new HashMap<>();
        assessmentData.put(Constants.PRIMARY_CATEGORY, "Assessment");
        assessmentData.put(Constants.IDENTIFIER, "assess1");
        assessmentData.put(Constants.CHILDREN, List.of(
                Map.of(Constants.IDENTIFIER, "section1", Constants.CHILD_NODES, List.of("q1", "q2"))
        ));

        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + "assess1")).thenReturn(assessmentDataJson);
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(assessmentData);

        // Simulate cache hit for q1, miss for q2
        List<String> identifierList = List.of("q1", "q2");
        List<String> cachedQuestions = List.of("{\"identifier\":\"q1\"}");
        when(redisCacheMgr.mget(identifierList)).thenReturn(cachedQuestions);

        // Simulate DB hit for q2, but question is empty
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.RESULT, Map.of(Constants.QUESTIONS, List.of(Collections.emptyMap())));
        List<Map<String, Object>> newQuestionList = List.of(questionMap);
        lenient().when(assessUtilServ.readQuestionDetails(anyList())).thenReturn(newQuestionList);

        // Simulate DB assessment data
        Map<String, Object> dbData = new HashMap<>();
        dbData.put(Constants.ASSESSMENT_READ_RESPONSE, assessmentDataJson);
        List<Map<String, Object>> existingDataList = List.of(dbData);
        when(assessmentRepository.fetchUserAssessmentDataFromDB("user1", "assess1")).thenReturn(existingDataList);

        // filterQuestionMapDetail returns a non-empty map for q1
        when(assessUtilServ.filterQuestionMapDetail(any(), anyString(), anyBoolean())).thenReturn(Map.of(Constants.IDENTIFIER, "q1"));

        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);

        // Should not return questions, status should be success but no questions in result
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertFalse(response.getResult().containsKey(Constants.QUESTIONS));
    }

    @Test
    void testReadQuestionList_redisThrowsException() {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.ASSESSMENT_ID_KEY, "assess1");
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> search = new HashMap<>();
        search.put(Constants.IDENTIFIER, List.of("q1"));
        request.put(Constants.SEARCH, search);
        requestBody.put(Constants.REQUEST, request);

        when(accessTokenValidator.fetchUserIdFromAccessToken(anyString())).thenReturn("user1");
        lenient().when(redisCacheMgr.mget(anyList())).thenThrow(new RuntimeException("Redis error"));

        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().contains("Assessment hierarchy read failed, failed to process request"));
    }

    // ---------------------------------------------------------------------------------------------
    // Additional coverage: flows exercised with a real ObjectMapper so JSON round-trips are genuine
    // ---------------------------------------------------------------------------------------------

    private static final ObjectMapper REAL_MAPPER = new ObjectMapper();
    private static final String SUBMIT_ID = "assess1";

    private AssessmentServiceV2Impl realMapperService() {
        return new AssessmentServiceV2Impl(assessUtilServ, serverProperties, producer, assessmentRepository,
                redisCacheMgr, outboundRequestHandlerService, REAL_MAPPER, accessTokenValidator);
    }

    private static String json(Object value) throws JsonProcessingException {
        return REAL_MAPPER.writeValueAsString(value);
    }

    private static Map<String, Object> mapOf(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private void stubHierarchyInCache(String assessmentId, Map<String, Object> hierarchy) throws JsonProcessingException {
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + assessmentId)).thenReturn(json(hierarchy));
    }

    private Map<String, Object> simpleHierarchy(String primaryCategory) {
        return mapOf(Constants.IDENTIFIER, ASSESSMENT_ID, Constants.PRIMARY_CATEGORY, primaryCategory,
                Constants.EXPECTED_DURATION, 120, Constants.CHILDREN, new ArrayList<>());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessment_resumeFromUserCache_withInstantEndTime() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(ASSESSMENT_ID, simpleHierarchy("Assessment"));
        Map<String, Object> dbData = mapOf(Constants.STATUS, Constants.NOT_SUBMITTED,
                Constants.END_TIME, Instant.now().plusSeconds(600));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(dbData));
        when(redisCacheMgr.getCache(Constants.USER_ASSESS_REQ + ASSESSMENT_ID + "_" + TOKEN))
                .thenReturn("{\"identifier\":\"assess123\",\"cached\":true}");

        SBApiResponse response = service.readAssessment(ASSESSMENT_ID, TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        Map<String, Object> questionSet = (Map<String, Object>) response.getResult().get(Constants.QUESTION_SET);
        assertEquals(Boolean.TRUE, questionSet.get("cached"));
        assertEquals(ASSESSMENT_ID, questionSet.get(Constants.IDENTIFIER));
        verify(assessmentRepository, never()).addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any());
    }

    @Test
    void testReadAssessment_submittedWithinWindow_restartFailsToPersist() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(ASSESSMENT_ID, simpleHierarchy("Assessment"));
        Map<String, Object> dbData = mapOf(Constants.STATUS, Constants.SUBMITTED,
                Constants.END_TIME, Date.from(Instant.now().plusSeconds(600)));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(dbData));
        when(assessmentRepository.addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any())).thenReturn(false);

        SBApiResponse response = service.readAssessment(ASSESSMENT_ID, TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED, response.getParams().getErrmsg());
        assertTrue(response.getResult().containsKey(Constants.QUESTION_SET));
        verify(assessmentRepository).addUserAssesmentDataToDB(eq(USER_ID), eq(ASSESSMENT_ID), any(), any(), any(),
                eq(Constants.NOT_SUBMITTED));
        verify(redisCacheMgr).putCache(eq(Constants.USER_ASSESS_REQ + ASSESSMENT_ID + "_" + TOKEN), any());
    }

    @Test
    void testReadAssessment_unknownStatusWithinWindow_returnsNoQuestionSet() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(ASSESSMENT_ID, simpleHierarchy("Assessment"));
        Map<String, Object> dbData = mapOf(Constants.STATUS, "IN PROGRESS",
                Constants.END_TIME, Date.from(Instant.now().plusSeconds(600)));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(dbData));

        SBApiResponse response = service.readAssessment(ASSESSMENT_ID, TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertFalse(response.getResult().containsKey(Constants.QUESTION_SET));
        verify(assessmentRepository, never()).addUserAssesmentDataToDB(any(), any(), any(), any(), any(), any());
    }

    // ----- readQuestionList -----

    private Map<String, Object> questionListRequest(List<String> ids) {
        Map<String, Object> search = mapOf(Constants.IDENTIFIER, ids);
        Map<String, Object> request = mapOf(Constants.SEARCH, search);
        return mapOf(Constants.REQUEST, request, Constants.ASSESSMENT_ID_KEY, SUBMIT_ID);
    }

    private Map<String, Object> userAssessment(String identifier, String primaryCategory, List<String> childNodes) {
        return mapOf(Constants.IDENTIFIER, identifier, Constants.PRIMARY_CATEGORY, primaryCategory,
                Constants.CHILDREN, List.of(mapOf(Constants.IDENTIFIER, "s1", Constants.CHILD_NODES, childNodes)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadQuestionList_allQuestionsResolved_fromCacheAndService() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, mapOf(Constants.PRIMARY_CATEGORY, "Assessment"));
        when(redisCacheMgr.getCache(Constants.USER_ASSESS_REQ + SUBMIT_ID + "_" + TOKEN))
                .thenReturn(json(userAssessment(SUBMIT_ID, "Assessment", List.of("q1", "q2"))));
        when(redisCacheMgr.mget(List.of("q1", "q2"))).thenReturn(Arrays.asList("{\"identifier\":\"q1\"}", ""));

        Map<String, Object> q2 = mapOf(Constants.IDENTIFIER, "q2");
        List<Map<String, Object>> fetched = List.of(
                new HashMap<>(),
                mapOf(Constants.RESULT, mapOf(Constants.QUESTIONS, new ArrayList<>())),
                mapOf(Constants.RESULT, mapOf(Constants.QUESTIONS, List.of(new HashMap<>(), q2))));
        when(assessUtilServ.readQuestionDetails(List.of("q2"))).thenReturn(fetched);
        when(assessUtilServ.filterQuestionMapDetail(any(), eq("Assessment"), eq(true)))
                .thenAnswer(inv -> inv.getArgument(0));

        SBApiResponse response = service.readQuestionList(questionListRequest(List.of("q1", "q2")), TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        List<Map<String, Object>> questions = (List<Map<String, Object>>) response.getResult().get(Constants.QUESTIONS);
        assertEquals(2, questions.size());
        assertEquals("q1", questions.get(0).get(Constants.IDENTIFIER));
        assertEquals("q2", questions.get(1).get(Constants.IDENTIFIER));
        verify(redisCacheMgr).putCache(Constants.QUESTION_ID + "q2", q2);
    }

    @Test
    void testReadQuestionList_invalidCachedQuestionJson_returnsException() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, mapOf(Constants.PRIMARY_CATEGORY, "Assessment"));
        when(redisCacheMgr.getCache(Constants.USER_ASSESS_REQ + SUBMIT_ID + "_" + TOKEN))
                .thenReturn(json(userAssessment(SUBMIT_ID, "Assessment", List.of("q1"))));
        when(redisCacheMgr.mget(List.of("q1"))).thenReturn(List.of("not-a-json"));

        SBApiResponse response = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().startsWith("Failed to fetch the question list. Exception:"));
        assertFalse(response.getResult().containsKey(Constants.QUESTIONS));
    }

    @Test
    void testReadQuestionList_practiceSetWithoutUserCopy_fails() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, mapOf(Constants.PRIMARY_CATEGORY, Constants.PRACTICE_QUESTION_SET));

        SBApiResponse response = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertTrue(response.getParams().getErrmsg().startsWith("Failed to fetch the question list"));
        verify(assessmentRepository, never()).fetchUserAssessmentDataFromDB(anyString(), anyString());
    }

    @Test
    void testReadQuestionList_noUserAssessmentData() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, mapOf(Constants.PRIMARY_CATEGORY, "Assessment"));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID)).thenReturn(new ArrayList<>());

        SBApiResponse response = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, response.getParams().getErrmsg());
    }

    @Test
    void testReadQuestionList_assessmentIdMismatch() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, mapOf(Constants.PRIMARY_CATEGORY, "Assessment"));
        when(redisCacheMgr.getCache(Constants.USER_ASSESS_REQ + SUBMIT_ID + "_" + TOKEN))
                .thenReturn(json(userAssessment("otherAssessment", "Assessment", List.of("q1"))));

        SBApiResponse response = service.readQuestionList(questionListRequest(List.of("q1")), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_ID_INVALID, response.getParams().getErrmsg());
        verify(redisCacheMgr, never()).mget(anyList());
    }

    @Test
    void testReadQuestionList_questionIdsNotInAssessment() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, mapOf(Constants.PRIMARY_CATEGORY, "Assessment"));
        when(redisCacheMgr.getCache(Constants.USER_ASSESS_REQ + SUBMIT_ID + "_" + TOKEN))
                .thenReturn(json(userAssessment(SUBMIT_ID, "Assessment", List.of("q1"))));

        SBApiResponse response = service.readQuestionList(questionListRequest(List.of("q1", "q9")), TOKEN);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.THE_QUESTIONS_IDS_PROVIDED_DONT_MATCH, response.getParams().getErrmsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetQuestionIdList_malformedRequests() throws Exception {
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("getQuestionIdList", Map.class);
        method.setAccessible(true);

        // request present but without a search block
        List<String> result = (List<String>) method.invoke(assessmentServiceV2,
                mapOf(Constants.REQUEST, mapOf("filters", "x")));
        assertTrue(result.isEmpty());
        // empty search block
        result = (List<String>) method.invoke(assessmentServiceV2,
                mapOf(Constants.REQUEST, mapOf(Constants.SEARCH, new HashMap<>())));
        assertTrue(result.isEmpty());
        // search block without identifier
        result = (List<String>) method.invoke(assessmentServiceV2,
                mapOf(Constants.REQUEST, mapOf(Constants.SEARCH, mapOf("name", "x"))));
        assertTrue(result.isEmpty());
        // request of the wrong type triggers the exception handler
        result = (List<String>) method.invoke(assessmentServiceV2, mapOf(Constants.REQUEST, "not-a-map"));
        assertTrue(result.isEmpty());
    }

    // ----- submitAssessment -----

    private Map<String, Object> hierarchySection(String id) {
        return mapOf(Constants.IDENTIFIER, id, Constants.MINIMUM_PASS_PERCENTAGE, 50,
                Constants.OBJECT_TYPE, "QuestionSet", Constants.PRIMARY_CATEGORY, "Section");
    }

    private Map<String, Object> submitHierarchy(String primaryCategory, String cutOff, List<String> sectionIds) {
        List<Map<String, Object>> sections = new ArrayList<>();
        for (String id : sectionIds) {
            sections.add(hierarchySection(id));
        }
        return mapOf(Constants.IDENTIFIER, SUBMIT_ID, Constants.PRIMARY_CATEGORY, primaryCategory,
                Constants.SCORE_CUTOFF_TYPE, cutOff, Constants.EXPECTED_DURATION, 120, Constants.CHILDREN, sections);
    }

    private Map<String, Object> submitRequest(List<Map<String, Object>> sections) {
        return mapOf(Constants.IDENTIFIER, SUBMIT_ID, Constants.CHILDREN, sections);
    }

    private Map<String, Object> submittedSection(String id, String... questionIds) {
        Map<String, Object> section = mapOf(Constants.IDENTIFIER, id);
        if (questionIds.length > 0) {
            List<Map<String, Object>> questions = new ArrayList<>();
            for (String q : questionIds) {
                questions.add(mapOf(Constants.IDENTIFIER, q));
            }
            section.put(Constants.CHILDREN, questions);
        }
        return section;
    }

    private Map<String, Object> dbRecord(String status, Date startTime, String readResponse) {
        return mapOf(Constants.STATUS, status, Constants.START_TIME, startTime,
                Constants.ASSESSMENT_READ_RESPONSE_KEY, readResponse);
    }

    private String storedQuestionSet() throws JsonProcessingException {
        return json(mapOf(Constants.CHILDREN, List.of(
                mapOf(Constants.IDENTIFIER, "s1", Constants.CHILD_NODES, List.of("q1")),
                mapOf(Constants.IDENTIFIER, "s2", Constants.CHILD_NODES, List.of("q2")))));
    }

    private SBApiResponse submitWithDbRecord(Map<String, Object> hierarchy, Map<String, Object> request,
                                            Map<String, Object> submittedRecord) throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, hierarchy);
        lenient().when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("120");
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID)).thenReturn(List.of(submittedRecord));
        return service.submitAssessment(request, TOKEN, false);
    }

    @Test
    void testSubmitAssessment_emptyHierarchy() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + SUBMIT_ID)).thenReturn("{}");

        SBApiResponse response = service.submitAssessment(submitRequest(new ArrayList<>()), TOKEN, false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.READ_ASSESSMENT_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_noUserAssessmentRecord() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID)).thenReturn(new ArrayList<>());

        SBApiResponse response = service.submitAssessment(submitRequest(new ArrayList<>()), TOKEN, false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_alreadySubmitted() throws Exception {
        SBApiResponse response = submitWithDbRecord(
                submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")),
                submitRequest(new ArrayList<>()), dbRecord(Constants.SUBMITTED, new Date(), storedQuestionSet()));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_ALREADY_SUBMITTED, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_missingStartTime() throws Exception {
        SBApiResponse response = submitWithDbRecord(
                submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")),
                submitRequest(new ArrayList<>()), dbRecord(Constants.NOT_SUBMITTED, null, storedQuestionSet()));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.READ_ASSESSMENT_START_TIME_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_expired() throws Exception {
        Date longAgo = Date.from(Instant.now().minusSeconds(24 * 3600));
        SBApiResponse response = submitWithDbRecord(
                submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")),
                submitRequest(new ArrayList<>()), dbRecord(Constants.NOT_SUBMITTED, longAgo, storedQuestionSet()));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_SUBMIT_EXPIRED, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_defaultsSubmissionDuration_andRejectsUnknownSection() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")));
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("", "120");
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID))
                .thenReturn(List.of(dbRecord(Constants.NOT_SUBMITTED, new Date(), storedQuestionSet())));

        SBApiResponse response = service.submitAssessment(
                submitRequest(List.of(submittedSection("unknownSection", "q1"))), TOKEN, false);

        verify(serverProperties).setUserAssessmentSubmissionDuration("120");
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.WRONG_SECTION_DETAILS, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_blankStoredQuestionSet() throws Exception {
        SBApiResponse response = submitWithDbRecord(
                submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")),
                submitRequest(List.of(submittedSection("s1", "q1"))),
                dbRecord(Constants.NOT_SUBMITTED, new Date(), " "));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_questionNotInStoredSet() throws Exception {
        SBApiResponse response = submitWithDbRecord(
                submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")),
                submitRequest(List.of(submittedSection("s1", "q1", "q99"))),
                dbRecord(Constants.NOT_SUBMITTED, new Date(), storedQuestionSet()));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_SUBMIT_INVALID_QUESTION, response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_nullStoredQuestionSet_sectionLevel() throws Exception {
        SBApiResponse response = submitWithDbRecord(
                submitHierarchy("Assessment", Constants.SECTION_LEVEL_SCORE_CUTOFF, List.of("s1")),
                submitRequest(List.of(submittedSection("s1", "q1"))),
                dbRecord(Constants.NOT_SUBMITTED, new Date(), "null"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Question Set From The Database returns Null", response.getParams().getErrmsg());
        assertTrue(response.getResult().isEmpty());
        verify(assessUtilServ, never()).validateQumlAssessment(any(), any(), any());
        verify(producer, never()).push(anyString(), any());
    }

    @Test
    void testSubmitAssessment_storedQuestionSetWithoutSections() throws Exception {
        SBApiResponse response = submitWithDbRecord(
                submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")),
                submitRequest(List.of(submittedSection("s1", "q1"))),
                dbRecord(Constants.NOT_SUBMITTED, new Date(), "{}"));

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Question Set From The Database returns Null", response.getParams().getErrmsg());
    }

    @Test
    void testSubmitAssessment_storedRecordMissingDuringQuestionValidation() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")));
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("120");
        List<Map<String, Object>> records = List.of(dbRecord(Constants.NOT_SUBMITTED, new Date(), storedQuestionSet()));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID))
                .thenReturn(records, new ArrayList<>());

        SBApiResponse response = service.submitAssessment(
                submitRequest(List.of(submittedSection("s1", "q1"))), TOKEN, false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED, response.getParams().getErrmsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testBuildSubmitEvent_competencyAssessmentWithoutCompetencies() throws Exception {
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("buildSubmitEvent", Map.class, Map.class,
                String.class);
        method.setAccessible(true);
        Map<String, Object> result = mapOf(Constants.OVERALL_RESULT, 75.0);

        // key absent
        Map<String, Object> event = (Map<String, Object>) method.invoke(assessmentServiceV2,
                mapOf(Constants.IDENTIFIER, SUBMIT_ID), result, "Competency Assessment");
        assertFalse(event.containsKey(Constants.COMPETENCY));
        assertEquals(SUBMIT_ID, event.get(Constants.CONTENT_ID_KEY));
        assertEquals(75.0, event.get(Constants.TOTAL_SCORE));

        // key present but null
        event = (Map<String, Object>) method.invoke(assessmentServiceV2,
                mapOf(Constants.IDENTIFIER, SUBMIT_ID, Constants.COMPETENCIES_V3, null), result, "Competency Assessment");
        assertFalse(event.containsKey(Constants.COMPETENCY));
        assertEquals("Competency Assessment", event.get(Constants.PRIMARY_CATEGORY));
    }

    @Test
    void testSubmitAssessment_storedRecordDisappearsBeforeScoring() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")));
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("120");
        List<Map<String, Object>> records = List.of(dbRecord(Constants.NOT_SUBMITTED, new Date(), storedQuestionSet()));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID))
                .thenReturn(records, records, new ArrayList<>());

        SBApiResponse response = service.submitAssessment(
                submitRequest(List.of(submittedSection("s1", "q1"))), TOKEN, false);

        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Question Set From The Database returns Null", response.getParams().getErrmsg());
        verify(assessmentRepository, times(3)).fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSubmitAssessment_competencyAssessment_assessmentLevel_pushesCompetency() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, submitHierarchy("Competency Assessment", "AssessmentLevel", List.of("s1")));
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("120");
        when(serverProperties.getAssessmentSubmitTopic()).thenReturn("submit-topic");
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID))
                .thenReturn(List.of(dbRecord(Constants.NOT_SUBMITTED, new Date(), storedQuestionSet())));
        // user's in-progress question set is still cached
        Map<String, Object> cachedSet = mapOf(Constants.START_TIME, 1700000000000L, Constants.CHILDREN, List.of(
                mapOf(Constants.IDENTIFIER, "s1", Constants.CHILD_NODES, List.of("q1"))));
        when(redisCacheMgr.getCache(Constants.USER_ASSESS_REQ + SUBMIT_ID + "_" + TOKEN)).thenReturn(json(cachedSet));
        when(assessUtilServ.validateQumlAssessment(any(), any(), any())).thenReturn(
                mapOf(Constants.RESULT, 100.0, Constants.TOTAL, 1, Constants.BLANK, 0, Constants.CORRECT, 1,
                        Constants.INCORRECT, 0));
        Instant start = Instant.ofEpochMilli(1700000000000L);
        when(assessUtilServ.parseStartTimeToInstant(any())).thenReturn(start);
        when(assessmentRepository.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(true);

        Map<String, Object> request = submitRequest(List.of(submittedSection("s1", "q1")));
        request.put(Constants.COURSE_ID, "course1");
        request.put(Constants.BATCH_ID, "batch1");
        request.put(Constants.COMPETENCIES_V3, "[{\"id\":\"comp1\"},{\"id\":\"comp2\"}]");

        SBApiResponse response = service.submitAssessment(request, TOKEN, false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(100.0, response.getResult().get(Constants.OVERALL_RESULT));
        assertEquals(Boolean.TRUE, response.getResult().get(Constants.PASS));
        verify(assessUtilServ).validateQumlAssessment(eq(List.of("q1")), any(), any());
        verify(assessmentRepository).updateUserAssesmentDataToDB(eq(USER_ID), eq(SUBMIT_ID), eq(request), any(),
                eq(Constants.SUBMITTED), eq(start), isNull());
        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(producer).push(eq("submit-topic"), eventCaptor.capture());
        Map<String, Object> event = (Map<String, Object>) eventCaptor.getValue();
        assertEquals("course1", event.get(Constants.COURSE_ID));
        assertEquals("batch1", event.get(Constants.BATCH_ID));
        assertEquals(USER_ID, event.get(Constants.USER_ID));
        assertEquals(100.0, event.get(Constants.TOTAL_SCORE));
        assertEquals(Map.of("id", "comp1"), event.get(Constants.COMPETENCY));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSubmitAssessment_competencyAssessment_sectionLevel_emptyCompetencies() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, submitHierarchy("Competency Assessment", Constants.SECTION_LEVEL_SCORE_CUTOFF,
                List.of("s1", "s2")));
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("120");
        when(serverProperties.getAssessmentSubmitTopic()).thenReturn("submit-topic");
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID))
                .thenReturn(List.of(dbRecord(Constants.NOT_SUBMITTED, new Date(), storedQuestionSet())));
        // cached copy: s2 carries no child nodes, so it falls back to the previous section's ids
        Map<String, Object> cachedSet = mapOf(Constants.START_TIME, 1700000000000L, Constants.CHILDREN, List.of(
                mapOf(Constants.IDENTIFIER, "s1", Constants.CHILD_NODES, List.of("q1")),
                mapOf(Constants.IDENTIFIER, "s2")));
        when(redisCacheMgr.getCache(Constants.USER_ASSESS_REQ + SUBMIT_ID + "_" + TOKEN)).thenReturn(json(cachedSet));
        when(assessUtilServ.validateQumlAssessment(any(), any(), any())).thenReturn(
                mapOf(Constants.RESULT, 100.0, Constants.TOTAL, 1, Constants.BLANK, 0, Constants.CORRECT, 1,
                        Constants.INCORRECT, 0),
                mapOf(Constants.RESULT, 0.0, Constants.TOTAL, 1, Constants.BLANK, 1, Constants.CORRECT, 0,
                        Constants.INCORRECT, 0));
        when(assessUtilServ.parseStartTimeToInstant(any())).thenReturn(Instant.now());
        when(assessmentRepository.updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(true);

        Map<String, Object> request = submitRequest(List.of(submittedSection("s1", "q1"), submittedSection("s2")));
        request.put(Constants.COMPETENCIES_V3, "[]");

        SBApiResponse response = service.submitAssessment(request, TOKEN, false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(50.0, response.getResult().get(Constants.OVERALL_RESULT));
        assertEquals(Boolean.FALSE, response.getResult().get(Constants.PASS));
        assertEquals(2, response.getResult().get(Constants.TOTAL));
        assertEquals(1, response.getResult().get(Constants.BLANK));
        verify(assessUtilServ, times(2)).validateQumlAssessment(eq(List.of("q1")), any(), any());
        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(producer).push(eq("submit-topic"), eventCaptor.capture());
        Map<String, Object> event = (Map<String, Object>) eventCaptor.getValue();
        assertEquals("", event.get(Constants.COMPETENCY));
        assertEquals("", event.get(Constants.COURSE_ID));
        assertEquals("", event.get(Constants.BATCH_ID));
    }

    @Test
    void testSubmitAssessment_noSubmittedSections_fallsBackToCurrentIds() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, submitHierarchy("Assessment", Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF, List.of("s1")));
        when(serverProperties.getUserAssessmentSubmissionDuration()).thenReturn("120");
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, SUBMIT_ID))
                .thenReturn(List.of(dbRecord(Constants.NOT_SUBMITTED, new Date(), storedQuestionSet())));
        when(assessUtilServ.validateQumlAssessment(any(), any(), any())).thenReturn(
                mapOf(Constants.RESULT, 0.0, Constants.TOTAL, 1, Constants.BLANK, 1, Constants.CORRECT, 0,
                        Constants.INCORRECT, 0));

        SBApiResponse response = service.submitAssessment(submitRequest(new ArrayList<>()), TOKEN, false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(Boolean.FALSE, response.getResult().get(Constants.PASS));
        // no section matched, so the (empty) current question id list is used
        verify(assessUtilServ).validateQumlAssessment(eq(new ArrayList<>()), eq(new ArrayList<>()), any());
        // stored question set (from the DB) carries no start time, so nothing is persisted
        verify(assessmentRepository, never()).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void testSubmitAssessment_practiceSetWithoutCutOffType() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(SUBMIT_ID, submitHierarchy(Constants.PRACTICE_QUESTION_SET, "", List.of("s1")));

        SBApiResponse response = service.submitAssessment(
                submitRequest(List.of(submittedSection("s1", "q1"))), TOKEN, false);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertTrue(response.getResult().isEmpty());
        verify(assessUtilServ, never()).validateQumlAssessment(any(), any(), any());
        verify(assessmentRepository, never()).fetchUserAssessmentDataFromDB(anyString(), anyString());
    }

    // ----- private helpers via reflection -----

    @Test
    @SuppressWarnings("unchecked")
    void testCalculateAssessmentFinalResults_failAndException() throws Exception {
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("calculateAssessmentFinalResults", Map.class);
        method.setAccessible(true);

        Map<String, Object> failing = mapOf(Constants.RESULT, 40.0, Constants.PASS_PERCENTAGE, 60, Constants.TOTAL, 5);
        Map<String, Object> res = (Map<String, Object>) method.invoke(assessmentServiceV2, failing);
        assertEquals(Boolean.FALSE, res.get(Constants.PASS));
        assertEquals(40.0, res.get(Constants.OVERALL_RESULT));

        Map<String, Object> broken = mapOf(Constants.PASS_PERCENTAGE, 60);
        res = (Map<String, Object>) method.invoke(assessmentServiceV2, broken);
        assertFalse(res.containsKey(Constants.PASS));
        assertNull(res.get(Constants.OVERALL_RESULT));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCalculateSectionFinalResults_failingSectionAndException() throws Exception {
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("calculateSectionFinalResults", List.class);
        method.setAccessible(true);

        Map<String, Object> failing = mapOf(Constants.RESULT, 20.0, Constants.TOTAL, 5, Constants.BLANK, 1,
                Constants.CORRECT, 1, Constants.INCORRECT, 3, Constants.PASS_PERCENTAGE, 50);
        Map<String, Object> res = (Map<String, Object>) method.invoke(assessmentServiceV2, List.of(failing));
        assertEquals(Boolean.FALSE, res.get(Constants.PASS));
        assertEquals(20.0, res.get(Constants.OVERALL_RESULT));

        Map<String, Object> broken = mapOf(Constants.RESULT, 20.0);
        res = (Map<String, Object>) method.invoke(assessmentServiceV2, List.of(broken));
        assertFalse(res.containsKey(Constants.OVERALL_RESULT));
        assertFalse(res.containsKey(Constants.PASS));
    }

    @Test
    void testWriteDataToDatabaseAndTriggerKafkaEvent_nullQuestionSet() throws Exception {
        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod(
                "writeDataToDatabaseAndTriggerKafkaEvent", Map.class, String.class, Map.class, Map.class, String.class);
        method.setAccessible(true);
        method.invoke(assessmentServiceV2, new HashMap<>(), USER_ID, null, new HashMap<>(), "Assessment");

        verify(assessmentRepository, never()).updateUserAssesmentDataToDB(any(), any(), any(), any(), any(), any(), any());
        verify(producer, never()).push(anyString(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadAssessmentLevelData_skipsMissingParams() throws Exception {
        when(serverProperties.getAssessmentLevelParams()).thenReturn(List.of(Constants.IDENTIFIER, "missingParam"));
        when(serverProperties.getAssessmentSectionParams()).thenReturn(List.of(Constants.IDENTIFIER, "missingSectionParam"));
        Map<String, Object> section = mapOf(Constants.IDENTIFIER, "s1",
                Constants.CHILDREN, List.of(mapOf(Constants.IDENTIFIER, "q1")));
        Map<String, Object> detail = mapOf(Constants.IDENTIFIER, "a1", Constants.CHILDREN, List.of(section));

        Method method = AssessmentServiceV2Impl.class.getDeclaredMethod("readAssessmentLevelData", Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(assessmentServiceV2, detail);

        assertEquals("a1", result.get(Constants.IDENTIFIER));
        assertFalse(result.containsKey("missingParam"));
        Map<String, Object> filteredSection = ((List<Map<String, Object>>) result.get(Constants.CHILDREN)).get(0);
        assertEquals("s1", filteredSection.get(Constants.IDENTIFIER));
        assertFalse(filteredSection.containsKey("missingSectionParam"));
        // no maxQuestions configured -> no child nodes selected
        assertEquals(new ArrayList<>(), filteredSection.get(Constants.CHILD_NODES));
        assertEquals(List.of("s1"), result.get(Constants.CHILD_NODES));
    }

    // ----- retakeAssessment -----

    @Test
    void testRetakeAssessment_preEnrolmentAssessment() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(ASSESSMENT_ID, mapOf(Constants.CONTEXT_CATEGORY_TAG, Constants.PRE_ENROLLED_ASSESSMENT_KEY,
                Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 5));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(
                mapOf(Constants.SUBMIT_ASSESSMENT_RESPONSE, "r1")));

        SBApiResponse response = service.retakeAssessment(ASSESSMENT_ID, TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(1, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(0, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }

    @Test
    void testRetakeAssessment_withConfiguredMaxAttempts() throws Exception {
        AssessmentServiceV2Impl service = realMapperService();
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN)).thenReturn(USER_ID);
        stubHierarchyInCache(ASSESSMENT_ID, mapOf(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3));
        when(assessmentRepository.fetchUserAssessmentDataFromDB(USER_ID, ASSESSMENT_ID)).thenReturn(List.of(
                mapOf(Constants.SUBMIT_ASSESSMENT_RESPONSE, "r1"),
                mapOf(Constants.SUBMIT_ASSESSMENT_RESPONSE, "r2"),
                mapOf(Constants.STATUS, Constants.NOT_SUBMITTED)));

        SBApiResponse response = service.retakeAssessment(ASSESSMENT_ID, TOKEN);

        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(3, response.getResult().get(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED));
        assertEquals(2, response.getResult().get(Constants.RETAKE_ATTEMPTS_CONSUMED));
    }
}
