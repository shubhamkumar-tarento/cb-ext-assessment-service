package com.igot.cb.assessment.service;



import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.utils.CassandraOperation;

import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.service.ContentService;
import com.igot.cb.common.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import com.igot.cb.core.exception.ApplicationLogicError;
import com.igot.cb.core.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.*;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

class AssessmentUtilServiceV2ImplTest {

    @InjectMocks
    AssessmentUtilServiceV2Impl utilService;

    @Mock
    CbExtAssessmentServerProperties serverProperties;
    @Mock
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    @Mock
    ObjectMapper mapper;
    @Mock
    CassandraOperation cassandraOperation;
    @Mock
    RedisCacheMgr redisCacheMgr;
    @Mock
    ContentService contentService;
    @Mock
    Producer kafkaProducer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    /**
     * Builds an instance with no dependencies wired, which is what these tests relied on when the
     * class still used field injection and therefore had an implicit no-arg constructor. The tests
     * that use it exercise methods which touch none of the injected collaborators.
     */
    private static AssessmentUtilServiceV2Impl newBareService() {
        return new AssessmentUtilServiceV2Impl(null, null, null, null, null, null, null);
    }

    @Test
    void testValidateQumlAssessment_Positive() {
        List<String> originalQ = List.of("q1");
        Map<String, Object> qMap = new HashMap<>();
        Map<String, Object> q = new HashMap<>();
        q.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> opt = new HashMap<>();
        opt.put(Constants.INDEX, "1");
        opt.put(Constants.SELECTED_ANSWER, true);
        opt.put(Constants.ANSWER, true);
        editorState.put(Constants.OPTIONS, List.of(opt));
        q.put(Constants.EDITOR_STATE, editorState);
        q.put(Constants.IDENTIFIER, "q1");
        qMap.put("q1", q);

        List<Map<String, Object>> userQ = new ArrayList<>();
        Map<String, Object> userQ1 = new HashMap<>(q);
        userQ.add(userQ1);

        Map<String, Object> result = utilService.validateQumlAssessment(originalQ, userQ, qMap);
        assertNotNull(result);
        assertTrue(result.containsKey(Constants.RESULT));
    }

    @Test
    void testValidateQumlAssessment_NullInputs() {
        Map<String, Object> result = utilService.validateQumlAssessment(null, null, null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // --- fetchQuestionIdentifierValue ---

    @Test
    void testFetchQuestionIdentifierValue_Positive() {
        when(serverProperties.getAssessmentQuestionParams()).thenReturn(List.of(Constants.IDENTIFIER));
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(
                        Constants.RESPONSE_CODE, Constants.OK,
                        Constants.RESULT, Map.of(
                                Constants.QUESTIONS, List.of(
                                        Map.of(Constants.IDENTIFIER, "q1")
                                )
                        )
                ));
        List<String> ids = List.of("q1");
        List<Object> qList = new ArrayList<>();
        String result = utilService.fetchQuestionIdentifierValue(ids, qList, "category");
        assertEquals("", result);
        assertTrue(qList.isEmpty());
    }

    @Test
    void testFetchQuestionIdentifierValue_EmptyIds() {
        List<Object> qList = new ArrayList<>();
        String result = utilService.fetchQuestionIdentifierValue(Collections.emptyList(), qList, "cat");
        assertEquals("", result);
        assertTrue(qList.isEmpty());
    }

    // --- filterQuestionMapDetail ---

    @Test
    void testFilterQuestionMapDetail_Positive() {
        Map<String, Object> qMap = new HashMap<>();
        qMap.put(Constants.IDENTIFIER, "q1");
        when(serverProperties.getAssessmentQuestionParams()).thenReturn(List.of(Constants.IDENTIFIER));
        Map<String, Object> result = utilService.filterQuestionMapDetail(qMap, Constants.PRACTICE_QUESTION_SET, true);
        assertEquals("q1", result.get(Constants.IDENTIFIER));
    }

    @Test
    void testFilterQuestionMapDetail_MissingParams() {
        Map<String, Object> qMap = new HashMap<>();
        when(serverProperties.getAssessmentQuestionParams()).thenReturn(List.of("nonexistent"));
        Map<String, Object> result = utilService.filterQuestionMapDetail(qMap, "cat", true);
        assertTrue(result.isEmpty());
    }

    // --- readQuestionDetails ---

    @Test
    void testReadQuestionDetails_Positive() {
        when(serverProperties.getAssessmentHost()).thenReturn("http://host/");
        when(serverProperties.getAssessmentQuestionListPath()).thenReturn("path");
        when(serverProperties.getSbApiKey()).thenReturn("key");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), anyMap()))
                .thenReturn(Map.of(Constants.RESULT, Map.of(Constants.QUESTIONS, List.of(Map.of(Constants.IDENTIFIER, "q1")))));
        List<Map<String, Object>> result = utilService.readQuestionDetails(List.of("q1"));
        assertFalse(result.isEmpty());
    }

    @Test
    void testReadQuestionDetails_Exception() {
        when(serverProperties.getAssessmentHost()).thenThrow(new RuntimeException("fail"));
        List<Map<String, Object>> result = utilService.readQuestionDetails(List.of("q1"));
        assertTrue(result.isEmpty());
    }

    // --- getReadHierarchyApiResponse ---

    @Test
    void testGetReadHierarchyApiResponse_Positive() {
        when(serverProperties.getAssessmentHost()).thenReturn("http://host/");
        when(serverProperties.getAssessmentHierarchyReadPath()).thenReturn("read/{id}");
        when(serverProperties.getSbApiKey()).thenReturn("key");
        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap()))
                .thenReturn(Map.of(Constants.RESULT, Map.of(Constants.QUESTION_SET, Map.of("id", "qset"))));
        when(mapper.convertValue(any(), eq(Map.class))).thenReturn(Map.of(Constants.RESULT, Map.of(Constants.QUESTION_SET, Map.of("id", "qset"))));
        Map<String, Object> result = utilService.getReadHierarchyApiResponse("id", "token");
        assertTrue(result.containsKey(Constants.RESULT));
    }

    @Test
    void testGetReadHierarchyApiResponse_Exception() {
        when(serverProperties.getAssessmentHost()).thenThrow(new RuntimeException("fail"));
        Map<String, Object> result = utilService.getReadHierarchyApiResponse("id", "token");
        assertTrue(result.isEmpty());
    }

    // --- parseStartTimeToInstant ---

    @Test
    void testParseStartTimeToInstant_Long() {
        Instant now = Instant.now();
        Instant result = utilService.parseStartTimeToInstant(now.toEpochMilli());
        assertEquals(now.getEpochSecond(), result.getEpochSecond(), 1);
    }

    @Test
    void testParseStartTimeToInstant_StringEpoch() {
        Instant now = Instant.now();
        Instant result = utilService.parseStartTimeToInstant(String.valueOf(now.toEpochMilli()));
        assertEquals(now.getEpochSecond(), result.getEpochSecond(), 1);
    }

    @Test
    void testParseStartTimeToInstant_ISO8601() {
        Instant now = Instant.now();
        Instant result = utilService.parseStartTimeToInstant(now.toString());
        assertEquals(now.getEpochSecond(), result.getEpochSecond(), 1);
    }

    @Test
    void testParseStartTimeToInstant_Instant() {
        Instant now = Instant.now();
        Instant result = utilService.parseStartTimeToInstant(now);
        assertEquals(now, result);
    }

    @Test
    void testParseStartTimeToInstant_Date() {
        Date date = new Date();
        Instant result = utilService.parseStartTimeToInstant(date);
        assertEquals(date.toInstant(), result);
    }

    @Test
    void testParseStartTimeToInstant_Invalid() {
        Object unsupported = new Object();
        assertThrows(IllegalArgumentException.class, () -> utilService.parseStartTimeToInstant(unsupported));
    }

    // --- parseStartTimeToLong ---

    @Test
    void testParseStartTimeToLong_Date() {
        Date date = new Date();
        Long result = utilService.parseStartTimeToLong(date);
        assertEquals(date.getTime(), result);
    }

    @Test
    void testParseStartTimeToLong_Instant() {
        Instant now = Instant.now();
        Long result = utilService.parseStartTimeToLong(now);
        assertEquals(now.toEpochMilli(), result);
    }

    @Test
    void testParseStartTimeToLong_Long() {
        Long now = System.currentTimeMillis();
        Long result = utilService.parseStartTimeToLong(now);
        assertEquals(now, result);
    }

    @Test
    void testParseStartTimeToLong_StringEpoch() {
        Long now = System.currentTimeMillis();
        Long result = utilService.parseStartTimeToLong(String.valueOf(now));
        assertEquals(now, result);
    }

    @Test
    void testParseStartTimeToLong_ISO8601() {
        Instant now = Instant.now();
        Long result = utilService.parseStartTimeToLong(now.toString());
        assertEquals(now.toEpochMilli(), result);
    }

    @Test
    void testParseStartTimeToLong_Invalid() {
        assertEquals(0L, utilService.parseStartTimeToLong(new Object()));
    }

    @Test
    void testReadAssessmentHierarchyFromCache_FromRedis() throws Exception {
        String assessmentId = "aid";
        String cacheKey = Constants.ASSESSMENT_ID + assessmentId + Constants.UNDER_SCORE + Constants.QUESTION_SET;
        Map<String, Object> expected = Map.of("foo", "bar");
        String json = new ObjectMapper().writeValueAsString(expected);

        when(serverProperties.qListFromCacheEnabled()).thenReturn(true);
        when(redisCacheMgr.getCache(cacheKey)).thenReturn(json);
        when(mapper.readValue(eq(json), any(TypeReference.class))).thenReturn(expected);

        Map<String, Object> result = utilService.readAssessmentHierarchyFromCache(assessmentId, false, "token");
        assertEquals(expected, result);
    }

    @Test
    void testReadAssessmentHierarchyFromCache_FromCassandra() throws Exception {
        String assessmentId = "aid";
        String cacheKey = Constants.ASSESSMENT_ID + assessmentId + Constants.UNDER_SCORE + Constants.QUESTION_SET;
        when(serverProperties.qListFromCacheEnabled()).thenReturn(true);
        when(redisCacheMgr.getCache(cacheKey)).thenReturn("");
        Map<String, Object> dbEntry = new HashMap<>();
        dbEntry.put(Constants.HIERARCHY, "{\"foo\":\"bar\"}");
        List<Map<String, Object>> dbList = List.of(dbEntry);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any())).thenReturn(dbList);
        Map<String, Object> expected = Map.of("foo", "bar");
        when(mapper.readValue(anyString(), any(TypeReference.class))).thenReturn(expected);

        Map<String, Object> result = utilService.readAssessmentHierarchyFromCache(assessmentId, false, "token");
        assertEquals(expected, result);
    }

// --- fetchWheebox ---

    @Test
    void testFetchWheebox_FromRedis() throws Exception {
        String userId = "user1";
        String key = "wheebox_user1";
        Map<String, Object> expected = Map.of("score", 99);
        String json = new ObjectMapper().writeValueAsString(expected);

        when(serverProperties.getRedisWheeboxKey()).thenReturn("wheebox");
        when(redisCacheMgr.getContentFromCache(key)).thenReturn(json);
        when(mapper.readValue(eq(json), any(TypeReference.class))).thenReturn(expected);

        Map<String, Object> result = utilService.fetchWheebox(userId);
        assertEquals(expected, result);
    }

    @Test
    void testFetchWheebox_EmptyRedis() {
        when(serverProperties.getRedisWheeboxKey()).thenReturn("wheebox");
        when(redisCacheMgr.getContentFromCache(anyString())).thenReturn(null);

        Map<String, Object> result = utilService.fetchWheebox("user1");
        assertTrue(result.isEmpty());
    }

    @Test
    void testValidateContextLocking_ContentNotFound() {
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        assessmentAllDetail.put(Constants.CONTEXT_CATEGORY_TAG, Constants.FINAL_PROGRAM_ASSESSMENT);
        String parentContextId = "parent1";
        SBApiResponse response = new SBApiResponse();
        String userId = "user1";
        String assessmentIdentifier = "assessment1";

        when(contentService.readContentFromCache(parentContextId, null)).thenReturn(Collections.emptyMap());

        String result = utilService.validateContextLocking(assessmentAllDetail, parentContextId, response, userId, assessmentIdentifier);
        assertEquals(Constants.CONTENT_NOT_FOUND, result);
    }


    @Test
    void testReadAssessmentRecord_Positive() {
        String assessmentId = "aid";
        List<String> fields = List.of("language");
        Map<String, Object> content = new HashMap<>();
        content.put(Constants.LANGUAGE, List.of("en"));
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(Constants.CONTENT, content);
        Map<String, Object> response = new HashMap<>();
        response.put(Constants.RESULT, resultMap);

        when(serverProperties.getContentHost()).thenReturn("http://host/");
        when(serverProperties.getCourseReadPath()).thenReturn("read/");
        when(outboundRequestHandlerService.fetchResultUsingGet(anyString(), anyMap())).thenReturn(response);

        String lang = utilService.readAssessmentRecord(assessmentId, fields);
        assertEquals("en", lang);
    }

    @Test
    void testReadAssessmentRecord_NoResult() {
        String assessmentId = "aid";
        List<String> fields = List.of("language");
        when(serverProperties.getContentHost()).thenReturn("http://host/");
        when(serverProperties.getCourseReadPath()).thenReturn("read/");
        when(outboundRequestHandlerService.fetchResultUsingGet(anyString(), anyMap())).thenReturn(Collections.emptyMap());

        String lang = utilService.readAssessmentRecord(assessmentId, fields);
        assertEquals("", lang);
    }

    @Test
    void testValidateQumlAssessmentV2_CorrectAndBlank() {
        // Setup question set details
        Map<String, Object> questionSetDetailsMap = new HashMap<>();
        questionSetDetailsMap.put(Constants.ASSESSMENT_TYPE, Constants.QUESTION_WEIGHTAGE);
        questionSetDetailsMap.put(Constants.MINIMUM_PASS_PERCENTAGE, 50);
        questionSetDetailsMap.put(Constants.TOTAL_MARKS, 10);
        Map<String, Object> sectionScheme = new HashMap<>();
        sectionScheme.put("EASY", 10);
        questionSetDetailsMap.put(Constants.QUESTION_SECTION_SCHEME, sectionScheme);
        questionSetDetailsMap.put(Constants.NEGATIVE_MARKING_PERCENTAGE, "0%");

        // Original question list
        List<String> originalQuestionList = List.of("q1", "q2");

        // Question map
        Map<String, Object> questionMap = new HashMap<>();
        Map<String, Object> q1 = new HashMap<>();
        q1.put(Constants.IDENTIFIER, "q1");
        q1.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        q1.put(Constants.EDITOR_STATE, Map.of(Constants.OPTIONS, List.of(
                Map.of(Constants.INDEX, "1", Constants.SELECTED_ANSWER, true, Constants.ANSWER, true)
        )));
        q1.put(Constants.QUESTION_LEVEL, "EASY");
        questionMap.put("q1", q1);

        Map<String, Object> q2 = new HashMap<>();
        q2.put(Constants.IDENTIFIER, "q2");
        q2.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        q2.put(Constants.EDITOR_STATE, Map.of(Constants.OPTIONS, List.of(
                Map.of(Constants.INDEX, "1", Constants.SELECTED_ANSWER, false, Constants.ANSWER, true)
        )));
        q2.put(Constants.QUESTION_LEVEL, "EASY");
        questionMap.put("q2", q2);

        // User question list: q1 answered, q2 blank
        Map<String, Object> userQ1 = new HashMap<>(q1);
        userQ1.put(Constants.RESULT, Constants.CORRECT);
        List<Map<String, Object>> userQuestionList = new ArrayList<>();
        userQuestionList.add(userQ1);
        Map<String, Object> userQ2 = new HashMap<>(q2); // blank, no result
        userQuestionList.add(userQ1);
        userQuestionList.add(userQ2);
        // Call method
        Map<String, Object> result = utilService.validateQumlAssessmentV2(
                questionSetDetailsMap, originalQuestionList, userQuestionList, questionMap);

        assertNotNull(result);
        assertEquals(0, result.get(Constants.CORRECT));
        assertEquals(1, result.get(Constants.BLANK));
        assertEquals(2, result.get(Constants.INCORRECT));
        assertEquals(0.0, result.get(Constants.SECTION_MARKS));
        assertEquals(10, result.get(Constants.TOTAL_MARKS));
        assertEquals(Constants.FAIL, result.get(Constants.SECTION_RESULT));
    }

    @Test
    void testGetQumlAnswersV2_MCQ_SCA() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String qid = "q1";
        Map<String, Object> questionMap = new HashMap<>();
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        Map<String, Object> editorState = new HashMap<>();
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.ANSWER, true);
        Map<String, Object> valueObj = new HashMap<>();
        valueObj.put(Constants.VALUE, "A");
        option.put(Constants.VALUE, valueObj);
        options.add(option);
        editorState.put(Constants.OPTIONS, options);
        question.put(Constants.EDITOR_STATE, editorState);
        questionMap.put(qid, question);

        // Mock mapper behavior
        when(mapper.convertValue(any(), any(TypeReference.class)))
                .thenReturn(question)
                .thenReturn(editorState)
                .thenReturn(options)
                .thenReturn(valueObj);

        List<String> questions = List.of(qid);
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswersV2", List.class, Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, questions, questionMap);

        assertNotNull(result);
        assertTrue(result.containsKey(qid));
        assertEquals(List.of("A"), result.get(qid));
    }

    @Test
    void testIsAllCourseCompleted_AllCompleted() {
        ReflectionTestUtils.setField(utilService, "cassandraOperation", cassandraOperation);

        String userId = "user1";
        List<String> courseIds = List.of("courseA", "courseB");
        Map<String, Object> enrolment1 = new HashMap<>();
        enrolment1.put(Constants.STATUS, Constants.ASSESSMENT_STATUS_COMPLETED);
        enrolment1.put(Constants.COURSE_ID, "courseA");
        enrolment1.put(Constants.ACTIVE, true);
        Map<String, Object> enrolment2 = new HashMap<>();
        enrolment2.put(Constants.STATUS, Constants.ASSESSMENT_STATUS_COMPLETED);
        enrolment2.put(Constants.COURSE_ID, "courseB");
        enrolment2.put(Constants.ACTIVE, true);

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                anyString(), anyString(), anyMap(), anyList()))
                .thenReturn(List.of(enrolment1, enrolment2));

        boolean result = ReflectionTestUtils.invokeMethod(utilService, "isAllCourseCompleted", userId, courseIds);
        assertTrue(result);
    }

    @Test
    void testIsAllCourseCompleted_NotAllCompleted() {
        ReflectionTestUtils.setField(utilService, "cassandraOperation", cassandraOperation);

        String userId = "user1";
        List<String> courseIds = List.of("courseA", "courseB");
        Map<String, Object> enrolment1 = new HashMap<>();
        enrolment1.put(Constants.STATUS, Constants.ASSESSMENT_STATUS_COMPLETED);
        enrolment1.put(Constants.COURSE_ID, "courseA");
        Map<String, Object> enrolment2 = new HashMap<>();
        enrolment2.put(Constants.STATUS, 0); // Not completed
        enrolment2.put(Constants.COURSE_ID, "courseB");

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                anyString(), anyString(), anyMap(), anyList()))
                .thenReturn(List.of(enrolment1, enrolment2));

        boolean result = ReflectionTestUtils.invokeMethod(utilService, "isAllCourseCompleted", userId, courseIds);
        assertFalse(result);
    }

    @Test
    void testIsAllCourseCompleted_EmptyCourseIds() {
        boolean result = ReflectionTestUtils.invokeMethod(utilService, "isAllCourseCompleted", "user1", Collections.emptyList());
        assertFalse(result);
    }

    @Test
    void testReadAssessmentRecord_Exception() {
        String assessmentIdentifier = "assess1";
        List<String> fields = List.of("field1", "field2");

        when(outboundRequestHandlerService.fetchResultUsingGet(anyString(), anyMap()))
                .thenThrow(new RuntimeException("Service error"));

        String result = utilService.readAssessmentRecord(assessmentIdentifier, fields);

        assertEquals("", result);
    }

    @Test
    void testSortAnswers_MultipleElements() throws Exception {
        List<String> answers = new ArrayList<>(Arrays.asList("C", "A", "B"));
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("sortAnswers", List.class);
        method.setAccessible(true);
        method.invoke(utilService, answers);
        assertEquals(Arrays.asList("A", "B", "C"), answers);
    }

    @Test
    void testSortAnswers_SingleElement() throws Exception {
        List<String> answers = new ArrayList<>(Collections.singletonList("A"));
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("sortAnswers", List.class);
        method.setAccessible(true);
        method.invoke(utilService, answers);
        assertEquals(Collections.singletonList("A"), answers);
    }

    @Test
    void testSortAnswers_EmptyList() throws Exception {
        List<String> answers = new ArrayList<>();
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("sortAnswers", List.class);
        method.setAccessible(true);
        method.invoke(utilService, answers);
        assertTrue(answers.isEmpty());
    }

    @Test
    void testValidateQumlAssessment_CorrectAnswer() {
        List<String> originalQuestionList = List.of("q1");
        Map<String, Object> questionMap = new HashMap<>();
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, "q1");
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "1");
        option.put(Constants.SELECTED_ANSWER, true);
        option.put(Constants.ANSWER, true);
        editorState.put(Constants.OPTIONS, List.of(option));
        question.put(Constants.EDITOR_STATE, editorState);
        questionMap.put("q1", question);

        List<Map<String, Object>> userQuestionList = new ArrayList<>();
        Map<String, Object> userQuestion = new HashMap<>(question);
        userQuestionList.add(userQuestion);

        Map<String, Object> result = utilService.validateQumlAssessment(originalQuestionList, userQuestionList, questionMap);

        assertNotNull(result);
        assertTrue(result.containsKey(Constants.RESULT));
        assertTrue(result.containsKey(Constants.CORRECT));
        assertTrue(result.containsKey(Constants.INCORRECT));
        assertTrue(result.containsKey(Constants.BLANK));
    }

    @Test
    void testValidateQumlAssessment_IncorrectAnswer() {
        List<String> originalQuestionList = List.of("q1");
        Map<String, Object> questionMap = new HashMap<>();
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, "q1");
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "1");
        option.put(Constants.SELECTED_ANSWER, true);
        option.put(Constants.ANSWER, false);
        editorState.put(Constants.OPTIONS, List.of(option));
        question.put(Constants.EDITOR_STATE, editorState);
        questionMap.put("q1", question);

        List<Map<String, Object>> userQuestionList = new ArrayList<>();
        Map<String, Object> userQuestion = new HashMap<>(question);
        userQuestionList.add(userQuestion);

        Map<String, Object> result = utilService.validateQumlAssessment(originalQuestionList, userQuestionList, questionMap);

        assertNotNull(result);
        assertTrue(result.containsKey(Constants.INCORRECT));
        assertTrue((Integer) result.get(Constants.INCORRECT) > 0);
    }

    @Test
    void testHandleBlankAnswers_NoBlank() throws Exception {
        List<Map<String, Object>> userQuestionList = Arrays.asList(new HashMap<>(), new HashMap<>());
        Map<String, Object> answers = new HashMap<>();
        answers.put("q1", "A");
        answers.put("q2", "B");
        Integer blank = 0;

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("handleBlankAnswers", List.class, Map.class, Integer.class);
        method.setAccessible(true);
        Integer result = (Integer) method.invoke(utilService, userQuestionList, answers, blank);

        assertEquals(0, result);
    }

    @Test
    void testHandleBlankAnswers_WithBlank() throws Exception {
        List<Map<String, Object>> userQuestionList = List.of(new HashMap<>());
        Map<String, Object> answers = new HashMap<>();
        answers.put("q1", "A");
        answers.put("q2", "B");
        Integer blank = 0;

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("handleBlankAnswers", List.class, Map.class, Integer.class);
        method.setAccessible(true);
        Integer result = (Integer) method.invoke(utilService, userQuestionList, answers, blank);

        assertEquals(1, result);
    }

    @Test
    void testHandleBlankAnswers_EmptyAnswers() throws Exception {
        List<Map<String, Object>> userQuestionList = List.of(new HashMap<>());
        Map<String, Object> answers = new HashMap<>();
        Integer blank = 0;

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("handleBlankAnswers", List.class, Map.class, Integer.class);
        method.setAccessible(true);
        Integer result = (Integer) method.invoke(utilService, userQuestionList, answers, blank);

        assertEquals(0, result);
    }

    @Test
    void testHandleBlankAnswers_EmptyUserQuestions() throws Exception {
        List<Map<String, Object>> userQuestionList = new ArrayList<>();
        Map<String, Object> answers = new HashMap<>();
        answers.put("q1", "A");
        answers.put("q2", "B");
        Integer blank = 0;

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("handleBlankAnswers", List.class, Map.class, Integer.class);
        method.setAccessible(true);
        Integer result = (Integer) method.invoke(utilService, userQuestionList, answers, blank);

        assertEquals(2, result);
    }

    // MCQ_MCA: Multiple correct answers
    @Test
    void testGetQumlAnswersV2_MCQ_MCA_MultipleCorrect() throws Exception {
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswersV2", List.class, Map.class);
        method.setAccessible(true);

        Map<String, Object> mcqMca = new HashMap<>();
        mcqMca.put(Constants.IDENTIFIER, "q2");
        mcqMca.put(Constants.QUESTION_TYPE, Constants.MCQ_MCA);
        Map<String, Object> editorStateMca = new HashMap<>();
        Map<String, Object> optionMca1 = new HashMap<>();
        optionMca1.put(Constants.ANSWER, true);
        optionMca1.put(Constants.VALUE, Map.of(Constants.VALUE, "B"));
        Map<String, Object> optionMca2 = new HashMap<>();
        optionMca2.put(Constants.ANSWER, true);
        optionMca2.put(Constants.VALUE, Map.of(Constants.VALUE, "C"));
        editorStateMca.put(Constants.OPTIONS, List.of(optionMca1, optionMca2));
        mcqMca.put(Constants.EDITOR_STATE, editorStateMca);

        Map<String, Object> questionMap = Map.of("q2", mcqMca);
        List<String> questions = List.of("q2");

        when(utilService.mapper.convertValue(any(), any(TypeReference.class))).thenAnswer(invocation -> invocation.getArgument(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, questions, questionMap);

        assertNotNull(result);
        assertEquals(List.of("B", "C"), result.get("q2"));
    }

    // FTB: Fill the blank with position
    @Test
    void testGetQumlAnswersV2_FTB() throws Exception {
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswersV2", List.class, Map.class);
        method.setAccessible(true);

        Map<String, Object> ftb = new HashMap<>();
        ftb.put(Constants.IDENTIFIER, "q3");
        ftb.put(Constants.QUESTION_TYPE, Constants.FTB);
        Map<String, Object> editorStateFtb = new HashMap<>();
        Map<String, Object> optionFtb = new HashMap<>();
        optionFtb.put(Constants.ANSWER, true);
        optionFtb.put("position", "2");
        optionFtb.put(Constants.VALUE, Map.of(Constants.BODY, "D"));
        editorStateFtb.put(Constants.OPTIONS, List.of(optionFtb));
        ftb.put(Constants.EDITOR_STATE, editorStateFtb);

        Map<String, Object> questionMap = Map.of("q3", ftb);
        List<String> questions = List.of("q3");

        when(utilService.mapper.convertValue(any(), any(TypeReference.class))).thenAnswer(invocation -> invocation.getArgument(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, questions, questionMap);

        assertNotNull(result);
        assertEquals(List.of("1-D"), result.get("q3")); // position-1 (2-1)
    }

    // MTF: Match the following
    @Test
    void testGetQumlAnswersV2_MTF() throws Exception {
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswersV2", List.class, Map.class);
        method.setAccessible(true);

        Map<String, Object> mtf = new HashMap<>();
        mtf.put(Constants.IDENTIFIER, "q4");
        mtf.put(Constants.QUESTION_TYPE, Constants.MTF);
        Map<String, Object> editorStateMtf = new HashMap<>();
        Map<String, Object> optionMtf = new HashMap<>();
        optionMtf.put(Constants.ANSWER, true);
        optionMtf.put(Constants.VALUE, Map.of(Constants.VALUE, "E"));
        editorStateMtf.put(Constants.OPTIONS, List.of(optionMtf));
        mtf.put(Constants.EDITOR_STATE, editorStateMtf);

        Map<String, Object> questionMap = Map.of("q4", mtf);
        List<String> questions = List.of("q4");

        when(utilService.mapper.convertValue(any(), any(TypeReference.class))).thenAnswer(invocation -> invocation.getArgument(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, questions, questionMap);

        assertNotNull(result);
        assertEquals(List.of("E-true"), result.get("q4"));
    }

    // Edge case: No options
    @Test
    void testGetQumlAnswersV2_NoOptions() throws Exception {
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswersV2", List.class, Map.class);
        method.setAccessible(true);

        Map<String, Object> mcqSca = new HashMap<>();
        mcqSca.put(Constants.IDENTIFIER, "q5");
        mcqSca.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        Map<String, Object> editorStateSca = new HashMap<>();
        editorStateSca.put(Constants.OPTIONS, Collections.emptyList());
        mcqSca.put(Constants.EDITOR_STATE, editorStateSca);

        Map<String, Object> questionMap = Map.of("q5", mcqSca);
        List<String> questions = List.of("q5");

        when(utilService.mapper.convertValue(any(), any(TypeReference.class))).thenAnswer(invocation -> invocation.getArgument(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, questions, questionMap);

        assertNotNull(result);
        assertEquals(Collections.emptyList(), result.get("q5"));
    }

    @Test
    void testGetQumlAnswers_MCQ_SCA() throws Exception {
        AssessmentUtilServiceV2Impl bareService = newBareService();
        String qid = "q1";
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.ANSWER, true);
        option.put(Constants.VALUE, Map.of(Constants.VALUE, "A"));
        editorState.put(Constants.OPTIONS, List.of(option));
        question.put(Constants.EDITOR_STATE, editorState);
        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(bareService, List.of(qid), questionMap);
        assertEquals(List.of("A"), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_MCQ_MCA() throws Exception {
        AssessmentUtilServiceV2Impl bareService = newBareService();
        String qid = "q2";
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_MCA);
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> option1 = new HashMap<>();
        option1.put(Constants.ANSWER, true);
        option1.put(Constants.VALUE, Map.of(Constants.VALUE, "B"));
        Map<String, Object> option2 = new HashMap<>();
        option2.put(Constants.ANSWER, true);
        option2.put(Constants.VALUE, Map.of(Constants.VALUE, "C"));
        editorState.put(Constants.OPTIONS, List.of(option1, option2));
        question.put(Constants.EDITOR_STATE, editorState);
        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(bareService, List.of(qid), questionMap);
        assertEquals(List.of("B", "C"), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_FTB() throws Exception {
        AssessmentUtilServiceV2Impl bareService = newBareService();
        String qid = "q3";
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.FTB);
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.ANSWER, true);
        option.put(Constants.VALUE, Map.of(Constants.BODY, "D"));
        editorState.put(Constants.OPTIONS, List.of(option));
        question.put(Constants.EDITOR_STATE, editorState);
        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(bareService, List.of(qid), questionMap);
        assertEquals(List.of("D"), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_MTF() throws Exception {
        AssessmentUtilServiceV2Impl bareService = newBareService();
        String qid = "q4";
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.MTF);
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.ANSWER, true);
        option.put(Constants.VALUE, Map.of(Constants.VALUE, "E"));
        editorState.put(Constants.OPTIONS, List.of(option));
        question.put(Constants.EDITOR_STATE, editorState);
        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(bareService, List.of(qid), questionMap);
        assertEquals(List.of("E-true"), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_NoOptions() throws Exception {
        AssessmentUtilServiceV2Impl bareService = newBareService();
        String qid = "q5";
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        Map<String, Object> editorState = new HashMap<>();
        editorState.put(Constants.OPTIONS, Collections.emptyList());
        question.put(Constants.EDITOR_STATE, editorState);
        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(bareService, List.of(qid), questionMap);
        assertEquals(Collections.emptyList(), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_EmptyQuestion() throws Exception {
        AssessmentUtilServiceV2Impl bareService = newBareService();
        String qid = "q6";
        Map<String, Object> questionMap = Map.of(qid, Collections.emptyMap());

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) method.invoke(bareService, List.of(qid), questionMap);
        assertEquals(Collections.emptyList(), result.get(qid));
    }

    @Test
    void testValidateQumlAssessmentV3_EmptyInputs() {
        assertThrows(ApplicationLogicError.class, () -> utilService.validateQumlAssessmentV3(null, null, null, null));
    }

    @Test
    void testValidateQumlAssessment_MTF() {
        List<String> originalQ = List.of("q1");
        Map<String, Object> qMap = new HashMap<>();
        Map<String, Object> q = new HashMap<>();
        q.put(Constants.QUESTION_TYPE, Constants.MTF);
        q.put(Constants.IDENTIFIER, "q1");
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "1");
        option.put(Constants.ANSWER, true);
        option.put(Constants.VALUE, Map.of(Constants.VALUE, "MatchA"));
        editorState.put(Constants.OPTIONS, List.of(option));
        q.put(Constants.EDITOR_STATE, editorState);
        qMap.put("q1", q);

        // User answer should match the expected structure
        Map<String, Object> userQ = new HashMap<>(q);
        Map<String, Object> userOption = new HashMap<>(option);
        userOption.put(Constants.SELECTED_ANSWER, "MatchA");
        editorState.put(Constants.OPTIONS, List.of(userOption));
        userQ.put(Constants.EDITOR_STATE, editorState);

        List<Map<String, Object>> userQList = List.of(userQ);

        Map<String, Object> result = utilService.validateQumlAssessment(originalQ, userQList, qMap);
        assertEquals(1, result.get(Constants.INCORRECT));
    }

    @Test
    void testValidateQumlAssessment_FTB() {
        List<String> originalQ = List.of("q1");
        Map<String, Object> qMap = new HashMap<>();
        Map<String, Object> q = new HashMap<>();
        q.put(Constants.QUESTION_TYPE, Constants.FTB);
        q.put(Constants.IDENTIFIER, "q1");
        Map<String, Object> editorState = new HashMap<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.SELECTED_ANSWER, "fillthis");
        option.put(Constants.ANSWER, true);
        option.put(Constants.VALUE, Map.of(Constants.BODY, "fillthis"));
        editorState.put(Constants.OPTIONS, List.of(option));
        q.put(Constants.EDITOR_STATE, editorState);
        qMap.put("q1", q);

        List<Map<String, Object>> userQ = List.of(new HashMap<>(q));

        Map<String, Object> result = utilService.validateQumlAssessment(originalQ, userQ, qMap);
        assertEquals(1, result.get(Constants.CORRECT));
    }

    @Test
    void testValidateQumlAssessment_BlankAnswer() {
        List<String> originalQ = List.of("q1");
        Map<String, Object> qMap = new HashMap<>();
        Map<String, Object> q = new HashMap<>();
        q.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        q.put(Constants.IDENTIFIER, "q1");
        Map<String, Object> editorState = new HashMap<>();
        // No options selected
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "1");
        option.put(Constants.SELECTED_ANSWER, false); // Not selected
        editorState.put(Constants.OPTIONS, List.of(option));
        q.put(Constants.EDITOR_STATE, editorState);
        qMap.put("q1", q);

        List<Map<String, Object>> userQ = List.of(new HashMap<>(q));

        Map<String, Object> result = utilService.validateQumlAssessment(originalQ, userQ, qMap);
        assertEquals(1, result.get(Constants.BLANK));
    }

    @Test
    void testValidateQumlAssessment_MultipleAnswersSorting() {
        List<String> originalQ = List.of("q1");
        Map<String, Object> qMap = new HashMap<>();
        Map<String, Object> q = new HashMap<>();
        q.put(Constants.QUESTION_TYPE, Constants.MCQ_MCA);
        q.put(Constants.IDENTIFIER, "q1");
        Map<String, Object> editorState = new HashMap<>();

        Map<String, Object> option1 = new HashMap<>();
        option1.put(Constants.INDEX, "B");
        option1.put(Constants.SELECTED_ANSWER, true);
        option1.put(Constants.ANSWER, true);
        option1.put(Constants.VALUE, Map.of(Constants.VALUE, "B"));

        Map<String, Object> option2 = new HashMap<>();
        option2.put(Constants.INDEX, "A");
        option2.put(Constants.SELECTED_ANSWER, true);
        option2.put(Constants.ANSWER, true);
        option2.put(Constants.VALUE, Map.of(Constants.VALUE, "A"));

        editorState.put(Constants.OPTIONS, List.of(option1, option2));
        q.put(Constants.EDITOR_STATE, editorState);
        qMap.put("q1", q);

        List<Map<String, Object>> userQ = List.of(new HashMap<>(q));

        Map<String, Object> result = utilService.validateQumlAssessment(originalQ, userQ, qMap);
        assertEquals(1, result.get(Constants.CORRECT));
    }

    @Test
    void testFetchQuestionIdentifierValue_InvalidResponse() {
        List<String> identifiers = List.of("q1");
        List<Object> questionList = new ArrayList<>();
        String primaryCategory = "category";

        Map<String, Object> badResponse = new HashMap<>();
        badResponse.put(Constants.RESPONSE_CODE, "ERROR"); // Not OK

        when(serverProperties.getAssessmentHost()).thenReturn("http://localhost/");
        when(serverProperties.getAssessmentQuestionListPath()).thenReturn("api/question/list");
        when(serverProperties.getSbApiKey()).thenReturn("Bearer dummy");

        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), anyMap()))
                .thenReturn(badResponse);

        String result = utilService.fetchQuestionIdentifierValue(identifiers, questionList, primaryCategory);

        assertTrue(result.contains("Failed to get Question Details from the Question List API"));
    }

    @Test
    void testValidateContextLocking_CoursesNotCompleted() {
        Map<String, Object> assessmentAllDetail = Map.of(Constants.CONTEXT_CATEGORY_TAG, Constants.FINAL_PROGRAM_ASSESSMENT);
        String parentContextId = "parent123";
        SBApiResponse response = new SBApiResponse();
        String userId = "user1";
        String assessmentIdentifier = "assessment1";

        Map<String, Object> contentDetails = new HashMap<>();
        contentDetails.put(Constants.CONTEXT_LOCKING_TYPE, Constants.COURSE_ASSESSMENT_ONLY);

        Set<String> courseIds = Set.of("course1", "course2");

        when(contentService.readContentFromCache(eq(parentContextId), any())).thenReturn(contentDetails);
        when(contentService.readChildCoursesFromCache(parentContextId)).thenReturn(courseIds);

        // Mock private method isAllCourseCompleted to return false
        ReflectionTestUtils.setField(utilService, "cassandraOperation", cassandraOperation);

        String result = utilService.validateContextLocking(assessmentAllDetail, parentContextId, response, userId, assessmentIdentifier);

        assertEquals(Constants.USER_COURSES_NOT_COMPLETED, result);
    }

    @Test
    void testValidateContextLocking_UnsupportedFeature() {
        Map<String, Object> assessmentAllDetail = Map.of(Constants.CONTEXT_CATEGORY_TAG, Constants.FINAL_PROGRAM_ASSESSMENT);
        String parentContextId = "parent123";
        SBApiResponse response = new SBApiResponse();
        String userId = "user1";
        String assessmentIdentifier = "assessment1";

        Map<String, Object> contentDetails = new HashMap<>();
        contentDetails.put(Constants.CONTEXT_LOCKING_TYPE, "UNKNOWN_LOCK_TYPE");

        when(contentService.readContentFromCache(eq(parentContextId), any())).thenReturn(contentDetails);

        String result = utilService.validateContextLocking(assessmentAllDetail, parentContextId, response, userId, assessmentIdentifier);

        assertEquals(Constants.UNSUPPORTED_FEATURE, result);
    }

    @Test
    void testValidateContextLocking_ContentNotFoundError() {
        Map<String, Object> assessmentAllDetail = Map.of(Constants.CONTEXT_CATEGORY_TAG, Constants.FINAL_PROGRAM_ASSESSMENT);
        String parentContextId = "parent123";
        SBApiResponse response = new SBApiResponse();
        String userId = "user1";
        String assessmentIdentifier = "assessment1";

        when(contentService.readContentFromCache(eq(parentContextId), any())).thenReturn(Collections.emptyMap());

        String result = utilService.validateContextLocking(assessmentAllDetail, parentContextId, response, userId, assessmentIdentifier);

        assertEquals(Constants.CONTENT_NOT_FOUND, result);
    }

    @Test
    void testValidateContextLocking_InvalidCourseRequest() {
        Map<String, Object> assessmentAllDetail = Map.of(Constants.CONTEXT_CATEGORY_TAG, Constants.FINAL_PROGRAM_ASSESSMENT);
        String parentContextId = ""; // blank
        SBApiResponse response = new SBApiResponse();
        String userId = "user1";
        String assessmentIdentifier = "assessment1";

        String result = utilService.validateContextLocking(assessmentAllDetail, parentContextId, response, userId, assessmentIdentifier);

        assertEquals(Constants.INVALID_COURSE_REQUEST, result);
    }

    @Test
    void testGetMarkedIndexForEachQuestionV2_MTF() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "1");
        option.put(Constants.SELECTED_ANSWER, "AnswerA");
        options.add(option);

        List<String> marked = new ArrayList<>();

        ReflectionTestUtils.invokeMethod(utilService,
                "getMarkedIndexForEachQuestionV2", Constants.MTF, options, marked, "anyType");

        assertEquals(List.of("1-answera"), marked);
    }

    @Test
    void testGetMarkedIndexForEachQuestionV2_FTB() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "2");
        option.put(Constants.SELECTED_ANSWER, "AnswerB");
        options.add(option);

        List<String> marked = new ArrayList<>();

        ReflectionTestUtils.invokeMethod(utilService,
                "getMarkedIndexForEachQuestionV2", Constants.FTB, options, marked, "anyType");

        assertEquals(List.of("2-AnswerB"), marked);
    }

    @Test
    void testGetMarkedIndexForEachQuestionV2_MCQ_SCA_QuestionWeightage() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "3");
        option.put(Constants.SELECTED_ANSWER, true);
        options.add(option);

        List<String> marked = new ArrayList<>();

        // Call the private method via reflection
        ReflectionTestUtils.invokeMethod(
                utilService,
                "getMarkedIndexForEachQuestionV2",
                Constants.MCQ_SCA,
                options,
                marked,
                Constants.QUESTION_WEIGHTAGE
        );

        // Assert outcome instead of verifying private method
        assertFalse(marked.isEmpty(), "Marked list should be populated");
    }



    @Test
    void testGetMarkedIndexForEachQuestionV2_MCQ_SCA_OptionWeightage() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "3");
        option.put(Constants.SELECTED_ANSWER, "true");  // Use string if expected by logic
        options.add(option);

        List<String> marked = new ArrayList<>();

        // No spy needed
        ReflectionTestUtils.invokeMethod(utilService,
                "getMarkedIndexForEachQuestionV2",
                Constants.MCQ_SCA,
                options,
                marked,
                Constants.OPTION_WEIGHTAGE);

        // Assert that marked list got updated
        assertFalse(marked.isEmpty(), "Marked list should not be empty for MCQ_SCA with OPTION_WEIGHTAGE");
    }



    @Test
    void testGetMarkedIndexForEachQuestionV2_MCQ_MCA_W_OptionWeightage() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "4");
        option.put(Constants.SELECTED_ANSWER, "true");
        options.add(option);

        List<String> marked = new ArrayList<>();

        // No spy needed since we're not verifying private method
        ReflectionTestUtils.invokeMethod(utilService,
                "getMarkedIndexForEachQuestionV2", Constants.MCQ_MCA_W, options, marked, Constants.OPTION_WEIGHTAGE);

        // Validate that marking happened as expected
        assertFalse(marked.isEmpty());
    }


    @Test
    void testGetMarkedIndexForEachQuestionV2_Default() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "5");
        option.put(Constants.SELECTED_ANSWER, true);
        options.add(option);

        List<String> marked = new ArrayList<>();

        // Case: Unknown type → default case, nothing happens
        ReflectionTestUtils.invokeMethod(utilService,
                "getMarkedIndexForEachQuestionV2", "UNKNOWN_TYPE", options, marked, "UNKNOWN_ASSESSMENT");

        assertTrue(marked.isEmpty());
    }


    @Test
    void testHandleqTypeQuestionV2_MCQ_SCA_OptionWeightage() {
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "1");
        option.put(Constants.SELECTED_ANSWER, true);
        List<Map<String, Object>> options = List.of(option);

        Map<String, Object> editorState = new HashMap<>();
        editorState.put(Constants.OPTIONS, options);

        Map<String, Object> question = new HashMap<>();
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        question.put(Constants.EDITOR_STATE, editorState);

        List<String> marked = new ArrayList<>();

        // Inject ObjectMapper into the service instance
        ObjectMapper objectMapper = new ObjectMapper();
        ReflectionTestUtils.setField(utilService, "mapper", objectMapper);

        // Invoke private method
        ReflectionTestUtils.invokeMethod(
                utilService,
                "handleqTypeQuestionV2",
                question,
                marked,
                Constants.OPTION_WEIGHTAGE
        );

        assertFalse(marked.isEmpty(), "Marked list should be filled with processed answers");
    }

    @Test
    void testGetQumlAnswersV2_WithoutQuestionType_UsesOptionsList() {
        // Arrange
        List<String> questions = List.of("q1");

        Map<String, Object> option = new HashMap<>();
        option.put(Constants.IS_CORRECT, true);
        option.put(Constants.OPTION_ID, "opt123");

        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, "q1");
        question.put(Constants.OPTIONS, List.of(option)); // No QUESTION_TYPE key

        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put("q1", question);

        ObjectMapper realMapper = new ObjectMapper();
        ReflectionTestUtils.setField(utilService, "mapper", realMapper);

        // Act
        Map<String, Object> result = ReflectionTestUtils.invokeMethod(
                utilService,
                "getQumlAnswersV2",
                questions,
                questionMap
        );

        // Assert
        assertTrue(result.containsKey("q1"));
        List<String> correctAnswers = (List<String>) result.get("q1");
        assertEquals(1, correctAnswers.size());
        assertEquals("opt123", correctAnswers.get(0));
    }

    @Test
    void testGetTotalMarks_PositiveAndNegativeCases() {
        // Prepare maps
        Map<String, Object> validMap = new HashMap<>();
        validMap.put(Constants.TOTAL_MARKS, 10); // Integer (instanceof Number)

        Map<String, Object> invalidMap = new HashMap<>();
        invalidMap.put(Constants.TOTAL_MARKS, "ten"); // Not a Number

        // Use Reflection to call private method
        int result1 = ReflectionTestUtils.invokeMethod(utilService, "getTotalMarks", validMap);
        int result2 = ReflectionTestUtils.invokeMethod(utilService, "getTotalMarks", invalidMap);

        // Assertions
        assertEquals(10, result1);  // Valid integer case
        assertEquals(0, result2);   // Invalid type returns 0
    }


    @Test
    void testGetAssessmentType_ReturnsExpectedValue() {
        Map<String, Object> questionSetDetailsMap = new HashMap<>();
        questionSetDetailsMap.put(Constants.ASSESSMENT_TYPE, "option-weightage");

        String result = ReflectionTestUtils.invokeMethod(utilService, "getAssessmentType", questionSetDetailsMap);

        assertEquals("option-weightage", result);
    }

    @Test
    void testGetMinimumPassPercentage_ValuePresent() {
        Map<String, Object> questionSetDetailsMap = new HashMap<>();
        questionSetDetailsMap.put(Constants.MINIMUM_PASS_PERCENTAGE, 60);

        int result = ReflectionTestUtils.invokeMethod(utilService, "getMinimumPassPercentage", questionSetDetailsMap);

        assertEquals(60, result);
    }

    @Test
    void testGetQuestionSetSectionScheme() {
        // Prepare test data
        Map<String, Object> expectedScheme = new HashMap<>();
        expectedScheme.put("section1", "details");

        Map<String, Object> questionSetDetailsMap = new HashMap<>();
        questionSetDetailsMap.put(Constants.QUESTION_SECTION_SCHEME, expectedScheme);

        // Ensure realMapper is initialized
        ObjectMapper realMapper = new ObjectMapper();
        ReflectionTestUtils.setField(utilService, "mapper", realMapper);

        // Invoke method
        Map<String, Object> result = ReflectionTestUtils.invokeMethod(
                utilService,
                "getQuestionSetSectionScheme",
                questionSetDetailsMap
        );

        // Assertion
        assertNotNull(result);
        assertEquals("details", result.get("section1"));
    }

    @Test
    void testGetNegativeMarksValue() {
        Map<String, Object> questionSetDetailsMap = new HashMap<>();
        questionSetDetailsMap.put(Constants.NEGATIVE_MARKING_PERCENTAGE, "25%");

        int result = ReflectionTestUtils.invokeMethod(
                AssessmentUtilServiceV2Impl.class, // replace with your actual class
                "getNegativeMarksValue",
                questionSetDetailsMap
        );

        assertEquals(25, result);
    }

    @Test
    void testShuffleOptions_ShouldReturnShuffledCopy() {
        // Arrange
        List<Map<String, Object>> originalList = new ArrayList<>();
        Map<String, Object> option1 = Map.of("index", 1, "value", "A");
        Map<String, Object> option2 = Map.of("index", 2, "value", "B");
        Map<String, Object> option3 = Map.of("index", 3, "value", "C");

        originalList.add(option1);
        originalList.add(option2);
        originalList.add(option3);

        // Act
        List<Map<String, Object>> result = AssessmentUtilServiceV2Impl.shuffleOptions(originalList);

        // Assert
        assertNotNull(result, "Shuffled list should not be null");
        assertEquals(3, result.size(), "Shuffled list should have same size as original");

        // Should contain same elements regardless of order
        assertTrue(result.containsAll(originalList), "Shuffled list should contain all original elements");

        // Allow for the rare chance that shuffle returns same order (don't fail the test on it)
        System.out.println("Shuffled result: " + result);
        System.out.println("Original list: " + originalList);
    }

    @Test
    void testCalculatePassPercentage_QuestionWeightage() {
        Map<String, Object> resultMap = new HashMap<>();
        Double sectionMarks = 80.0;
        Integer totalMarks = 100;
        Integer correct = 0, blank = 0, inCorrect = 0;

        // Act
        ReflectionTestUtils.invokeMethod(
                AssessmentUtilServiceV2Impl.class,
                "calculatePassPercentage",
                sectionMarks,
                totalMarks,
                correct,
                blank,
                inCorrect,
                Constants.QUESTION_WEIGHTAGE,
                resultMap
        );

        // Assert
        assertEquals(80.0, resultMap.get(Constants.RESULT));
        assertFalse(resultMap.containsKey(Constants.TOTAL), "TOTAL key should not be present in question weightage");
    }

    @Test
    void testHandleCorrectAnswer_Success() {
        // Given
        Double sectionMarks = 5.0;

        Map<String, Object> questionSetSectionScheme = new HashMap<>();
        questionSetSectionScheme.put("level1", 10);  // level mapping to marks

        Map<String, Object> proficiencyMap = new HashMap<>();
        proficiencyMap.put(Constants.QUESTION_LEVEL, "level1");

        // When
        Double result = ReflectionTestUtils.invokeMethod(
                utilService,
                "handleCorrectAnswer",
                sectionMarks,
                questionSetSectionScheme,
                proficiencyMap
        );

        // Then
        assertEquals(15.0, result); // 5 + 10 = 15
    }

    @Test
    void testGetMarkedIndexForEachQuestion_MTF() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "1");
        option.put(Constants.SELECTED_ANSWER, true);
        options.add(option);

        List<String> marked = new ArrayList<>();

        ReflectionTestUtils.invokeMethod(
                utilService,
                "getMarkedIndexForEachQuestion",
                Constants.MTF,
                options,
                marked,
                Constants.QUESTION_WEIGHTAGE
        );

        assertEquals(1, marked.size());
        assertEquals("1-true", marked.get(0));
    }

    @Test
    void testGetMarkedIndexForEachQuestion_FTB() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.SELECTED_ANSWER, "answer");
        options.add(option);

        List<String> marked = new ArrayList<>();

        ReflectionTestUtils.invokeMethod(
                utilService,
                "getMarkedIndexForEachQuestion",
                Constants.FTB,
                options,
                marked,
                Constants.QUESTION_WEIGHTAGE
        );

        assertEquals(1, marked.size());
        assertEquals("answer", marked.get(0));
    }

    @Test
    void testGetMarkedIndexForEachQuestion_MCQ_SCA_QuestionWeightage() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "2");
        option.put(Constants.SELECTED_ANSWER, true);
        options.add(option);

        List<String> marked = new ArrayList<>();

        // Mock or stub getMarkedIndexForQuestionWeightAge() if required
        ReflectionTestUtils.invokeMethod(
                utilService,
                "getMarkedIndexForEachQuestion",
                Constants.MCQ_SCA,
                options,
                marked,
                Constants.QUESTION_WEIGHTAGE
        );

        assertFalse(marked.isEmpty());
    }

    @Test
    void testGetMarkedIndexForEachQuestion_MCQ_MCA_W_OptionWeightage() {
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "3");
        option.put(Constants.SELECTED_ANSWER, true);
        options.add(option);

        List<String> marked = new ArrayList<>();

        // Mock getMarkedIndexForOptionWeightAge() if needed
        ReflectionTestUtils.invokeMethod(
                utilService,
                "getMarkedIndexForEachQuestion",
                Constants.MCQ_MCA_W,
                options,
                marked,
                Constants.OPTION_WEIGHTAGE
        );

        assertFalse(marked.isEmpty());
    }

    @Test
    void testGetOptionWeightages_withMCQTypeQuestions() {
        // Arrange
        String questionId = "q1";

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(Constants.VALUE, "Option A");

        Map<String, Object> option = new HashMap<>();
        option.put(Constants.VALUE, valueMap);
        option.put(Constants.ANSWER, 10);

        List<Map<String, Object>> options = List.of(option);

        Map<String, Object> editorState = new HashMap<>();
        editorState.put(Constants.OPTIONS, options);

        Map<String, Object> question = new HashMap<>();
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA); // Can test MCQ_MCA and MCQ_MCA_W similarly
        question.put(Constants.EDITOR_STATE, editorState);
        question.put(Constants.IDENTIFIER, questionId);

        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(questionId, question);

        List<String> questions = List.of(questionId);

        // Act
        Map<String, Object> result = ReflectionTestUtils.invokeMethod(
                utilService,
                "getOptionWeightages",
                questions,
                questionMap
        );

        // Assert
        assertNotNull(result);
        assertTrue(result.containsKey("q1"));
        Map<String, Object> weightage = (Map<String, Object>) result.get("q1");
        assertEquals(10, weightage.get("Option A"));
    }

    @Test
    void testGetOptionWeightages_withUnsupportedQuestionType() {
        String questionId = "q2";

        Map<String, Object> question = new HashMap<>();
        question.put(Constants.QUESTION_TYPE, "essay"); // Not matched in switch-case
        question.put(Constants.EDITOR_STATE, new HashMap<>());
        question.put(Constants.IDENTIFIER, questionId);

        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(questionId, question);

        List<String> questions = List.of(questionId);

        Map<String, Object> result = ReflectionTestUtils.invokeMethod(
                utilService,
                "getOptionWeightages",
                questions,
                questionMap
        );

        assertTrue(result.containsKey("q2"));
        assertTrue(((Map<?, ?>) result.get("q2")).isEmpty());
    }

    @Test
    void testCalculateScoreForOptionWeightage_WithNumericWeightage() {
        // Arrange
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, "q1");

        Map<String, Object> weightMap = new HashMap<>();
        weightMap.put("OptionA", 2.5);

        Map<String, Object> optionWeightages = new HashMap<>();
        optionWeightages.put("q1", weightMap);

        List<String> marked = List.of("OptionA");

        // Act
        Double result = ReflectionTestUtils.invokeMethod(
                AssessmentUtilServiceV2Impl.class,
                "calculateScoreForOptionWeightage",
                question,
                Constants.OPTION_WEIGHTAGE,
                optionWeightages,
                1.0,
                marked
        );

        // Assert
        assertEquals(3.5, result);
    }

    @Test
    void testFetchRecursiveQuestionIds_WithNestedAndDirectQuestions() {
        // Arrange
        AssessmentUtilServiceV2Impl service = newBareService();

        Map<String, Object> question1 = new HashMap<>();
        question1.put(Constants.OBJECT_TYPE, "Question");
        question1.put(Constants.IDENTIFIER, "q1");

        Map<String, Object> question2 = new HashMap<>();
        question2.put(Constants.OBJECT_TYPE, "Question");
        question2.put(Constants.IDENTIFIER, "q2");

        Map<String, Object> innerSet = new HashMap<>();
        innerSet.put(Constants.OBJECT_TYPE, Constants.QUESTION_SET);
        innerSet.put(Constants.CHILDREN, List.of(question2));

        Map<String, Object> outerSet = new HashMap<>();
        outerSet.put(Constants.OBJECT_TYPE, Constants.QUESTION_SET);
        outerSet.put(Constants.CHILDREN, List.of(innerSet, question1));

        List<Map<String, Object>> children = List.of(outerSet);

        // Act
        List<String> result = ReflectionTestUtils.invokeMethod(service, "fetchRecursiveQuestionIds", children, new ArrayList<>());

        // Assert
        assertEquals(List.of("q2", "q1"), result);
    }

    @Test
    void testFetchRecursiveQuestionIds_WithEmptyChildren() {
        // Arrange
        AssessmentUtilServiceV2Impl service = newBareService();
        List<Map<String, Object>> emptyChildren = new ArrayList<>();

        // Act
        List<String> result = ReflectionTestUtils.invokeMethod(service, "fetchRecursiveQuestionIds", emptyChildren, new ArrayList<>());

        // Assert
        assertTrue(result.isEmpty());
    }

    @Test
    void testFetchHierarchyFromAssessServc_Success() {
        String qSetId = "qSet123";
        String token = "dummyToken";

        Map<String, Object> questionSet = Map.of("identifier", qSetId);
        Map<String, Object> resultMap = Map.of(Constants.QUESTION_SET, questionSet);
        Map<String, Object> mockApiResponse = Map.of(
                Constants.RESPONSE_CODE, Constants.OK,
                Constants.RESULT, resultMap
        );

        AssessmentUtilServiceV2Impl serviceSpy = Mockito.spy(newBareService());
        Mockito.doReturn(mockApiResponse).when(serviceSpy).getReadHierarchyApiResponse(qSetId, token);

        Map<String, Object> result = serviceSpy.fetchHierarchyFromAssessServc(qSetId, token);

        assertNotNull(result);
        assertEquals(qSetId, result.get("identifier"));
    }

    @Test
    void testFetchHierarchyFromAssessServc_FailureResponse() {
        String qSetId = "qSet123";
        String token = "dummyToken";

        Map<String, Object> mockApiResponse = Map.of(
                Constants.RESPONSE_CODE, "ERROR",
                Constants.RESULT, Map.of()
        );

        AssessmentUtilServiceV2Impl serviceSpy = Mockito.spy(newBareService());
        Mockito.doReturn(mockApiResponse).when(serviceSpy).getReadHierarchyApiResponse(qSetId, token);

        assertThrows(RuntimeException.class, () -> {
            serviceSpy.fetchHierarchyFromAssessServc(qSetId, token);
        });
    }

    @Test
    void testValidateQumlAssessmentV3_OptionWeightage() {
        Map<String, Object> questionSetDetailsMap = new HashMap<>();
        questionSetDetailsMap.put(Constants.ASSESSMENT_TYPE, Constants.OPTION_WEIGHTAGE);
        questionSetDetailsMap.put(Constants.TOTAL_MARKS, 10);
        questionSetDetailsMap.put(Constants.MINIMUM_PASS_PERCENTAGE, 40);

        String qId = "q1";
        List<String> originalQuestionList = List.of(qId);

        // userQuestionList with one question attempted
        Map<String, Object> userQuestion = new HashMap<>();
        userQuestion.put(Constants.IDENTIFIER, qId);
        userQuestion.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);

        Map<String, Object> option = new HashMap<>();
        option.put(Constants.INDEX, "1");
        option.put(Constants.SELECTED_ANSWER, true);

        List<Map<String, Object>> options = new ArrayList<>();
        options.add(option);

        Map<String, Object> editorState = new HashMap<>();
        editorState.put(Constants.OPTIONS, options);
        userQuestion.put(Constants.EDITOR_STATE, editorState);

        List<Map<String, Object>> userQuestionList = new ArrayList<>();
        userQuestionList.add(userQuestion);

        // questionMap with one question and its option weightage
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qId);
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        question.put(Constants.EDITOR_STATE, editorState);

        // Value object for OPTION_WEIGHTAGE
        Map<String, Object> valueObj = new HashMap<>();
        valueObj.put(Constants.VALUE, "1");
        option.put(Constants.VALUE, valueObj);
        option.put(Constants.ANSWER, true);  // 5 marks for correct answer

        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(qId, question);

        // Setup service with mock mapper
        AssessmentUtilServiceV2Impl service = newBareService();
        ReflectionTestUtils.setField(service, "mapper", new ObjectMapper());

        Map<String, Object> result = service.validateQumlAssessmentV3(
                questionSetDetailsMap,
                originalQuestionList,
                userQuestionList,
                questionMap
        );

        assertNotNull(result);
        assertTrue(result.containsKey(Constants.RESULT));
        assertEquals(0.0, (Double) result.get(Constants.RESULT));
    }

    @Test
    void testFilterQuestionMapDetailV2_AllScenarios() {
        // Setup service and mocks
        AssessmentUtilServiceV2Impl service = newBareService();
        CbExtAssessmentServerProperties mockProps = mock(CbExtAssessmentServerProperties.class);
        ReflectionTestUtils.setField(service, "serverProperties", mockProps);

        when(mockProps.getAssessmentQuestionParams()).thenReturn(List.of("identifier", "primaryCategory", "questionType"));
        when(mockProps.getShuffleAllowedQTypes()).thenReturn(List.of("MCQ-SCA"));

        // Input question map
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put("identifier", "q1");
        questionMap.put("primaryCategory", Constants.MTF_QUESTION);
        questionMap.put("questionType", "mcq");

        // Add editorState (should be picked because it's a practice question)
        Map<String, Object> editorState = Map.of("someKey", "someVal");
        questionMap.put(Constants.EDITOR_STATE, editorState);

        // Add choices
        Map<String, Object> option = new HashMap<>();
        option.put("index", 1);
        option.put("text", "Option A");
        Map<String, Object> choices = Map.of(Constants.OPTIONS, List.of(option));
        questionMap.put(Constants.CHOICES, choices);

        // Add RHS_CHOICES
        questionMap.put(Constants.RHS_CHOICES, new ArrayList<>(List.of("A", "B", "C")));

        // Call method with PRACTICE_QUESTION_SET
        Map<String, Object> result = service.filterQuestionMapDetailV2(questionMap, Constants.PRACTICE_QUESTION_SET, true);

        // Assertions
        assertEquals("q1", result.get("identifier"));
        assertEquals(Constants.MTF_QUESTION, result.get("primaryCategory"));
        assertEquals("mcq", result.get("questionType"));
        assertEquals(editorState, result.get(Constants.EDITOR_STATE));
        assertTrue(result.containsKey(Constants.CHOICES));
        assertTrue(result.containsKey(Constants.RHS_CHOICES));
    }

    @Test
    void testCalculatePassPercentage_OptionWeightage() {
        Double sectionMarks = 0.0;
        Integer totalMarks = 0;
        Integer correct = 6, blank = 2, inCorrect = 2;
        String assessmentType = Constants.OPTION_WEIGHTAGE;
        Map<String, Object> resultMap = new HashMap<>();

        // Invoke
        ReflectionTestUtils.invokeMethod(
                AssessmentUtilServiceV2Impl.class,
                "calculatePassPercentage",
                sectionMarks,
                totalMarks,
                correct,
                blank,
                inCorrect,
                assessmentType,
                resultMap
        );

        // Verify
        assertEquals(60.0, resultMap.get(Constants.RESULT));
        assertEquals(10, resultMap.get(Constants.TOTAL));
    }

    @Test
    void testQListFromCache_CacheHit() throws Exception {
        String assessmentId = "test123";
        String token = "dummyToken";

        // Mocked cached JSON string
        Map<String, Object> cachedData = Map.of("q1", Map.of("identifier", "q1"));
        String cachedStr = new ObjectMapper().writeValueAsString(cachedData);

        when(redisCacheMgr.getCache(Constants.ASSESSMENT_ID + assessmentId + Constants.UNDER_SCORE + Constants.QUESTIONS))
                .thenReturn(cachedStr);

        when(mapper.readValue(eq(cachedStr), any(TypeReference.class))).thenReturn(cachedData);

        // Call method
        Map<String, Object> actual = utilService.qListFromCache(assessmentId, false, token);

        // Assertions
        assertNotNull(actual);
        assertEquals("q1", ((Map<String, Object>) actual.get("q1")).get("identifier"));
    }

    @Test
    void testQListFrmAssessService_EmptyQuestionList_Reflection() throws Exception {
        List<String> questionIds = List.of();

        // Setup spy to override the private method readQuestionDetails
        AssessmentUtilServiceV2Impl spyService = Mockito.spy(utilService);
        Method readMethod = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("qListFrmAssessService", List.class);
        readMethod.setAccessible(true);

        // Mock internal method readQuestionDetails via reflection as well if needed
        doReturn(Collections.emptyList()).when(spyService).readQuestionDetails(questionIds);

        // Invoke private method
        Map<String, Object> result = (Map<String, Object>) readMethod.invoke(spyService, questionIds);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testQListFrmAssessService_ValidQuestionDetails_Reflection() throws Exception {
        List<String> questionIds = List.of("q1", "q2");

        Map<String, Object> q1 = new HashMap<>();
        q1.put(Constants.IDENTIFIER, "q1");

        Map<String, Object> q2 = new HashMap<>();
        q2.put(Constants.IDENTIFIER, "q2");

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(Constants.QUESTIONS, List.of(q1, q2));

        Map<String, Object> outerMap = new HashMap<>();
        outerMap.put(Constants.RESULT, resultMap);

        // Spy to mock internal call
        AssessmentUtilServiceV2Impl spyService = Mockito.spy(utilService);
        doReturn(List.of(outerMap)).when(spyService).readQuestionDetails(questionIds);

        // Use reflection to invoke private method
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("qListFrmAssessService", List.class);
        method.setAccessible(true);

        Map<String, Object> result = (Map<String, Object>) method.invoke(spyService, questionIds);

        // Assertions
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("q1"));
        assertTrue(result.containsKey("q2"));
    }

    @Test
    void testReadQListfromCache_CacheEnabled_NotEditMode() throws Exception {
        List<String> questionIds = List.of("q1", "q2");
        String assessmentId = "assessment123";
        String token = "dummyToken";

        when(serverProperties.qListFromCacheEnabled()).thenReturn(true);

        // Use reflection to stub qListFromCache
        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("qListFromCache", String.class, boolean.class, String.class);
        method.setAccessible(true);

        // Manually inject the expected behavior (if needed, skip this and just rely on actual logic)
        // Or let it execute actual method (better coverage)

        // Execute
        Map<String, Object> actual = utilService.readQListfromCache(questionIds, assessmentId, false, token);

        // Assert
        assertNotNull(actual);
    }

    @Test
    void testReadQListfromCache_CacheDisabledOrEditModeTrue() throws Exception {
        List<String> questionIds = List.of("q1", "q2");
        String assessmentId = "assessment123";
        String token = "dummyToken";

        // Reflectively invoke private method
        Method privateMethod = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("qListFrmAssessService", List.class);
        privateMethod.setAccessible(true); // Bypass private access

        @SuppressWarnings("unchecked")
        Map<String, Object> qListResult = (Map<String, Object>) privateMethod.invoke(utilService, questionIds);

        // Now stub readQListfromCache to return what private method returned
        assertNotNull(qListResult); // optionally validate reflective result

        // Call actual method under test
        Map<String, Object> result1 = utilService.readQListfromCache(questionIds, assessmentId, false, token);
        assertEquals(qListResult, result1);

        // Case 2: Cache enabled, but edit mode is true
        when(serverProperties.qListFromCacheEnabled()).thenReturn(true);

        Map<String, Object> result2 = utilService.readQListfromCache(questionIds, assessmentId, true, token);
        assertEquals(qListResult, result2);
    }
    @Test
    void testReadUserSubmittedAssessmentRecords() {
        List<Map<String, Object>> records = List.of(Map.of("test", "value"));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), isNull()))
                .thenReturn(records);

        List<Map<String, Object>> result = utilService.readUserSubmittedAssessmentRecords("user1", "assess1");
        assertEquals(1, result.size());
    }

    @Test
    void testGetQumlAnswers_WhenQuestionTypeMissingButOptionsExist() throws Exception {
        String qid = "q1";
        Map<String, Object> question = new HashMap<>();
        List<Map<String, Object>> options = new ArrayList<>();
        Map<String, Object> opt = new HashMap<>();
        opt.put(Constants.IS_CORRECT, true);
        opt.put(Constants.OPTION_ID, "opt1");
        options.add(opt);
        question.put(Constants.OPTIONS, options);
        question.put(Constants.IDENTIFIER, qid);

        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, List.of(qid), questionMap);

        assertEquals(List.of("opt1"), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_WhenEditorStateIsEmpty() throws Exception {
        String qid = "q2";
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA);
        question.put(Constants.EDITOR_STATE, new HashMap<>()); // editor state is empty

        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, List.of(qid), questionMap);

        assertEquals(Collections.emptyList(), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_MTF_WithValueCheck() throws Exception {
        String qid = "q3";
        Map<String, Object> value = Map.of(Constants.VALUE, "val");
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.VALUE, value);
        option.put(Constants.ANSWER, true);
        List<Map<String, Object>> options = List.of(option);
        Map<String, Object> editorState = Map.of(Constants.OPTIONS, options);
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.MTF);
        question.put(Constants.EDITOR_STATE, editorState);

        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, List.of(qid), questionMap);

        assertEquals(List.of("val-true"), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_FTB_WithBodyCheck() throws Exception {
        String qid = "q4";
        Map<String, Object> value = Map.of(Constants.BODY, "D");
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.VALUE, value);
        option.put(Constants.ANSWER, true);
        List<Map<String, Object>> options = List.of(option);
        Map<String, Object> editorState = Map.of(Constants.OPTIONS, options);
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.FTB);
        question.put(Constants.EDITOR_STATE, editorState);

        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, List.of(qid), questionMap);

        assertEquals(List.of("D"), result.get(qid));
    }

    @Test
    void testGetQumlAnswers_MCQ_WithValueCheck() throws Exception {
        String qid = "q5";
        Map<String, Object> value = Map.of(Constants.VALUE, "A");
        Map<String, Object> option = new HashMap<>();
        option.put(Constants.VALUE, value);
        option.put(Constants.ANSWER, true);
        List<Map<String, Object>> options = List.of(option);
        Map<String, Object> editorState = Map.of(Constants.OPTIONS, options);
        Map<String, Object> question = new HashMap<>();
        question.put(Constants.IDENTIFIER, qid);
        question.put(Constants.QUESTION_TYPE, Constants.MCQ_SCA_TF);
        question.put(Constants.EDITOR_STATE, editorState);

        Map<String, Object> questionMap = Map.of(qid, question);

        Method method = AssessmentUtilServiceV2Impl.class.getDeclaredMethod("getQumlAnswers", List.class, Map.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(utilService, List.of(qid), questionMap);

        assertEquals(List.of("A"), result.get(qid));
    }
    @Test
    void testFetchQuestionIdentifierValue_EmptyInputList() {
        List<String> identifiers = Collections.emptyList();
        List<Object> questionList = new ArrayList<>();

        String result = utilService.fetchQuestionIdentifierValue(identifiers, questionList, "category");

        assertEquals("", result); // branch skip path
    }

    @Test
    void testFilterQuestionMapDetail_WithEditorStateAndPracticeCategory() {
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.IDENTIFIER, "q1");
        questionMap.put(Constants.EDITOR_STATE, Map.of("someKey", "someVal"));

        when(serverProperties.getAssessmentQuestionParams()).thenReturn(List.of(Constants.IDENTIFIER));

        Map<String, Object> result = utilService.filterQuestionMapDetail(questionMap, Constants.PRACTICE_QUESTION_SET, true);

        assertTrue(result.containsKey(Constants.EDITOR_STATE));
        assertEquals("q1", result.get(Constants.IDENTIFIER));
    }

    @Test
    void testFilterQuestionMapDetail_WithChoicesNotFTB() {
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.PRIMARY_CATEGORY, "MCQ");
        questionMap.put(Constants.IDENTIFIER, "q1");
        questionMap.put(Constants.QUESTION_TYPE, "MCQ-SCA");
        questionMap.put(Constants.CHOICES, Map.of(Constants.OPTIONS, List.of(Map.of("key", "value"))));

        when(serverProperties.getAssessmentQuestionParams()).thenReturn(List.of(Constants.IDENTIFIER, Constants.PRIMARY_CATEGORY, Constants.QUESTION_TYPE));
        when(serverProperties.getShuffleAllowedQTypes()).thenReturn(List.of("MCQ-SCA"));

        Map<String, Object> result = utilService.filterQuestionMapDetail(questionMap, "anyCategory", true);

        assertTrue(result.containsKey(Constants.CHOICES));
        Map<String, Object> choices = (Map<String, Object>) result.get(Constants.CHOICES);
        assertTrue(choices.containsKey(Constants.OPTIONS));
    }

    @Test
    void testFilterQuestionMapDetail_WithRHSChoicesMTF() {
        List<Object> rhsOptions = new ArrayList<>(List.of("A", "B"));
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.PRIMARY_CATEGORY, Constants.MTF_QUESTION);
        questionMap.put(Constants.IDENTIFIER, "q2");
        questionMap.put(Constants.RHS_CHOICES, rhsOptions);

        when(serverProperties.getAssessmentQuestionParams()).thenReturn(List.of(Constants.IDENTIFIER, Constants.PRIMARY_CATEGORY));

        Map<String, Object> result = utilService.filterQuestionMapDetail(questionMap, "anyCategory", true);

        assertTrue(result.containsKey(Constants.RHS_CHOICES));
        assertEquals(2, ((List<?>) result.get(Constants.RHS_CHOICES)).size());
    }

    @Test
    void testFilterQuestionMapDetail_WithoutChoicesOrRHS() {
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.IDENTIFIER, "q3");
        questionMap.put(Constants.PRIMARY_CATEGORY, "FTB");

        when(serverProperties.getAssessmentQuestionParams()).thenReturn(List.of(Constants.IDENTIFIER, Constants.PRIMARY_CATEGORY));

        Map<String, Object> result = utilService.filterQuestionMapDetail(questionMap, "nonPractice", true);

        assertEquals("q3", result.get(Constants.IDENTIFIER));
        assertFalse(result.containsKey(Constants.CHOICES));
        assertFalse(result.containsKey(Constants.RHS_CHOICES));
    }

    @Test
    void testWithEditorStateAndPracticeCategory() {
        List<String> questionParams = List.of("identifier", "primaryCategory");
        when(serverProperties.getAssessmentQuestionParams()).thenReturn(questionParams);

        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("identifier", "q1");
        inputMap.put("primaryCategory", "practice");
        inputMap.put(Constants.EDITOR_STATE, Map.of("foo", "bar"));

        Map<String, Object> output = utilService.filterQuestionMapDetail(inputMap, Constants.PRACTICE_QUESTION_SET, true);

        assertEquals("q1", output.get("identifier"));
        assertTrue(output.containsKey(Constants.EDITOR_STATE));
    }

    

    @Test
    void testIsEnrolmentActive_WithBooleanTrue() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        enrolmentRecord.put(Constants.ACTIVE, true);
        boolean result = invokePrivateMethod("isEnrolmentActive",
            enrolmentRecord, "user123", "assessment123");
        assertTrue(result, "Should return true when active is Boolean true");
    }

    @Test
    void testIsEnrolmentActive_WithBooleanFalse() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        enrolmentRecord.put(Constants.ACTIVE, false);
        boolean result = invokePrivateMethod("isEnrolmentActive",
            enrolmentRecord, "user123", "assessment123");
        assertFalse(result, "Should return false when active is Boolean false");
    }

    @Test
    void testIsEnrolmentActive_WithStringTrue() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        enrolmentRecord.put(Constants.ACTIVE, "true");
        boolean result = invokePrivateMethod("isEnrolmentActive",
            enrolmentRecord, "user123", "assessment123");
        assertTrue(result, "Should return true when active is String 'true'");
    }

    @Test
    void testIsEnrolmentActive_WithStringFalse() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        enrolmentRecord.put(Constants.ACTIVE, "false");
        boolean result = invokePrivateMethod("isEnrolmentActive",
            enrolmentRecord, "user123", "assessment123");
        assertFalse(result, "Should return false when active is String 'false'");
    }

    @Test
    void testIsEnrolmentActive_WithNull() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        boolean result = invokePrivateMethod("isEnrolmentActive",
            enrolmentRecord, "user123", "assessment123");
        assertFalse(result, "Should return false when active field is null");
    }

    @Test
    void testGetRecentLanguage_Success() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        enrolmentRecord.put(Constants.RECENT_LANGUAGE, "english");
        String result = invokePrivateMethod("getRecentLanguage",
            enrolmentRecord, "user123", "assessment123");
        assertEquals("english", result, "Should return the recent language");
    }

    @Test
    void testGetRecentLanguage_Null() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        String result = invokePrivateMethod("getRecentLanguage",
            enrolmentRecord, "user123", "assessment123");
        assertNull(result, "Should return null when recent_language field is missing");
    }

    @Test
    void testIsAssessmentStatusCompleted_Success() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        Map<String, Integer> assessmentStatus = new HashMap<>();
        assessmentStatus.put("assessment123", 2); // Status 2 = completed
        Map<String, Map<String, Integer>> langContentStatus = new HashMap<>();
        langContentStatus.put("english", assessmentStatus);
        enrolmentRecord.put(Constants.LANG_CONTENT_STATUS, langContentStatus);
        boolean result = invokePrivateMethod("isAssessmentStatusCompleted",
            enrolmentRecord, "assessment123", "user123", "english");
        assertTrue(result, "Should return true when assessment status is 2");
    }

    @Test
    void testIsAssessmentStatusCompleted_NotCompleted() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        Map<String, Integer> assessmentStatus = new HashMap<>();
        assessmentStatus.put("assessment123", 1); // Status 1 = in progress
        Map<String, Map<String, Integer>> langContentStatus = new HashMap<>();
        langContentStatus.put("english", assessmentStatus);
        enrolmentRecord.put(Constants.LANG_CONTENT_STATUS, langContentStatus);
        boolean result = invokePrivateMethod("isAssessmentStatusCompleted",
            enrolmentRecord, "assessment123", "user123", "english");
        assertFalse(result, "Should return false when assessment status is not 2");
    }

    @Test
    void testIsAssessmentStatusCompleted_LanguageNotFound() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        Map<String, Integer> assessmentStatus = new HashMap<>();
        assessmentStatus.put("assessment123", 2);
        Map<String, Map<String, Integer>> langContentStatus = new HashMap<>();
        langContentStatus.put("hindi", assessmentStatus); // Different language
        enrolmentRecord.put(Constants.LANG_CONTENT_STATUS, langContentStatus);
        boolean result = invokePrivateMethod("isAssessmentStatusCompleted",
            enrolmentRecord, "assessment123", "user123", "english");
        assertFalse(result, "Should return false when language not found");
    }

    @Test
    void testIsAssessmentStatusCompleted_AssessmentNotFound() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        Map<String, Integer> assessmentStatus = new HashMap<>();
        assessmentStatus.put("other_assessment", 2);
        Map<String, Map<String, Integer>> langContentStatus = new HashMap<>();
        langContentStatus.put("english", assessmentStatus);
        enrolmentRecord.put(Constants.LANG_CONTENT_STATUS, langContentStatus);
        boolean result = invokePrivateMethod("isAssessmentStatusCompleted",
            enrolmentRecord, "assessment123", "user123", "english");
        assertFalse(result, "Should return false when assessment not found in map");
    }

    @Test
    void testIsAssessmentStatusCompleted_NullLangContentStatus() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        boolean result = invokePrivateMethod("isAssessmentStatusCompleted",
            enrolmentRecord, "assessment123", "user123", "english");
        assertFalse(result, "Should return false when lang_contentstatus is null");
    }

    @Test
    void testIsAssessmentCompletedInEnrolment_AllConditionsMet() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        enrolmentRecord.put(Constants.ACTIVE, true);
        enrolmentRecord.put(Constants.RECENT_LANGUAGE, "english");
        Map<String, Integer> assessmentStatus = new HashMap<>();
        assessmentStatus.put("assessment123", 2);
        Map<String, Map<String, Integer>> langContentStatus = new HashMap<>();
        langContentStatus.put("english", assessmentStatus);
        enrolmentRecord.put(Constants.LANG_CONTENT_STATUS, langContentStatus);
        boolean result = invokePrivateMethod("isAssessmentCompletedInEnrolment",
            enrolmentRecord, "assessment123", "user123");
        assertTrue(result, "Should return true when all conditions are met");
    }

    @Test
    void testIsAssessmentCompletedInEnrolment_NotActive() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        enrolmentRecord.put(Constants.ACTIVE, false);
        enrolmentRecord.put(Constants.RECENT_LANGUAGE, "english");
        boolean result = invokePrivateMethod("isAssessmentCompletedInEnrolment",
            enrolmentRecord, "assessment123", "user123");
        assertFalse(result, "Should return false when enrolment is not active");
    }

    @Test
    void testIsAssessmentCompletedInEnrolment_NoRecentLanguage() {
        Map<String, Object> enrolmentRecord = new HashMap<>();
        enrolmentRecord.put(Constants.ACTIVE, true);
        boolean result = invokePrivateMethod("isAssessmentCompletedInEnrolment",
            enrolmentRecord, "assessment123", "user123");
        assertFalse(result, "Should return false when recent_language is missing");
    }

    @Test
    void testIsCourseEnrolmentActiveAndCompleted_BothConditionsMet() {
        Map<String, Object> enrolment = new HashMap<>();
        enrolment.put(Constants.COURSE_ID, "course123");
        enrolment.put(Constants.ACTIVE, true);
        enrolment.put(Constants.STATUS, 2); // ASSESSMENT_STATUS_COMPLETED
        boolean result = invokePrivateMethod("isCourseEnrolmentActiveAndCompleted",
            enrolment, "user123");
        assertTrue(result, "Should return true when both active and completed");
    }

    @Test
    void testIsCourseEnrolmentActiveAndCompleted_NotActive() {
        Map<String, Object> enrolment = new HashMap<>();
        enrolment.put(Constants.COURSE_ID, "course123");
        enrolment.put(Constants.ACTIVE, false);
        enrolment.put(Constants.STATUS, 2);
        boolean result = invokePrivateMethod("isCourseEnrolmentActiveAndCompleted",
            enrolment, "user123");
        assertFalse(result, "Should return false when course is not active");
    }

    @Test
    void testIsCourseEnrolmentActiveAndCompleted_NotCompleted() {
        Map<String, Object> enrolment = new HashMap<>();
        enrolment.put(Constants.COURSE_ID, "course123");
        enrolment.put(Constants.ACTIVE, true);
        enrolment.put(Constants.STATUS, 1); // Not completed
        boolean result = invokePrivateMethod("isCourseEnrolmentActiveAndCompleted",
            enrolment, "user123");
        assertFalse(result, "Should return false when course is not completed");
    }

    @Test
    void testIsAllCourseCompletedV2_AllCoursesValid() {
        String userId = "user123";
        List<String> courseIds = Arrays.asList("course1", "course2");
        List<Map<String, Object>> enrolments = new ArrayList<>();
        Map<String, Object> enrolment1 = new HashMap<>();
        enrolment1.put(Constants.COURSE_ID, "course1");
        enrolment1.put(Constants.ACTIVE, true);
        enrolment1.put(Constants.STATUS, 2);
        enrolments.add(enrolment1);
        Map<String, Object> enrolment2 = new HashMap<>();
        enrolment2.put(Constants.COURSE_ID, "course2");
        enrolment2.put(Constants.ACTIVE, true);
        enrolment2.put(Constants.STATUS, 2);
        enrolments.add(enrolment2);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD_COURSES),
            eq(Constants.TABLE_USER_ENROLMENT),
            any(),
            anyList()
        )).thenReturn(enrolments);
        boolean result = invokePrivateMethod("isAllCourseCompletedV2", userId, courseIds);
        assertTrue(result, "Should return true when all courses are active and completed");
    }

    @Test
    void testIsAllCourseCompletedV2_OneCourseNotActive() {
        String userId = "user123";
        List<String> courseIds = Arrays.asList("course1", "course2");
        List<Map<String, Object>> enrolments = new ArrayList<>();
        Map<String, Object> enrolment1 = new HashMap<>();
        enrolment1.put(Constants.COURSE_ID, "course1");
        enrolment1.put(Constants.ACTIVE, true);
        enrolment1.put(Constants.STATUS, 2);
        enrolments.add(enrolment1);
        Map<String, Object> enrolment2 = new HashMap<>();
        enrolment2.put(Constants.COURSE_ID, "course2");
        enrolment2.put(Constants.ACTIVE, false); // Not active
        enrolment2.put(Constants.STATUS, 2);
        enrolments.add(enrolment2);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD_COURSES),
            eq(Constants.TABLE_USER_ENROLMENT),
            any(),
            anyList()
        )).thenReturn(enrolments);
        boolean result = invokePrivateMethod("isAllCourseCompletedV2", userId, courseIds);
        assertFalse(result, "Should return false when one course is not active");
    }

    @Test
    void testIsAllCourseCompletedV2_OneCourseNotCompleted() {
        String userId = "user123";
        List<String> courseIds = Arrays.asList("course1", "course2");
        List<Map<String, Object>> enrolments = new ArrayList<>();
        Map<String, Object> enrolment1 = new HashMap<>();
        enrolment1.put(Constants.COURSE_ID, "course1");
        enrolment1.put(Constants.ACTIVE, true);
        enrolment1.put(Constants.STATUS, 2);
        enrolments.add(enrolment1);
        Map<String, Object> enrolment2 = new HashMap<>();
        enrolment2.put(Constants.COURSE_ID, "course2");
        enrolment2.put(Constants.ACTIVE, true);
        enrolment2.put(Constants.STATUS, 1);
        enrolments.add(enrolment2);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD_COURSES),
            eq(Constants.TABLE_USER_ENROLMENT),
            any(),
            anyList()
        )).thenReturn(enrolments);
        boolean result = invokePrivateMethod("isAllCourseCompletedV2", userId, courseIds);
        assertFalse(result, "Should return false when one course is not completed");
    }

    @Test
    void testIsAllCourseCompletedV2_EmptyCourseIds() {
        String userId = "user123";
        List<String> courseIds = new ArrayList<>();
        boolean result = invokePrivateMethod("isAllCourseCompletedV2", userId, courseIds);
        assertFalse(result, "Should return false when course IDs list is empty");
    }

    @Test
    void testIsAllCourseCompletedV2_NullCourseIds() {
        String userId = "user123";
        List<String> courseIds = null;
        boolean result = invokePrivateMethod("isAllCourseCompletedV2", userId, courseIds);
        assertFalse(result, "Should return false when course IDs list is null");
    }

    @Test
    void testIsAllCourseCompletedV2_InsufficientEnrolments() {
        String userId = "user123";
        List<String> courseIds = Arrays.asList("course1", "course2", "course3");
        List<Map<String, Object>> enrolments = new ArrayList<>();
        Map<String, Object> enrolment1 = new HashMap<>();
        enrolment1.put(Constants.COURSE_ID, "course1");
        enrolment1.put(Constants.ACTIVE, true);
        enrolment1.put(Constants.STATUS, 2);
        enrolments.add(enrolment1);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD_COURSES),
            eq(Constants.TABLE_USER_ENROLMENT),
            any(),
            anyList()
        )).thenReturn(enrolments);
        boolean result = invokePrivateMethod("isAllCourseCompletedV2", userId, courseIds);
        assertFalse(result, "Should return false when enrolments size < courseIds size");
    }

    private <T> T invokePrivateMethod(String methodName, Object... args) {
        try {
            Class<?>[] paramTypes = new Class<?>[args.length];
            for (int i = 0; i < args.length; i++) {
                if (args[i] == null) {
                    paramTypes[i] = null;
                } else if (args[i] instanceof String) {
                    paramTypes[i] = String.class;
                } else if (args[i] instanceof Map) {
                    paramTypes[i] = Map.class;
                } else if (args[i] instanceof List) {
                    paramTypes[i] = List.class;
                } else {
                    paramTypes[i] = args[i].getClass();
                }
            }
            Method method = findMethod(methodName, paramTypes);
            method.setAccessible(true);
            return (T) method.invoke(utilService, args);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke method: " + methodName, e);
        }
    }
    
    private Method findMethod(String methodName, Class<?>[] paramTypes) throws NoSuchMethodException {
        Method[] methods = AssessmentUtilServiceV2Impl.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals(methodName) && method.getParameterCount() == paramTypes.length) {
                Class<?>[] methodParamTypes = method.getParameterTypes();
                boolean matches = true;
                for (int i = 0; i < paramTypes.length; i++) {
                    if (paramTypes[i] == null) {
                        continue;
                    }
                    if (!methodParamTypes[i].isAssignableFrom(paramTypes[i])) {
                        matches = false;
                        break;
                    }
                }
                if (matches) {
                    return method;
                }
            }
        }
        throw new NoSuchMethodException("Method not found: " + methodName);
    }

    @Test
    void testValidateLearningPathwayAssessment_PreliminaryAssessmentMatches() {
        Map<String, Object> contentRead = new HashMap<>();
        String preliminaryAssessmentId = "prelim-assess-123";
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, preliminaryAssessmentId);
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        String assessmentIdFromRequest = "prelim-assess-123";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateLearningPathwayAssessment_PreliminaryAssessmentDoesNotMatch() {
        Map<String, Object> contentRead = new HashMap<>();
        String preliminaryAssessmentId = "prelim-assess-123";
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, preliminaryAssessmentId);
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        String assessmentIdFromRequest = "other-assess-456";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        when(serverProperties.getAssessmentLearningPathwayAssessmentNotFoundError())
                .thenReturn("Assessment " + Constants.ASSESSMENT_ID_REPLACER + " not found in Learning Pathway for course " + Constants.COURSE_ID_REPLACER);
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertNotEquals(Constants.EMPTY, result);
        assertNotNull(result);
        assertTrue(result.contains("other-assess-456"));
        assertTrue(result.contains("course-123"));
    }

    @Test
    void testValidateLearningPathwayAssessment_EmptyPreliminaryAssessment_NoMilestones() {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, "");
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        String assessmentIdFromRequest = "assess-123";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        when(serverProperties.getAssessmentLearningPathwayAssessmentNotFoundError())
                .thenReturn("Assessment " + Constants.ASSESSMENT_ID_REPLACER + " not found in Learning Pathway for course " + Constants.COURSE_ID_REPLACER);
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertNotEquals(Constants.EMPTY, result);
        assertNotNull(result);
        assertTrue(result.contains("course-123"));
        assertTrue(result.contains("assess-123"));
    }

    @Test
    void testValidateLearningPathwayAssessment_NullMilestones() {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, "");
        String assessmentIdFromRequest = "assess-123";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        when(serverProperties.getAssessmentLearningPathwayAssessmentNotFoundError())
                .thenReturn("Assessment " + Constants.ASSESSMENT_ID_REPLACER + " not found in Learning Pathway for course " + Constants.COURSE_ID_REPLACER);
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                null,
                assessmentIdFromRequest,
                submitRequest
        );
        assertNotEquals(Constants.EMPTY, result);
        assertNotNull(result);
        assertTrue(result.contains("course-123"));
        assertTrue(result.contains("assess-123"));
    }

    @ParameterizedTest
    @MethodSource("provideMilestoneTestData")
    void testValidateLearningPathwayAssessment_MilestoneScenarios(String scenarioName, String assessmentIdFromRequest, String expectedResult, List<Map<String, Object>> milestonesV1) {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, "");
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        if (!"empty".equals(expectedResult)) {
            when(serverProperties.getAssessmentLearningPathwayAssessmentNotFoundError())
                    .thenReturn("Assessment " + Constants.ASSESSMENT_ID_REPLACER + " not found in Learning Pathway for course " + Constants.COURSE_ID_REPLACER);
        }
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        if ("empty".equals(expectedResult)) {
            assertEquals(Constants.EMPTY, result);
        } else {
            assertNotEquals(Constants.EMPTY, result);
            assertNotNull(result);
            assertTrue(result.contains(assessmentIdFromRequest));
            assertTrue(result.contains("course-123"));
        }
    }

    private static Stream<Arguments> provideMilestoneTestData() {
        Map<String, Object> assessmentDetailFound = new HashMap<>();
        assessmentDetailFound.put(Constants.IDENTIFIER, "assess-123");
        Map<String, Object> milestoneFound = new HashMap<>();
        milestoneFound.put(Constants.ASSESSMENT_DETAIL, assessmentDetailFound);
        List<Map<String, Object>> milestonesWithMatch = new ArrayList<>();
        milestonesWithMatch.add(milestoneFound);
        Map<String, Object> assessmentDetailNotFound = new HashMap<>();
        assessmentDetailNotFound.put(Constants.IDENTIFIER, "different-assess-456");
        Map<String, Object> milestoneNotFound = new HashMap<>();
        milestoneNotFound.put(Constants.ASSESSMENT_DETAIL, assessmentDetailNotFound);
        List<Map<String, Object>> milestonesWithoutMatch = new ArrayList<>();
        milestonesWithoutMatch.add(milestoneNotFound);
        Map<String, Object> assessmentDetail1 = new HashMap<>();
        assessmentDetail1.put(Constants.IDENTIFIER, "assess-001");
        Map<String, Object> milestone1 = new HashMap<>();
        milestone1.put(Constants.ASSESSMENT_DETAIL, assessmentDetail1);
        Map<String, Object> assessmentDetail2 = new HashMap<>();
        assessmentDetail2.put(Constants.IDENTIFIER, "assess-123");
        Map<String, Object> milestone2 = new HashMap<>();
        milestone2.put(Constants.ASSESSMENT_DETAIL, assessmentDetail2);
        Map<String, Object> assessmentDetail3 = new HashMap<>();
        assessmentDetail3.put(Constants.IDENTIFIER, "assess-999");
        Map<String, Object> milestone3 = new HashMap<>();
        milestone3.put(Constants.ASSESSMENT_DETAIL, assessmentDetail3);
        List<Map<String, Object>> multipleMilestones = new ArrayList<>();
        multipleMilestones.add(milestone1);
        multipleMilestones.add(milestone2);
        multipleMilestones.add(milestone3);
        return Stream.of(
            Arguments.of("Assessment found in single milestone", "assess-123", "empty", milestonesWithMatch),
            Arguments.of("Assessment not found in milestones", "assess-123", "error", milestonesWithoutMatch),
            Arguments.of("Assessment found in multiple milestones", "assess-123", "empty", multipleMilestones)
        );
    }

    @Test
    void testValidateLearningPathwayAssessment_MilestoneWithEmptyAssessmentDetail() {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, "");
        String assessmentIdFromRequest = "assess-123";
        Map<String, Object> milestone1 = new HashMap<>();
        milestone1.put(Constants.ASSESSMENT_DETAIL, new HashMap<>());
        Map<String, Object> milestone2 = new HashMap<>();
        milestone2.put(Constants.ASSESSMENT_DETAIL, null);
        Map<String, Object> assessmentDetail3 = new HashMap<>();
        assessmentDetail3.put(Constants.IDENTIFIER, assessmentIdFromRequest);
        Map<String, Object> milestone3 = new HashMap<>();
        milestone3.put(Constants.ASSESSMENT_DETAIL, assessmentDetail3);
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        milestonesV1.add(milestone1);
        milestonesV1.add(milestone2);
        milestonesV1.add(milestone3);
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateLearningPathwayAssessment_MilestoneWithoutAssessmentDetail() {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, "");
        String assessmentIdFromRequest = "assess-123";
        Map<String, Object> milestone = new HashMap<>();
        milestone.put("otherKey", "otherValue");
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        milestonesV1.add(milestone);
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        when(serverProperties.getAssessmentLearningPathwayAssessmentNotFoundError())
                .thenReturn("Assessment " + Constants.ASSESSMENT_ID_REPLACER + " not found in Learning Pathway for course " + Constants.COURSE_ID_REPLACER);
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertNotEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateLearningPathwayAssessment_NullPreliminaryAssessment() {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, null);
        String assessmentIdFromRequest = "assess-123";
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.IDENTIFIER, assessmentIdFromRequest);
        Map<String, Object> milestone = new HashMap<>();
        milestone.put(Constants.ASSESSMENT_DETAIL, assessmentDetail);
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        milestonesV1.add(milestone);
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateLearningPathwayAssessment_CaseInsensitivePreliminaryMatch() {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, "PRELIM-ASSESS-123");
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        String assessmentIdFromRequest = "prelim-assess-123";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateLearningPathwayAssessment_AllMilestonesHaveEmptyAssessmentDetails() {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, "");
        String assessmentIdFromRequest = "assess-123";
        Map<String, Object> milestone1 = new HashMap<>();
        milestone1.put(Constants.ASSESSMENT_DETAIL, new HashMap<>());
        Map<String, Object> milestone2 = new HashMap<>();
        milestone2.put(Constants.ASSESSMENT_DETAIL, new HashMap<>());
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        milestonesV1.add(milestone1);
        milestonesV1.add(milestone2);
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        when(serverProperties.getAssessmentLearningPathwayAssessmentNotFoundError())
                .thenReturn("Assessment " + Constants.ASSESSMENT_ID_REPLACER + " not found in Learning Pathway for course " + Constants.COURSE_ID_REPLACER);
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertNotEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateLearningPathwayAssessment_ContentReadWithoutPreliminaryAssessmentKey() {
        Map<String, Object> contentRead = new HashMap<>();
        String assessmentIdFromRequest = "assess-123";
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.IDENTIFIER, assessmentIdFromRequest);
        Map<String, Object> milestone = new HashMap<>();
        milestone.put(Constants.ASSESSMENT_DETAIL, assessmentDetail);
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        milestonesV1.add(milestone);
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateLearningPathwayAssessment_MixedScenarioWithWhitespace() {
        Map<String, Object> contentRead = new HashMap<>();
        contentRead.put(Constants.PRELIMINARY_ASSESSMENT, "  ");
        String assessmentIdFromRequest = "assess-123";
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.IDENTIFIER, assessmentIdFromRequest);
        Map<String, Object> milestone = new HashMap<>();
        milestone.put(Constants.ASSESSMENT_DETAIL, assessmentDetail);
        List<Map<String, Object>> milestonesV1 = new ArrayList<>();
        milestonesV1.add(milestone);
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put(Constants.COURSE_ID, "course-123");
        String result = ReflectionTestUtils.invokeMethod(
                utilService,
                "validateLearningPathwayAssessment",
                contentRead,
                milestonesV1,
                assessmentIdFromRequest,
                submitRequest
        );
        assertEquals(Constants.EMPTY, result);
    }


    @Test
    void testCalculateCyclicalRetakeAttempts_NullList_ReturnsZero() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, null);
        assertEquals(0, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_EmptyList_ReturnsZero() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, new ArrayList<>());
        assertEquals(0, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_NoSubmittedAttempts_ReturnsZero() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt1 = new HashMap<>();
        attempt1.put(Constants.END_TIME, Instant.now());
        attempts.add(attempt1);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(0, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_SingleAttempt_ReturnsOne() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt1 = new HashMap<>();
        attempt1.put(Constants.END_TIME, Instant.now());
        attempt1.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt1);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(1, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_ThreeAttemptsWithinSameDay_ReturnsThree() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt1 = new HashMap<>();
        attempt1.put(Constants.END_TIME, now);
        attempt1.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt1);
        Map<String, Object> attempt2 = new HashMap<>();
        attempt2.put(Constants.END_TIME, now.minusSeconds(2 * 3600));
        attempt2.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt2);
        Map<String, Object> attempt3 = new HashMap<>();
        attempt3.put(Constants.END_TIME, now.minusSeconds(4 * 3600));
        attempt3.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt3);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(3, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_SixAttemptsExhausted_ReturnsSix() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            Map<String, Object> attempt = new HashMap<>();
            attempt.put(Constants.END_TIME, now.minusSeconds(i * 3600)); // Each 1 hour apart
            attempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(attempt);
        }
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(6, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_CycleBoundaryDetected_FreshCycle_ReturnsOne() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1); // 1 day
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> newAttempt = new HashMap<>();
        newAttempt.put(Constants.END_TIME, now);
        newAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(newAttempt);
        for (int i = 0; i < 6; i++) {
            Map<String, Object> oldAttempt = new HashMap<>();
            oldAttempt.put(Constants.END_TIME, now.minusSeconds((5 * 86400) + (i * 3600))); // 5 days ago
            oldAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(oldAttempt);
        }
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(1, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_CycleBoundary_ThreeNewAttempts_ReturnsThree() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1); // 1 day
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> newAttempt = new HashMap<>();
            newAttempt.put(Constants.END_TIME, now.minusSeconds(i * 3600)); // Each 1 hour apart
            newAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(newAttempt);
        }
        for (int i = 0; i < 6; i++) {
            Map<String, Object> oldAttempt = new HashMap<>();
            oldAttempt.put(Constants.END_TIME, now.minusSeconds((5 * 86400) + (i * 3600))); // 5 days ago
            oldAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(oldAttempt);
        }
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(3, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_SparseAttempts_NoExhaustion_ReturnsThree() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 3); // 3 days
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt1 = new HashMap<>();
        attempt1.put(Constants.END_TIME, now);
        attempt1.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt1);
        Map<String, Object> attempt2 = new HashMap<>();
        attempt2.put(Constants.END_TIME, now.minusSeconds(14 * 86400));
        attempt2.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt2);
        Map<String, Object> attempt3 = new HashMap<>();
        attempt3.put(Constants.END_TIME, now.minusSeconds(28 * 86400));
        attempt3.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt3);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(3, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_GapExistsButNotEnoughOlderAttempts_ReturnsAll() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1); // 1 day
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> newAttempt = new HashMap<>();
            newAttempt.put(Constants.END_TIME, now.minusSeconds(i * 3600));
            newAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(newAttempt);
        }
        for (int i = 0; i < 2; i++) {
            Map<String, Object> oldAttempt = new HashMap<>();
            oldAttempt.put(Constants.END_TIME, now.minusSeconds((5 * 86400) + (i * 3600)));
            oldAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(oldAttempt);
        }
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(5, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_ExactlyAtBoundary_SixOldSixNew_ReturnsSix() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1); // 1 day
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            Map<String, Object> newAttempt = new HashMap<>();
            newAttempt.put(Constants.END_TIME, now.minusSeconds(i * 3600));
            newAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(newAttempt);
        }
        for (int i = 0; i < 6; i++) {
            Map<String, Object> oldAttempt = new HashMap<>();
            oldAttempt.put(Constants.END_TIME, now.minusSeconds((2 * 86400) + (i * 3600)));
            oldAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(oldAttempt);
        }
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(6, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_OptimizationStopsEarly() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 3);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> newAttempt = new HashMap<>();
            newAttempt.put(Constants.END_TIME, now.minusSeconds(i * 3600));
            newAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(newAttempt);
        }
        for (int i = 0; i < 100; i++) {
            Map<String, Object> oldAttempt = new HashMap<>();
            oldAttempt.put(Constants.END_TIME, now.minusSeconds((5 * 86400) + (i * 3600)));
            oldAttempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempts.add(oldAttempt);
        }
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(3, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_NullEndTime_HandledGracefully() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt1 = new HashMap<>();
        attempt1.put(Constants.END_TIME, now);
        attempt1.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt1);
        Map<String, Object> attempt2 = new HashMap<>();
        attempt2.put(Constants.END_TIME, null); // Null endtime
        attempt2.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt2);
        Map<String, Object> attempt3 = new HashMap<>();
        attempt3.put(Constants.END_TIME, now.minusSeconds(7200));
        attempt3.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt3);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(0, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_DateTypeEndTime_Converted() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        assessmentDetail.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS, 6);
        Date now = new Date();
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt1 = new HashMap<>();
        attempt1.put(Constants.END_TIME, now); // Date type
        attempt1.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt1);
        Map<String, Object> attempt2 = new HashMap<>();
        attempt2.put(Constants.END_TIME, new Date(now.getTime() - 3600000)); // 1 hour ago
        attempt2.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt2);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(2, result);
    }

    @Test
    void testCalculateCyclicalRetakeAttempts_MissingMaxRetakeAttempts_DefaultsToZero() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        Instant now = Instant.now();
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt1 = new HashMap<>();
        attempt1.put(Constants.END_TIME, now);
        attempt1.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
        attempts.add(attempt1);
        int result = utilService.calculateCyclicalRetakeAttempts("user1", "assess1", assessmentDetail, attempts);
        assertEquals(1, result);
    }

    @Test
    void testHasCoolOffPeriod_ValidCoolOff_ReturnsTrue() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        boolean result = utilService.hasCoolOffPeriod(assessmentDetail);
        assertTrue(result);
    }

    @Test
    void testHasCoolOffPeriod_ZeroCoolOff_ReturnsFalse() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 0);
        boolean result = utilService.hasCoolOffPeriod(assessmentDetail);
        assertFalse(result);
    }

    @Test
    void testHasCoolOffPeriod_NegativeCoolOff_ReturnsFalse() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, -1);
        boolean result = utilService.hasCoolOffPeriod(assessmentDetail);
        assertFalse(result);
    }

    @Test
    void testHasCoolOffPeriod_NoCoolOffKey_ReturnsFalse() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        boolean result = utilService.hasCoolOffPeriod(assessmentDetail);
        assertFalse(result);
    }

    @Test
    void testHasCoolOffPeriod_NonIntegerValue_ReturnsFalse() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, "1"); // String instead of Integer
        boolean result = utilService.hasCoolOffPeriod(assessmentDetail);
        assertFalse(result);
    }

    @Test
    void testValidateCoolOffPeriod_EmptyList_ReturnsEmpty() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        String result = utilService.validateCoolOffPeriod("user1", "assess1", assessmentDetail, new ArrayList<>());
        assertEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateCoolOffPeriod_NullEndTime_ReturnsEmpty() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1);
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.END_TIME, null);
        attempts.add(attempt);
        String result = utilService.validateCoolOffPeriod("user1", "assess1", assessmentDetail, attempts);
        assertEquals(Constants.EMPTY, result);
    }

    @Test
    void testValidateCoolOffPeriod_WithinCoolOff_ReturnsErrorMessage() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 2); // 2 days
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.END_TIME, Instant.now().minusSeconds(86400)); // 1 day ago
        attempts.add(attempt);
        when(serverProperties.getAssessmentCoolOffErrorMessage())
                .thenReturn("Please wait {remainingDays} days. Cooloff period is {coolOffPeriod} days.");
        String result = utilService.validateCoolOffPeriod("user1", "assess1", assessmentDetail, attempts);
        assertNotEquals(Constants.EMPTY, result);
        assertTrue(result.contains("days"));
    }

    @Test
    void testValidateCoolOffPeriod_CoolOffExpired_ReturnsEmpty() {
        Map<String, Object> assessmentDetail = new HashMap<>();
        assessmentDetail.put(Constants.COOL_OFF_PERIOD, 1); // 1 day
        List<Map<String, Object>> attempts = new ArrayList<>();
        Map<String, Object> attempt = new HashMap<>();
        attempt.put(Constants.END_TIME, Instant.now().minusSeconds(3 * 86400)); // 3 days ago
        attempts.add(attempt);
        String result = utilService.validateCoolOffPeriod("user1", "assess1", assessmentDetail, attempts);
        assertEquals(Constants.EMPTY, result);
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
        int result = utilService.calculateRetakeAttemptsConsumed(userId, assessmentId, assessmentDetail,
                retakeAttemptsAllowed, userAttempts, response,
                new AssessmentUtilServiceV2.RetakeValidationOptions(HttpStatus.BAD_REQUEST, false));
        assertEquals(3, result);
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
        when(serverProperties.getAssessmentCoolOffErrorMessage())
                .thenReturn("Please wait {remainingDays} days. Cooloff period is {coolOffPeriod} days.");
        int result = utilService.calculateRetakeAttemptsConsumed(userId, assessmentId, assessmentDetail,
                retakeAttemptsAllowed, userAttempts, response,
                new AssessmentUtilServiceV2.RetakeValidationOptions(HttpStatus.BAD_REQUEST, false));
        assertEquals(6, result);
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
        List<Map<String, Object>> userAttempts = new ArrayList<>();
        Instant now = Instant.now();
        for (int i = 0; i < 6; i++) {
            Map<String, Object> attempt = new HashMap<>();
            attempt.put(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY, "submitted");
            attempt.put(Constants.END_TIME, now.minusSeconds(3 * 86400L + i * 3600L)); // 3+ days ago
            userAttempts.add(attempt);
        }
        int result = utilService.calculateRetakeAttemptsConsumed(userId, assessmentId, assessmentDetail,
                retakeAttemptsAllowed, userAttempts, response,
                new AssessmentUtilServiceV2.RetakeValidationOptions(HttpStatus.BAD_REQUEST, false));
        assertEquals(0, result); // New cycle starts
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testCalculateRetakeAttemptsConsumed_NonCyclicalMode_TotalAttempts_NotEnforced() {
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
        int result = utilService.calculateRetakeAttemptsConsumed(userId, assessmentId, assessmentDetail,
                retakeAttemptsAllowed, userAttempts, response,
                new AssessmentUtilServiceV2.RetakeValidationOptions(HttpStatus.BAD_REQUEST, false));
        assertEquals(4, result);
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
        int result = utilService.calculateRetakeAttemptsConsumed(userId, assessmentId, assessmentDetail,
                retakeAttemptsAllowed, new ArrayList<>(), response,
                new AssessmentUtilServiceV2.RetakeValidationOptions(HttpStatus.BAD_REQUEST, false));
        assertEquals(0, result);
    }

    @Test
    void testCalculateRetakeAttemptsConsumed_NonCyclicalMode_EnforceLimit_SetsError() {
        String userId = "user1";
        String assessmentId = "assess1";
        int retakeAttemptsAllowed = 4;
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
        int result = utilService.calculateRetakeAttemptsConsumed(userId, assessmentId, assessmentDetail,
                retakeAttemptsAllowed, userAttempts, response,
                new AssessmentUtilServiceV2.RetakeValidationOptions(HttpStatus.INTERNAL_SERVER_ERROR, true));
        assertEquals(4, result);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void testResolveAssessmentStartTimeAsInstant_Date() {
        Instant now = Instant.now();
        Instant result = utilService.resolveAssessmentStartTimeAsInstant(Date.from(now));
        assertEquals(now.getEpochSecond(), result.getEpochSecond());
    }

    @Test
    void testResolveAssessmentStartTimeAsInstant_Instant() {
        Instant now = Instant.now();
        Instant result = utilService.resolveAssessmentStartTimeAsInstant(now);
        assertEquals(now, result);
    }

    @Test
    void testResolveAssessmentStartTimeAsInstant_String() {
        Instant now = Instant.now();
        Instant result = utilService.resolveAssessmentStartTimeAsInstant(now.toString());
        assertEquals(now, result);
    }

    @Test
    void testResolveAssessmentStartTimeAsInstant_UnknownType_ReturnsNull() {
        assertNull(utilService.resolveAssessmentStartTimeAsInstant(12345));
    }

    @Test
    void testCollectHierarchyQuestionIds_FlattensChildNodes() {
        Map<String, Object> section1 = new HashMap<>();
        section1.put(Constants.CHILD_NODES, List.of("q1", "q2"));
        Map<String, Object> section2 = new HashMap<>();
        section2.put(Constants.CHILD_NODES, List.of("q3"));
        Map<String, Object> questionSet = new HashMap<>();
        questionSet.put(Constants.CHILDREN, List.of(section1, section2));
        List<Object> result = utilService.collectHierarchyQuestionIds(questionSet);
        assertEquals(List.of("q1", "q2", "q3"), result);
    }

    @Test
    void testCollectSubmittedQuestionIds_FlattensSubmittedQuestions() {
        Map<String, Object> question1 = new HashMap<>();
        question1.put(Constants.IDENTIFIER, "q1");
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.CHILDREN, List.of(question1));
        List<Object> result = utilService.collectSubmittedQuestionIds(List.of(section), List.of(Constants.IDENTIFIER));
        assertEquals(List.of("q1"), result);
    }

    @Test
    void testValidateIfQuestionIdsAreSame_BlankStoredQuestionSet_ReturnsError() throws Exception {
        Map<String, Object> existingAssessmentData = new HashMap<>();
        String result = utilService.validateIfQuestionIdsAreSame(new ArrayList<>(), List.of(Constants.IDENTIFIER),
                existingAssessmentData);
        assertEquals(Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED, result);
    }

    @Test
    void testValidateIfQuestionIdsAreSame_MatchingQuestionIds_ReturnsEmpty() throws Exception {
        AssessmentUtilServiceV2Impl realUtil = new AssessmentUtilServiceV2Impl(serverProperties,
                outboundRequestHandlerService, new ObjectMapper(), cassandraOperation, redisCacheMgr, contentService, kafkaProducer);
        Map<String, Object> section = new HashMap<>();
        section.put(Constants.CHILD_NODES, List.of("q1", "q2"));
        Map<String, Object> questionSetFromAssessment = new HashMap<>();
        questionSetFromAssessment.put(Constants.CHILDREN, List.of(section));
        Map<String, Object> existingAssessmentData = new HashMap<>();
        existingAssessmentData.put(Constants.ASSESSMENT_READ_RESPONSE_KEY,
                new ObjectMapper().writeValueAsString(questionSetFromAssessment));

        Map<String, Object> submittedQuestion = new HashMap<>();
        submittedQuestion.put(Constants.IDENTIFIER, "q1");
        Map<String, Object> submittedSection = new HashMap<>();
        submittedSection.put(Constants.CHILDREN, List.of(submittedQuestion));

        String result = realUtil.validateIfQuestionIdsAreSame(List.of(submittedSection), List.of(Constants.IDENTIFIER),
                existingAssessmentData);
        assertEquals(Constants.EMPTY, result);
    }

    @Test
    void testFilterQuestionMapDetail_ShuffleFalse_OptionsNotShuffled() {
        List<Map<String, Object>> options = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            options.add(Map.of("index", i, "text", "Option " + i));
        }
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.PRIMARY_CATEGORY, "MCQ");
        questionMap.put(Constants.IDENTIFIER, "q1");
        questionMap.put(Constants.QUESTION_TYPE, "MCQ-SCA");
        questionMap.put(Constants.CHOICES, Map.of(Constants.OPTIONS, options));
        when(serverProperties.getAssessmentQuestionParams())
                .thenReturn(List.of(Constants.IDENTIFIER, Constants.PRIMARY_CATEGORY, Constants.QUESTION_TYPE));
        Map<String, Object> result = utilService.filterQuestionMapDetail(questionMap, "anyCategory", false);
        Map<String, Object> choices = (Map<String, Object>) result.get(Constants.CHOICES);
        List<Map<String, Object>> resultOptions = (List<Map<String, Object>>) choices.get(Constants.OPTIONS);
        assertEquals(options, resultOptions, "Options order should be preserved when shuffle is false");
    }

    @Test
    void testFilterQuestionMapDetail_ShuffleTrue_QTypeNotAllowed_OptionsNotShuffled() {
        List<Map<String, Object>> options = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            options.add(Map.of("index", i, "text", "Option " + i));
        }
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.PRIMARY_CATEGORY, "MCQ");
        questionMap.put(Constants.IDENTIFIER, "q1");
        questionMap.put(Constants.QUESTION_TYPE, "MCQ-MCA");
        questionMap.put(Constants.CHOICES, Map.of(Constants.OPTIONS, options));
        when(serverProperties.getAssessmentQuestionParams())
                .thenReturn(List.of(Constants.IDENTIFIER, Constants.PRIMARY_CATEGORY, Constants.QUESTION_TYPE));
        when(serverProperties.getShuffleAllowedQTypes()).thenReturn(List.of("MCQ-SCA"));
        Map<String, Object> result = utilService.filterQuestionMapDetail(questionMap, "anyCategory", true);
        Map<String, Object> choices = (Map<String, Object>) result.get(Constants.CHOICES);
        List<Map<String, Object>> resultOptions = (List<Map<String, Object>>) choices.get(Constants.OPTIONS);
        assertEquals(options, resultOptions, "Options order should be preserved when qType is not in allowed list");
    }

    @Test
    void testFilterQuestionMapDetail_ShuffleTrue_QTypeBlank_OptionsNotShuffled() {
        List<Map<String, Object>> options = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            options.add(Map.of("index", i, "text", "Option " + i));
        }
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.PRIMARY_CATEGORY, "MCQ");
        questionMap.put(Constants.IDENTIFIER, "q1");
        questionMap.put(Constants.QUESTION_TYPE, "");
        questionMap.put(Constants.CHOICES, Map.of(Constants.OPTIONS, options));
        when(serverProperties.getAssessmentQuestionParams())
                .thenReturn(List.of(Constants.IDENTIFIER, Constants.PRIMARY_CATEGORY, Constants.QUESTION_TYPE));
        when(serverProperties.getShuffleAllowedQTypes()).thenReturn(List.of("MCQ-SCA"));
        Map<String, Object> result = utilService.filterQuestionMapDetail(questionMap, "anyCategory", true);
        Map<String, Object> choices = (Map<String, Object>) result.get(Constants.CHOICES);
        List<Map<String, Object>> resultOptions = (List<Map<String, Object>>) choices.get(Constants.OPTIONS);
        assertEquals(options, resultOptions, "Options order should be preserved when qType is blank");
    }

    @Test
    void testFilterQuestionMapDetailV2_ShuffleFalse_OptionsNotShuffled() {
        List<Map<String, Object>> options = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            options.add(Map.of("index", i, "text", "Option " + i));
        }
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.PRIMARY_CATEGORY, "MCQ");
        questionMap.put(Constants.IDENTIFIER, "q1");
        questionMap.put(Constants.QUESTION_TYPE, "MCQ-SCA");
        questionMap.put(Constants.CHOICES, Map.of(Constants.OPTIONS, options));
        when(serverProperties.getAssessmentQuestionParams())
                .thenReturn(List.of(Constants.IDENTIFIER, Constants.PRIMARY_CATEGORY, Constants.QUESTION_TYPE));
        Map<String, Object> result = utilService.filterQuestionMapDetailV2(questionMap, "anyCategory", false);
        Map<String, Object> choices = (Map<String, Object>) result.get(Constants.CHOICES);
        List<Map<String, Object>> resultOptions = (List<Map<String, Object>>) choices.get(Constants.OPTIONS);
        assertEquals(options, resultOptions, "Options order should be preserved when shuffle is false");
    }

    @Test
    void testFilterQuestionMapDetailV2_ShuffleTrue_QTypeNotAllowed_OptionsNotShuffled() {
        List<Map<String, Object>> options = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            options.add(Map.of("index", i, "text", "Option " + i));
        }
        Map<String, Object> questionMap = new HashMap<>();
        questionMap.put(Constants.PRIMARY_CATEGORY, "MCQ");
        questionMap.put(Constants.IDENTIFIER, "q1");
        questionMap.put(Constants.QUESTION_TYPE, "MCQ-MCA");
        questionMap.put(Constants.CHOICES, Map.of(Constants.OPTIONS, options));
        when(serverProperties.getAssessmentQuestionParams())
                .thenReturn(List.of(Constants.IDENTIFIER, Constants.PRIMARY_CATEGORY, Constants.QUESTION_TYPE));
        when(serverProperties.getShuffleAllowedQTypes()).thenReturn(List.of("MCQ-SCA"));
        Map<String, Object> result = utilService.filterQuestionMapDetailV2(questionMap, "anyCategory", true);
        Map<String, Object> choices = (Map<String, Object>) result.get(Constants.CHOICES);
        List<Map<String, Object>> resultOptions = (List<Map<String, Object>>) choices.get(Constants.OPTIONS);
        assertEquals(options, resultOptions, "Options order should be preserved when qType is not in allowed list");
    }

    @Test
    void testPublishFailedAssessmentAuditEvent_Success() throws Exception {
        String userId = "user123";
        String assessmentId = "assess456";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put("key", "value");
        String errMessage = "Some error occurred";
        String methodName = "submitAssessmentAsync";
        String topicName = "dev.assessment.failed.audit.error";
        String expectedJson = "{\"userId\":\"user123\"}";
        Map<String, Object> mockResponse = new HashMap<>();
        mockResponse.put("error", "validation_failed");
        mockResponse.put("statusCode", 400);
        when(serverProperties.getAssessmentFailedAuditErrorTopic()).thenReturn(topicName);
        when(mapper.writeValueAsString(any(Map.class))).thenReturn(expectedJson);
        utilService.publishFailedAssessmentAuditEvent(userId, assessmentId, submitRequest, errMessage, methodName, mockResponse);
        verify(mapper).writeValueAsString(any(Map.class));
        verify(kafkaProducer).push(topicName, expectedJson);
        verify(serverProperties).getAssessmentFailedAuditErrorTopic();
    }

    @Test
    void testPublishFailedAssessmentAuditEvent_WithNullSubmitRequest() throws Exception {
        String userId = "user123";
        String assessmentId = "assess456";
        String errMessage = "Error";
        String methodName = "submitAssessmentAsync";
        String topicName = "dev.assessment.failed.audit.error";
        String expectedJson = "{\"userId\":\"user123\"}";
        when(serverProperties.getAssessmentFailedAuditErrorTopic()).thenReturn(topicName);
        when(mapper.writeValueAsString(any(Map.class))).thenReturn(expectedJson);
        utilService.publishFailedAssessmentAuditEvent(userId, assessmentId, null, errMessage, methodName, null);
        ArgumentCaptor<Map<String, Object>> eventCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mapper).writeValueAsString(eventCaptor.capture());
        Map<String, Object> capturedEvent = eventCaptor.getValue();
        assertFalse(capturedEvent.containsKey(Constants.SUBMIT_ASSESSMENT_REQUEST),
                "Event should not contain submitRequest when it is null");
        verify(kafkaProducer).push(topicName, expectedJson);
    }

    @Test
    void testPublishFailedAssessmentAuditEvent_WithEmptySubmitRequest() throws Exception {
        String userId = "user123";
        String assessmentId = "assess456";
        Map<String, Object> submitRequest = new HashMap<>();
        String errMessage = "Error";
        String methodName = "submitAssessmentAsync";
        String topicName = "dev.assessment.failed.audit.error";
        String expectedJson = "{}";
        Map<String, Object> mockResponse = new HashMap<>();
        mockResponse.put("message", "Empty request");
        when(serverProperties.getAssessmentFailedAuditErrorTopic()).thenReturn(topicName);
        when(mapper.writeValueAsString(any(Map.class))).thenReturn(expectedJson);
        utilService.publishFailedAssessmentAuditEvent(userId, assessmentId, submitRequest, errMessage, methodName, mockResponse);
        ArgumentCaptor<Map<String, Object>> eventCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mapper).writeValueAsString(eventCaptor.capture());
        Map<String, Object> capturedEvent = eventCaptor.getValue();
        assertFalse(capturedEvent.containsKey(Constants.SUBMIT_ASSESSMENT_REQUEST),
                "Event should not contain submitRequest when it is empty");
        verify(kafkaProducer).push(topicName, expectedJson);
    }

    @Test
    void testPublishFailedAssessmentAuditEvent_EventContainsAllRequiredFields() throws Exception {
        String userId = "user123";
        String assessmentId = "assess456";
        Map<String, Object> submitRequest = new HashMap<>();
        submitRequest.put("questionId", "q1");
        String errMessage = "Processing failed";
        String methodName = "submitAssessmentAsyncV6";
        String topicName = "dev.assessment.failed.audit.error";
        Map<String, Object> mockResponse = new HashMap<>();
        mockResponse.put("error", "processing_error");
        mockResponse.put("details", "Failed to process assessment");
        when(serverProperties.getAssessmentFailedAuditErrorTopic()).thenReturn(topicName);
        when(mapper.writeValueAsString(any(Map.class))).thenReturn("{}");
        utilService.publishFailedAssessmentAuditEvent(userId, assessmentId, submitRequest, errMessage, methodName, mockResponse);
        ArgumentCaptor<Map<String, Object>> eventCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mapper).writeValueAsString(eventCaptor.capture());
        Map<String, Object> capturedEvent = eventCaptor.getValue();
        assertEquals(userId, capturedEvent.get(Constants.USER_ID));
        assertEquals(assessmentId, capturedEvent.get(Constants.ASSESSMENT_ID_KEY));
        assertEquals(errMessage, capturedEvent.get(Constants.ERROR_MESSAGE));
        assertEquals(methodName, capturedEvent.get(Constants.METHOD_NAME));
        assertEquals(Constants.FAILED, capturedEvent.get(Constants.STATUS));
        assertNotNull(capturedEvent.get(Constants.START_TIME), "startTime should be set");
        assertEquals(submitRequest, capturedEvent.get(Constants.SUBMIT_ASSESSMENT_REQUEST));
    }

    @Test
    void testPublishFailedAssessmentAuditEvent_SerializationException_DoesNotThrow() throws Exception {
        String userId = "user123";
        String assessmentId = "assess456";
        Map<String, Object> submitRequest = Map.of("key", "value");
        String errMessage = "Error";
        String methodName = "submitAssessmentAsync";
        Map<String, Object> mockResponse = Map.of("error", "serialization_test");
        when(mapper.writeValueAsString(any(Map.class)))
                .thenThrow(new com.fasterxml.jackson.core.JsonProcessingException("Serialization error") {});
        assertDoesNotThrow(() ->
                utilService.publishFailedAssessmentAuditEvent(userId, assessmentId, submitRequest, errMessage, methodName, mockResponse));
        verify(kafkaProducer, never()).push(anyString(), anyString());
    }

    @Test
    void testPublishFailedAssessmentAuditEvent_KafkaPushException_DoesNotThrow() throws Exception {
        String userId = "user123";
        String assessmentId = "assess456";
        Map<String, Object> submitRequest = Map.of("key", "value");
        String errMessage = "Error";
        String methodName = "submitAssessmentAsync";
        String topicName = "dev.assessment.failed.audit.error";
        Map<String, Object> mockResponse = Map.of("error", "kafka_test");
        when(serverProperties.getAssessmentFailedAuditErrorTopic()).thenReturn(topicName);
        when(mapper.writeValueAsString(any(Map.class))).thenReturn("{}");
        doThrow(new RuntimeException("Kafka unavailable")).when(kafkaProducer).push(anyString(), any());
        assertDoesNotThrow(() ->
                utilService.publishFailedAssessmentAuditEvent(userId, assessmentId, submitRequest, errMessage, methodName, mockResponse));
    }

    @Test
    void testPublishFailedAssessmentAuditEvent_StartTimeIsValidInstantFormat() throws Exception {
        String userId = "user123";
        String assessmentId = "assess456";
        String errMessage = "Error";
        String methodName = "submitAssessmentAsync";
        String topicName = "dev.assessment.failed.audit.error";
        when(serverProperties.getAssessmentFailedAuditErrorTopic()).thenReturn(topicName);
        when(mapper.writeValueAsString(any(Map.class))).thenReturn("{}");
        utilService.publishFailedAssessmentAuditEvent(userId, assessmentId, null, errMessage, methodName, null);
        ArgumentCaptor<Map<String, Object>> eventCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mapper).writeValueAsString(eventCaptor.capture());
        Map<String, Object> capturedEvent = eventCaptor.getValue();
        String startTime = (String) capturedEvent.get(Constants.START_TIME);
        assertNotNull(startTime);
        assertDoesNotThrow(() -> Instant.parse(startTime),
                "startTime should be a valid ISO-8601 instant string");
    }
}
