package com.igot.cb.assessment.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.assessment.dto.AssessmentSubmissionDTO;
import com.igot.cb.assessment.model.QuestionSet;
import com.igot.cb.assessment.repo.AssessmentRepository;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.common.model.SunbirdApiHierarchyResultContent;
import com.igot.cb.common.model.SunbirdApiResp;
import com.igot.cb.common.model.SunbirdApiRespResult;
import com.igot.cb.common.service.ContentService;
import com.igot.cb.common.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import com.igot.cb.common.util.UserUtilityService;
import com.igot.cb.core.exception.ApplicationLogicError;
import org.apache.coyote.BadRequestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.mockito.*;


import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AssessmentServiceImplTest {

    @InjectMocks
    AssessmentServiceImpl assessmentService;

    @Mock
    AssessmentRepository repository;
    @Mock
    ContentService contentService;
    @Mock
    UserUtilityService userUtilService;
    @Mock
    AssessmentUtilService assessUtilServ;
    @Mock
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    @Mock
    CbExtAssessmentServerProperties extServerProperties;
    @Mock
    RedisCacheMgr redisCacheMgr;

    @Mock
    ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        ReflectionTestUtils.setField(assessmentService, "mapper", mapper);
    }

    @Test
    void testSubmitAssessment_validUser() throws Exception {
        AssessmentSubmissionDTO dto = new AssessmentSubmissionDTO();
        dto.setIdentifier("assessId");
        dto.setTitle("title");
        dto.setIsAssessment(true);
        dto.setQuestions(Collections.emptyList());

        when(userUtilService.validateUser(anyString(), anyString())).thenReturn(true);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("result", 80.0);
        resultMap.put("correct", 8);
        resultMap.put("blank", 2);
        resultMap.put("incorrect", 0);
        when(assessUtilServ.validateAssessment(anyList())).thenReturn(resultMap);
        when(contentService.getParentIdentifier(anyString())).thenReturn("parentId");
        when(contentService.getContentType(anyString())).thenReturn("course");
        when(repository.insertQuizOrAssessment(anyMap(), anyBoolean())).thenReturn(Collections.singletonMap("response", "SUCCESS"));

        Map<String, Object> result = assessmentService.submitAssessment("rootOrg", dto, "userId");
        assertEquals(80.0, result.get("result"));
        assertEquals(8, result.get("correct"));
        assertEquals(2, result.get("blank"));
        assertEquals(0, result.get("inCorrect"));
        assertEquals(10, result.get("total"));
        assertEquals(60, result.get("passPercent"));
    }

    @Test
    void testSubmitAssessment_invalidUser() {
        AssessmentSubmissionDTO dto = new AssessmentSubmissionDTO();
        when(userUtilService.validateUser(anyString(), anyString())).thenReturn(false);
        assertThrows(BadRequestException.class, () -> assessmentService.submitAssessment("rootOrg", dto, "userId"));
    }

    @Test
    void testGetAssessmentByContentUser_success() {
        Map<String, Object> assessmentRow = new HashMap<>();
        assessmentRow.put("result_percent", "75.0");
        assessmentRow.put("correct_count", 6);
        assessmentRow.put("incorrect_count", 2);
        assessmentRow.put("not_answered_count", 2);
        assessmentRow.put("ts_created", "2024-06-01T10:00:00Z");
        List<Map<String, Object>> repoResult = Arrays.asList(assessmentRow);

        when(repository.getAssessmentbyContentUser(anyString(), anyString(), anyString())).thenReturn(repoResult);

        Map<String, Object> result = assessmentService.getAssessmentByContentUser("rootOrg", "courseId", "userId");
        assertTrue(result.containsKey("pastAssessments"));
        List<?> assessments = (List<?>) result.get("pastAssessments");
        assertEquals(1, assessments.size());
        assertEquals(new BigDecimal("75.00"), ((Map<?, ?>) assessments.get(0)).get("result"));
    }

    @Test
    void testGetAssessmentByContentUser_nullPointer() {
        when(repository.getAssessmentbyContentUser(anyString(), anyString(), anyString())).thenThrow(NullPointerException.class);
        assertThrows(ApplicationLogicError.class, () -> assessmentService.getAssessmentByContentUser("rootOrg", "courseId", "userId"));
    }

    private Map<String, Object> buildResultMap() {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("result", 40.0);
        resultMap.put("correct", 4);
        resultMap.put("blank", 1);
        resultMap.put("incorrect", 5);
        return resultMap;
    }

    @Test
    void testSubmitAssessment_quiz_parentContentTypeEmpty() throws Exception {
        AssessmentSubmissionDTO dto = new AssessmentSubmissionDTO();
        dto.setIdentifier("quizId");
        dto.setTitle("quiz");
        dto.setIsAssessment(false);
        dto.setQuestions(Collections.emptyList());

        when(userUtilService.validateUser(anyString(), anyString())).thenReturn(true);
        when(assessUtilServ.validateAssessment(anyList())).thenReturn(buildResultMap());
        when(contentService.getParentIdentifier(anyString())).thenReturn("parentId");

        Map<String, Object> result = assessmentService.submitAssessment("rootOrg", dto, "userId");
        assertEquals(10, result.get("total"));

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(repository).insertQuizOrAssessment(captor.capture(), eq(false));
        assertEquals("", captor.getValue().get(Constants.PARENT_CONTENT_TYPE));
        verify(contentService, never()).getContentType(anyString());
    }

    @Test
    void testSubmitAssessment_assessment_emptyParentId() throws Exception {
        AssessmentSubmissionDTO dto = new AssessmentSubmissionDTO();
        dto.setIdentifier("assessId");
        dto.setTitle("title");
        dto.setIsAssessment(true);
        dto.setQuestions(Collections.emptyList());

        when(userUtilService.validateUser(anyString(), anyString())).thenReturn(true);
        when(assessUtilServ.validateAssessment(anyList())).thenReturn(buildResultMap());
        when(contentService.getParentIdentifier(anyString())).thenReturn("");

        Map<String, Object> result = assessmentService.submitAssessment("rootOrg", dto, "userId");
        assertEquals(40.0, result.get("result"));

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(repository).insertQuizOrAssessment(captor.capture(), eq(true));
        assertEquals("", captor.getValue().get(Constants.PARENT_CONTENT_TYPE));
        verify(contentService, never()).getContentType(anyString());
    }

    private Map<String, Object> buildRow(String percent, String ts) {
        Map<String, Object> row = new HashMap<>();
        row.put("result_percent", percent);
        row.put("correct_count", 1);
        row.put("incorrect_count", 1);
        row.put("not_answered_count", 0);
        row.put("ts_created", ts);
        return row;
    }

    @Test
    void testGetAssessmentByContentUser_multipleAttempts() {
        // rows are newest first; the loop walks from oldest (last) to newest (first)
        List<Map<String, Object>> repoResult = Arrays.asList(
                buildRow("70.0", "t3"), buildRow("80.0", "t2"), buildRow("50.0", "t1"));
        when(repository.getAssessmentbyContentUser(anyString(), anyString(), anyString())).thenReturn(repoResult);

        Map<String, Object> result = assessmentService.getAssessmentByContentUser("rootOrg", "courseId", "userId");
        assertEquals("t2", result.get("firstPassOn"));
        assertEquals(2, result.get("attemptsToPass"));
        assertEquals(new BigDecimal("80.00"), result.get("maxScore"));
        assertEquals("t2", result.get("maxScoreAttainedOn"));
        assertEquals(2, result.get("attemptsForMaxScore"));
        assertEquals(3, ((List<?>) result.get("pastAssessments")).size());
    }

    @Test
    void testGetAssessmentByContentUser_neverPassed() {
        List<Map<String, Object>> repoResult = Arrays.asList(buildRow("30.0", "t2"), buildRow("40.0", "t1"));
        when(repository.getAssessmentbyContentUser(anyString(), anyString(), anyString())).thenReturn(repoResult);

        Map<String, Object> result = assessmentService.getAssessmentByContentUser("rootOrg", "courseId", "userId");
        assertFalse(result.containsKey("firstPassOn"));
        assertFalse(result.containsKey("attemptsToPass"));
        assertEquals(new BigDecimal("40.00"), result.get("maxScore"));
        assertEquals("t1", result.get("maxScoreAttainedOn"));
        assertEquals(1, result.get("attemptsForMaxScore"));
    }

    @Test
    void testGetAssessmentByContentUser_noAttempts() {
        when(repository.getAssessmentbyContentUser(anyString(), anyString(), anyString()))
                .thenReturn(Collections.emptyList());

        Map<String, Object> result = assessmentService.getAssessmentByContentUser("rootOrg", "courseId", "userId");
        assertEquals(1, result.size());
        assertTrue(((List<?>) result.get("pastAssessments")).isEmpty());
    }

    @Test
    void testGetAssessmentContent_cacheMiss_noMatchingChild() {
        when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        when(extServerProperties.getKmBaseHost()).thenReturn("http://host/");
        when(extServerProperties.getContentHierarchyDetailEndPoint()).thenReturn("endpoint/{courseId}/{hierarchyType}");
        SunbirdApiResp apiResp = new SunbirdApiResp();
        apiResp.setResponseCode("OK");
        apiResp.setResult(new SunbirdApiRespResult());
        SunbirdApiHierarchyResultContent otherChild = new SunbirdApiHierarchyResultContent();
        otherChild.setIdentifier("otherId");
        otherChild.setArtifactUrl("http://other.json");
        SunbirdApiHierarchyResultContent nonJsonChild = new SunbirdApiHierarchyResultContent();
        nonJsonChild.setIdentifier("assessmentContentId");
        nonJsonChild.setArtifactUrl("http://artifact.pdf");
        apiResp.getResult().setContent(new SunbirdApiHierarchyResultContent());
        apiResp.getResult().getContent().setChildren(Arrays.asList(otherChild, nonJsonChild));

        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap())).thenReturn(new HashMap<>());
        when(mapper.convertValue(any(), eq(SunbirdApiResp.class))).thenReturn(apiResp);

        Map<String, Object> result = assessmentService.getAssessmentContent("courseId", "assessmentContentId");
        assertTrue(result.isEmpty());
        verify(assessUtilServ, never()).removeAssessmentAnsKey(any());
        verify(redisCacheMgr, never()).putCache(anyString(), any());
    }

    @Test
    void testGetAssessmentContent_exception() {
        when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        when(extServerProperties.getKmBaseHost()).thenReturn("http://host/");
        when(extServerProperties.getContentHierarchyDetailEndPoint()).thenReturn("endpoint/{courseId}/{hierarchyType}");
        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap()))
                .thenThrow(new RuntimeException("boom"));

        Map<String, Object> result = assessmentService.getAssessmentContent("courseId", "assessmentContentId");
        assertEquals(Constants.FAILED, result.get(Constants.STATUS));
        assertFalse(result.containsKey(Constants.QUESTION_SET));
    }

    @Test
    void testSubmitAssessmentByIframe_returnsEmptyMap() {
        Map<String, Object> result = assessmentService.submitAssessmentByIframe("rootOrg", new HashMap<>());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAssessmentContent_cacheHit() {
        String cachedQnsSet = "cachedQuestionSet";
        when(redisCacheMgr.getCache(anyString())).thenReturn((String) cachedQnsSet);

        Map<String, Object> result = assessmentService.getAssessmentContent("courseId", "assessmentContentId");
        assertEquals(Constants.SUCCESSFUL, result.get(Constants.STATUS));
        assertEquals(cachedQnsSet, result.get(Constants.QUESTION_SET));
    }

    @Test
    void testGetAssessmentContent_cacheMiss_successfulResponse() {
        when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        when(extServerProperties.getKmBaseHost()).thenReturn("http://host/");
        when(extServerProperties.getContentHierarchyDetailEndPoint()).thenReturn("endpoint/{courseId}/{hierarchyType}");
        SunbirdApiResp apiResp = new SunbirdApiResp();
        apiResp.setResponseCode("Ok");
        apiResp.setResult(new SunbirdApiRespResult());
        SunbirdApiHierarchyResultContent child = new SunbirdApiHierarchyResultContent();
        child.setIdentifier("assessmentContentId");
        child.setArtifactUrl("http://artifact.json");
        List<SunbirdApiHierarchyResultContent> children = Arrays.asList(child);
        apiResp.getResult().setContent(new SunbirdApiHierarchyResultContent());
        apiResp.getResult().getContent().setChildren(children);

        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap())).thenReturn(new HashMap<>());
        when(mapper.convertValue(any(), eq(SunbirdApiResp.class))).thenReturn(apiResp);
        QuestionSet questionSet = new QuestionSet();
        when(mapper.convertValue(any(), eq(QuestionSet.class))).thenReturn(questionSet);
        when(assessUtilServ.removeAssessmentAnsKey(any(QuestionSet.class))).thenReturn(questionSet);

        Map<String, Object> result = assessmentService.getAssessmentContent("courseId", "assessmentContentId");
        assertEquals(Constants.SUCCESSFUL, result.get(Constants.STATUS));
        assertEquals(questionSet, result.get(Constants.QUESTION_SET));
    }

    @Test
    void testGetAssessmentContent_cacheMiss_failedResponse() {
        when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        when(extServerProperties.getKmBaseHost()).thenReturn("http://host/");
        when(extServerProperties.getContentHierarchyDetailEndPoint()).thenReturn("endpoint/{courseId}/{hierarchyType}");
        SunbirdApiResp apiResp = new SunbirdApiResp();
        apiResp.setResponseCode("Failed");
        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap())).thenReturn(new HashMap<>());
        when(mapper.convertValue(any(), eq(SunbirdApiResp.class))).thenReturn(apiResp);

        Map<String, Object> result = assessmentService.getAssessmentContent("courseId", "assessmentContentId");
        assertEquals(Constants.FAILED, result.get(Constants.STATUS));
    }
}