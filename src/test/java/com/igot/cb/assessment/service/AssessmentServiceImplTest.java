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
    void testGetAssessmentByContentUser_success() throws Exception {
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
    void testGetAssessmentByContentUser_nullPointer() throws Exception {
        when(repository.getAssessmentbyContentUser(anyString(), anyString(), anyString())).thenThrow(NullPointerException.class);
        assertThrows(ApplicationLogicError.class, () -> assessmentService.getAssessmentByContentUser("rootOrg", "courseId", "userId"));
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