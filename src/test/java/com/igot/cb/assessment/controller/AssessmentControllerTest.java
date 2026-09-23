package com.igot.cb.assessment.controller;

import com.igot.cb.assessment.dto.AssessmentSubmissionDTO;
import com.igot.cb.assessment.service.*;
import com.igot.cb.common.model.SBApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AssessmentControllerTest {

    @Mock AssessmentService assessmentService;
    @Mock AssessmentServiceV2 assessmentServiceV2;
    @Mock AssessmentServiceV4 assessmentServiceV4;
    @Mock AssessmentServiceV5 assessmentServiceV5;

    @InjectMocks AssessmentController assessmentController;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void submitAssessment() throws Exception {
        Map<String, Object> result = Collections.singletonMap("result", "ok");
        when(assessmentService.submitAssessment(anyString(), any(), anyString())).thenReturn(result);

        AssessmentSubmissionDTO dto = new AssessmentSubmissionDTO();
        ResponseEntity<Map<String, Object>> response = assessmentController.submitAssessment(dto, "user", "rootOrg");
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(result, response.getBody());
    }

    @Test
    void getAssessmentByContentUser() {
        Map<String, Object> result = Collections.singletonMap("assessment", "data");
        when(assessmentService.getAssessmentByContentUser(anyString(), anyString(), anyString())).thenReturn(result);

        ResponseEntity<Map<String, Object>> response = assessmentController.getAssessmentByContentUser("course", "user", "rootOrg");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(result, response.getBody());
    }

    @Test
    void submitUserAssessment() throws Exception {
        Map<String, Object> result = Collections.singletonMap("submit", "ok");
        when(assessmentService.submitAssessment(anyString(), any(), anyString())).thenReturn(result);

        AssessmentSubmissionDTO dto = new AssessmentSubmissionDTO();
        ResponseEntity<Map<String, Object>> response = assessmentController.submitUserAssessment(dto, "user", "rootOrg");
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(result, response.getBody());
    }

    @Test
    void getUserAssessmentByContent() {
        Map<String, Object> result = Collections.singletonMap("assessment", "data");
        when(assessmentService.getAssessmentByContentUser(anyString(), anyString(), anyString())).thenReturn(result);

        ResponseEntity<Map<String, Object>> response = assessmentController.getUserAssessmentByContent("course", "user", "rootOrg");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(result, response.getBody());
    }

    @Test
    void getAssessmentContent() {
        Map<String, Object> result = Collections.singletonMap("content", "questions");
        when(assessmentService.getAssessmentContent(anyString(), anyString())).thenReturn(result);

        ResponseEntity<Map<String, Object>> response = assessmentController.getAssessmentContent("course", "assessmentId", "rootOrg");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(result, response.getBody());
    }

    @Test
    void submitUserAssessmentV3() throws Exception {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.CREATED);
        when(assessmentServiceV2.submitAssessment(anyMap(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<?> response = assessmentController.submitUserAssessmentV3(Collections.emptyMap(), "token", "true");
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readAssessment() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV2.readAssessment(anyString(), anyString())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.readAssessment("id", "token");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readQuestionList() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV2.readQuestionList(anyMap(), anyString())).thenReturn(apiResponse);

        ResponseEntity<?> response = assessmentController.readQuestionList(Collections.emptyMap(), "token");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void retakeAssessment() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV2.retakeAssessment(anyString(), anyString())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.retakeAssessment("id", "token");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void submitUserAssessmentV4() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.CREATED);
        when(assessmentServiceV4.submitAssessmentAsync(anyMap(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<?> response = assessmentController.submitUserAssessmentV4(Collections.emptyMap(), "token", "true");
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readAssessmentV4() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV4.readAssessment(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.readAssessmentV4("id", "token", "true", "parentId");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readQuestionListV4() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV4.readQuestionList(anyMap(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<?> response = assessmentController.readQuestionListV4(Collections.emptyMap(), "token", "true");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void retakeAssessmentV4() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV4.retakeAssessment(anyString(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.retakeAssessmentV4("id", "token", "true");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void retakeAssessmentCount() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV4.retakeAssessmentByUserId(anyString(), anyString(), anyBoolean(), any())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.retakeAssessmentCount("assessId", "userId", "false");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readAssessmentResultV4() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV4.readAssessmentResultV4(anyMap(), anyString())).thenReturn(apiResponse);

        ResponseEntity<?> response = assessmentController.readAssessmentResultV4(Collections.emptyMap(), "token");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readWheebox() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV4.readWheebox(anyString())).thenReturn(apiResponse);

        ResponseEntity<?> response = assessmentController.readWheebox("token");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readAssessmentV5() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV5.readAssessment(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.readAssessmentV5("id", "token", "true", "parentId");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readQuestionListV5() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV5.readQuestionList(anyMap(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.readQuestionListV5(Collections.emptyMap(), "token", "true");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void submitUserAssessmentV5() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.CREATED);
        when(assessmentServiceV5.submitAssessmentAsync(anyMap(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.submitUserAssessmentV5(Collections.emptyMap(), "token", "true");
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void retakeAssessmentV5() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV5.retakeAssessment(anyString(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.retakeAssessmentV5("id", "token", "true");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readAssessmentResultV5() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV5.readAssessmentResultV5(anyMap(), anyString())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.readAssessmentResultV5(Collections.emptyMap(), "token");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void saveUserAssessmentV5() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.CREATED);
        when(assessmentServiceV5.saveAssessmentAsync(anyMap(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.saveUserAssessmentV5(Collections.emptyMap(), "token", "true");
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void readSavePointV5() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV5.readAssessmentSavePoint(anyString(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.readSavePointV5("id", "token", "true");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void autoPublish() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.OK);
        when(assessmentServiceV5.autoPublish(anyString(), anyString())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.autoPublish("id", "token");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }

    @Test
    void submitUserAssessmentV6() {
        SBApiResponse apiResponse = new SBApiResponse();
        apiResponse.setResponseCode(HttpStatus.CREATED);
        when(assessmentServiceV5.submitAssessmentAsyncV6(anyMap(), anyString(), anyBoolean())).thenReturn(apiResponse);

        ResponseEntity<SBApiResponse> response = assessmentController.submitUserAssessmentV6(Collections.emptyMap(), "token", "true");
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(apiResponse, response.getBody());
    }
}
