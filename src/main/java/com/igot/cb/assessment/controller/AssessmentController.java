package com.igot.cb.assessment.controller;

import com.igot.cb.assessment.dto.AssessmentSubmissionDTO;
import com.igot.cb.assessment.service.*;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.Constants;
import io.micrometer.common.util.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

@RestController
public class AssessmentController {
    AssessmentService assessmentService;

    AssessmentServiceV2 assessmentServiceV2;

    AssessmentServiceV4 assessmentServiceV4;

    AssessmentServiceV5 assessmentServiceV5;

    public AssessmentController(AssessmentService assessmentService, AssessmentServiceV2 assessmentServiceV2, AssessmentServiceV4 assessmentServiceV4, AssessmentServiceV5 assessmentServiceV5) {
        this.assessmentService = assessmentService;
        this.assessmentServiceV2 = assessmentServiceV2;
        this.assessmentServiceV4 = assessmentServiceV4;
        this.assessmentServiceV5 = assessmentServiceV5;
    }

    @PostMapping("/v2/user/{userId}/assessment/submit")
    public ResponseEntity<Map<String, Object>> submitAssessment(@Valid @RequestBody AssessmentSubmissionDTO requestBody,
                                                                @PathVariable("userId") String userId, @RequestHeader("rootOrg") String rootOrg) throws IOException, ParseException {

        return new ResponseEntity<>(assessmentService.submitAssessment(rootOrg, requestBody, userId),
                HttpStatus.CREATED);
    }

    @GetMapping("/v2/content/{courseId}/user/{userId}/assessment")
    public ResponseEntity<Map<String, Object>> getAssessmentByContentUser(@PathVariable String courseId,
                                                                          @PathVariable("userId") String userId, @RequestHeader("rootOrg") String rootOrg) {
        return new ResponseEntity<>(assessmentService.getAssessmentByContentUser(rootOrg, courseId, userId),
                HttpStatus.OK);
    }

    // =======================
    // KONG API Changes
    /**
     * validates, submits and inserts assessments and quizzes into the db
     *
     * @param requestBody
     * @param userId
     * @return
     * @throws IOException    if the submission cannot be read or written
     * @throws ParseException if a submitted timestamp is malformed
     */
    @PostMapping("/v2/user/assessment/submit")
    public ResponseEntity<Map<String, Object>> submitUserAssessment(
            @Valid @RequestBody AssessmentSubmissionDTO requestBody, @RequestHeader("userId") String userId,
            @RequestHeader("rootOrg") String rootOrg) throws IOException, ParseException {

        return new ResponseEntity<>(assessmentService.submitAssessment(rootOrg, requestBody, userId),
                HttpStatus.CREATED);
    }

    /**
     * Controller to a get request to Fetch AssessmentData the request requires
     * user_id and course_id returns a JSON of processed data and list of
     * Assessments Given
     *
     * @param courseId
     * @param userId
     * @param rootOrg
     * @return
     */
    @GetMapping("/v2/content/user/assessment")
    public ResponseEntity<Map<String, Object>> getUserAssessmentByContent(@RequestHeader("courseId") String courseId,
                                                                          @RequestHeader("userId") String userId, @RequestHeader("rootOrg") String rootOrg) {
        return new ResponseEntity<>(assessmentService.getAssessmentByContentUser(rootOrg, courseId, userId),
                HttpStatus.OK);
    }

    /**
     * To get the assessment question sets using the course and the assessment id
     *
     * @param courseId
     * @param assessmentContentId
     * @param rootOrg
     * @return
     * @throws Exception
     */
    @GetMapping("/v2/{courseId}/assessment/{assessmentContentId}")
    public ResponseEntity<Map<String, Object>> getAssessmentContent(@PathVariable("courseId") String courseId,
                                                                    @PathVariable("assessmentContentId") String assessmentContentId, @RequestHeader("rootOrg") String rootOrg) {
        return new ResponseEntity<>(assessmentService.getAssessmentContent(courseId, assessmentContentId),
                HttpStatus.OK);
    }

    // =======================
    // QUML based Assessment APIs
    @PostMapping("/v3/user/assessment/submit")
    public ResponseEntity<SBApiResponse>submitUserAssessmentV3(@Valid @RequestBody Map<String, Object> requestBody,
                                                    @RequestHeader("x-authenticated-user-token") String authUserToken , @RequestParam(name = "editMode" ,required = false) String editMode) throws IOException {

        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse submitResponse = assessmentServiceV2.submitAssessment(requestBody, authUserToken,edit);
        return new ResponseEntity<>(submitResponse, submitResponse.getResponseCode());
    }

    /**
     *
     * @param assessmentIdentifier
     * @param rootOrg
     * @return
     */

    @GetMapping("/v1/quml/assessment/read/{assessmentIdentifier}")
    public ResponseEntity<SBApiResponse> readAssessment(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        SBApiResponse readResponse = assessmentServiceV2.readAssessment(assessmentIdentifier, token);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }

    @PostMapping("/v1/quml/question/list")
    public ResponseEntity<SBApiResponse>readQuestionList(@Valid @RequestBody Map<String, Object> requestBody,
                                              @RequestHeader("x-authenticated-user-token") String authUserToken) {
        SBApiResponse response = assessmentServiceV2.readQuestionList(requestBody, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/quml/assessment/retake/{assessmentIdentifier}")
    public ResponseEntity<SBApiResponse> retakeAssessment(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        SBApiResponse readResponse = assessmentServiceV2.retakeAssessment(assessmentIdentifier, token);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }

    // =======================
    // V4 Enhancements
    // Async capability and not using Redis
    // =======================
    @PostMapping("/v4/user/assessment/submit")
    public ResponseEntity<SBApiResponse>submitUserAssessmentV4(@Valid @RequestBody Map<String, Object> requestBody,
                                                    @RequestHeader("x-authenticated-user-token") String authUserToken,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse submitResponse = assessmentServiceV4.submitAssessmentAsync(requestBody, authUserToken,edit);
        return new ResponseEntity<>(submitResponse, submitResponse.getResponseCode());
    }

    /**
     *
     * @param assessmentIdentifier
     * @param rootOrg
     * @return
     * @throws Exception
     */

    @GetMapping("/v4/quml/assessment/read/{assessmentIdentifier}")
    public ResponseEntity<SBApiResponse> readAssessmentV4(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestParam(name = "editMode" ,required = false) String editMode,
            @RequestParam (name = "parentContextId" ,required = false) String parentContextId) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse readResponse = assessmentServiceV4.readAssessment(assessmentIdentifier, token,edit, parentContextId);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }

    @PostMapping("/v4/quml/question/list")
    public ResponseEntity<SBApiResponse>readQuestionListV4(@Valid @RequestBody Map<String, Object> requestBody,
                                                @RequestHeader("x-authenticated-user-token") String authUserToken,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse response = assessmentServiceV4.readQuestionList(requestBody, authUserToken,edit);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v4/quml/assessment/retake/{assessmentIdentifier}")
    public ResponseEntity<SBApiResponse> retakeAssessmentV4(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse readResponse = assessmentServiceV4.retakeAssessment(assessmentIdentifier, token,edit);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }

    @PostMapping("/v4/quml/assessment/result")
    public ResponseEntity<SBApiResponse>readAssessmentResultV4(@Valid @RequestBody Map<String, Object> requestBody,
                                                    @RequestHeader("x-authenticated-user-token") String authUserToken) {
        SBApiResponse response = assessmentServiceV4.readAssessmentResultV4(requestBody, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/fetch/assessment/wheebox")
    public ResponseEntity<SBApiResponse>readWheebox(@RequestHeader("x-authenticated-user-token") String authUserToken) {
        SBApiResponse response = assessmentServiceV4.readWheebox(authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v5/quml/assessment/read/{assessmentIdentifier}")
    public ResponseEntity<SBApiResponse> readAssessmentV5(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestParam(name = "editMode" ,required = false) String editMode,
            @RequestParam (name = "parentContextId" ,required = false) String parentContextId) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse readResponse = assessmentServiceV5.readAssessment(assessmentIdentifier, token,edit, parentContextId);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }

    @PostMapping("/v5/quml/question/list")
    public ResponseEntity<SBApiResponse> readQuestionListV5(@Valid @RequestBody Map<String, Object> requestBody,
                                                            @RequestHeader("x-authenticated-user-token") String authUserToken,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse response = assessmentServiceV5.readQuestionList(requestBody, authUserToken,edit);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/v5/user/assessment/submit")
    public ResponseEntity<SBApiResponse> submitUserAssessmentV5(@Valid @RequestBody Map<String, Object> requestBody,
                                                                @RequestHeader("x-authenticated-user-token") String authUserToken,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse submitResponse = assessmentServiceV5.submitAssessmentAsync(requestBody, authUserToken,edit);
        return new ResponseEntity<>(submitResponse, submitResponse.getResponseCode());
    }

    @GetMapping("/v5/quml/assessment/retake/{assessmentIdentifier}")
    public ResponseEntity<SBApiResponse> retakeAssessmentV5(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,@RequestParam(name = "editMode" ,required = false) String editMode) {
        Boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse readResponse = assessmentServiceV5.retakeAssessment(assessmentIdentifier, token,edit);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }

    @PostMapping("/v5/quml/assessment/result")
    public ResponseEntity<SBApiResponse> readAssessmentResultV5(@Valid @RequestBody Map<String, Object> requestBody,
                                                                @RequestHeader("x-authenticated-user-token") String authUserToken) {
        SBApiResponse response = assessmentServiceV5.readAssessmentResultV5(requestBody, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/v5/user/assessment/save")
    public ResponseEntity<SBApiResponse> saveUserAssessmentV5(@Valid @RequestBody Map<String, Object> requestBody,
                                                              @RequestHeader("x-authenticated-user-token") String authUserToken,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse submitResponse = assessmentServiceV5.saveAssessmentAsync(requestBody, authUserToken,edit);
        return new ResponseEntity<>(submitResponse, submitResponse.getResponseCode());
    }

    @GetMapping("/v5/quml/assessment/savepoint/{assessmentIdentifier}")
    public ResponseEntity<SBApiResponse> readSavePointV5(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse readResponse = assessmentServiceV5.readAssessmentSavePoint(assessmentIdentifier, token,edit);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }

    @GetMapping("/autoPublish/{assessmentIdentifier}")
    public ResponseEntity<SBApiResponse> autoPublish(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        SBApiResponse response = assessmentServiceV5.autoPublish(assessmentIdentifier,token);
        return new ResponseEntity<>(response,response.getResponseCode());
    }

    @PostMapping("/v6/user/assessment/submit")
    public ResponseEntity<SBApiResponse> submitUserAssessmentV6(@Valid @RequestBody Map<String, Object> requestBody,
                                                                @RequestHeader("x-authenticated-user-token") String authUserToken,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse submitResponse = assessmentServiceV5.submitAssessmentAsyncV6(requestBody, authUserToken,edit);
        return new ResponseEntity<>(submitResponse, submitResponse.getResponseCode());
    }

    @GetMapping("/chatbot/assessment/retake/count")
    public ResponseEntity<SBApiResponse> retakeAssessmentCount(
            @RequestParam(name = "assessmentIdentifier") String assessmentIdentifier,
            @RequestParam(name = "userId") String userId,
            @RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse readResponse = assessmentServiceV4.retakeAssessmentByUserId(assessmentIdentifier, userId, edit, null);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }
}
