package com.igot.cb.assessment.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.igot.cb.assessment.repo.AssessmentRepository;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.service.ContentService;
import com.igot.cb.common.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.common.util.AccessTokenValidator;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import com.igot.cb.core.producer.Producer;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.igot.cb.common.util.Constants.RESPONSE;
import static com.igot.cb.common.util.ProjectUtil.createDefaultResponse;
import static com.igot.cb.common.util.ProjectUtil.updateErrorDetails;
import com.igot.cb.core.exception.ApplicationLogicError;

@Service
@SuppressWarnings("unchecked")
public class AssessmentServiceV4Impl implements AssessmentServiceV4 {

    private final Logger logger = LoggerFactory.getLogger(AssessmentServiceV4Impl.class);
    CbExtAssessmentServerProperties serverProperties;

    Producer kafkaProducer;

    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    AssessmentUtilServiceV2 assessUtilServ;

    ObjectMapper mapper;

    AssessmentRepository assessmentRepository;

    AccessTokenValidator accessTokenValidator;

    ContentService contentService;

    public AssessmentServiceV4Impl(CbExtAssessmentServerProperties serverProperties, Producer kafkaProducer, OutboundRequestHandlerServiceImpl outboundRequestHandlerService, AssessmentUtilServiceV2 assessUtilServ, ObjectMapper mapper, AssessmentRepository assessmentRepository, AccessTokenValidator accessTokenValidator, ContentService contentService) {
        this.serverProperties = serverProperties;
        this.kafkaProducer = kafkaProducer;
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.assessUtilServ = assessUtilServ;
        this.mapper = mapper;
        this.assessmentRepository = assessmentRepository;
        this.accessTokenValidator = accessTokenValidator;
        this.contentService = contentService;
    }

    @Override
    public SBApiResponse retakeAssessment(String assessmentIdentifier, String token, Boolean editMode) {
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
        return retakeAssessmentByUserId(assessmentIdentifier, userId, editMode, token);
    }

    @Override
    public SBApiResponse retakeAssessmentByUserId(String assessmentIdentifier, String userId, Boolean editMode, String token) {
        logger.info("AssessmentServiceV4Impl::retakeAssessmentByUserId... Started");
        SBApiResponse response = createDefaultResponse(Constants.API_RETAKE_ASSESSMENT_GET);
        String errMsg = "";
        int retakeAttemptsAllowed = 0;
        int retakeAttemptsConsumed = 0;
        try {
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            Map<String, Object> assessmentAllDetail = assessUtilServ
                    .readAssessmentHierarchyFromCache(assessmentIdentifier,editMode,token);
            if (MapUtils.isEmpty(assessmentAllDetail)) {
                updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED,
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            RetakeTally tally = resolveRetakeTally(assessmentAllDetail, userId, assessmentIdentifier, response);
            if (tally.respondNow()) {
                return response;
            }
            retakeAttemptsAllowed = tally.allowed();
            retakeAttemptsConsumed = tally.consumed();
        } catch (Exception e) {
            errMsg = String.format("Error while calculating retake assessment. Exception: %s", e.getMessage());
            logger.error(errMsg, e);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        } else {
            response.getResult().put(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED, retakeAttemptsAllowed);
            response.getResult().put(Constants.RETAKE_ATTEMPTS_CONSUMED, retakeAttemptsConsumed);
        }
        logger.info("AssessmentServiceV4Impl::retakeAssessmentByUserId... Completed");
        return response;
    }

    /** Retake counts for an assessment, with {@code respondNow} set when {@code response} is already final. */
    private record RetakeTally(int allowed, int consumed, boolean respondNow) {
    }

    /**
     * Resolves how many retakes are allowed and already consumed. Pre-enrolled assessments get a
     * single attempt; otherwise the configured maximum applies and, when retake verification is
     * enabled, the consumed count is computed against the user's history.
     */
    private RetakeTally resolveRetakeTally(Map<String, Object> assessmentAllDetail, String userId,
                                           String assessmentIdentifier, SBApiResponse response) {
        Object contextCategory = assessmentAllDetail.get(Constants.CONTEXT_CATEGORY_TAG);
        if (contextCategory != null && Constants.PRE_ENROLLED_ASSESSMENT_KEY.equals(contextCategory.toString())) {
            return new RetakeTally(1, 0, false);
        }
        int retakeAttemptsAllowed = 0;
        if (assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS) != null) {
            retakeAttemptsAllowed = (int) assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS);
            if (retakeAttemptsAllowed == 0) {
                response.getResult().put(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED, retakeAttemptsAllowed);
                response.getResult().put(Constants.RETAKE_ATTEMPTS_CONSUMED, -1);
                return new RetakeTally(retakeAttemptsAllowed, -1, true);
            }
        }
        int retakeAttemptsConsumed = 0;
        if (serverProperties.isAssessmentRetakeCountVerificationEnabled()) {
            List<Map<String, Object>> userAssessmentDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                    userId, assessmentIdentifier);
            retakeAttemptsConsumed = assessUtilServ.calculateRetakeAttemptsConsumed(
                    userId, assessmentIdentifier, assessmentAllDetail, retakeAttemptsAllowed,
                    userAssessmentDataList, response,
                    new AssessmentUtilServiceV2.RetakeValidationOptions(HttpStatus.BAD_REQUEST, false));
            if (response.getResponseCode() == HttpStatus.BAD_REQUEST) {
                return new RetakeTally(retakeAttemptsAllowed, retakeAttemptsConsumed, true);
            }
        }
        return new RetakeTally(retakeAttemptsAllowed, retakeAttemptsConsumed, false);
    }

    @Override
    public SBApiResponse readAssessment(String assessmentIdentifier, String token,boolean editMode, String parentContextId) {
        logger.info("AssessmentServiceV4Impl::readAssessment... Started");
        SBApiResponse response = createDefaultResponse(Constants.API_READ_ASSESSMENT);
        String errMsg = "";
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            logger.info("ReadAssessment... UserId: {}, AssessmentIdentifier: {}", userId, assessmentIdentifier);

            Map<String, Object> assessmentAllDetail = fetchAssessmentHierarchy(assessmentIdentifier, token, editMode);

            if (MapUtils.isEmpty(assessmentAllDetail)) {
                updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED,
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            if (Constants.PRACTICE_QUESTION_SET
                    .equalsIgnoreCase((String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY))||editMode) {
                response.getResult().put(Constants.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail));
                return response;
            }

            List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                    userId, assessmentIdentifier);
            Instant assessmentStartTime = Instant.now();
            AttemptOutcome outcome = existingDataList.isEmpty()
                    ? startFirstAttempt(response, assessmentAllDetail, assessmentIdentifier, parentContextId, userId,
                            assessmentStartTime)
                    : resumeOrRestartAttempt(response, assessmentAllDetail, assessmentIdentifier,
                            parentContextId, userId, assessmentStartTime, existingDataList);
            if (outcome.contextLockFailed()) {
                return response;
            }
            errMsg = outcome.errMsg();
        } catch (Exception e) {
            errMsg = String.format("Error while reading assessment. Exception: %s", e.getMessage());
            logger.error(errMsg, e);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Result of continuing an existing attempt. {@code contextLockFailed} means the caller must
     * return the response untouched, as context-lock validation has already populated it.
     */
    private record AttemptOutcome(String errMsg, boolean contextLockFailed) {
    }

    /**
     * Handles the very first read of an assessment for a user: rejects a question set with no
     * expected duration, applies context locking, then opens a fresh attempt.
     */
    private AttemptOutcome startFirstAttempt(SBApiResponse response, Map<String, Object> assessmentAllDetail,
                                             String assessmentIdentifier, String parentContextId, String userId,
                                             Instant assessmentStartTime) {
        logger.info("Assessment read first time for user.");
        // Add Null check for expectedDuration.throw bad questionSet Assessment Exam
        if (null == assessmentAllDetail.get(Constants.EXPECTED_DURATION)) {
            return new AttemptOutcome(Constants.ASSESSMENT_INVALID, false);
        }
        String errMsg = assessUtilServ.validateContextLocking(assessmentAllDetail, parentContextId, response, userId,
                assessmentIdentifier);
        if (StringUtils.isNotBlank(errMsg)) {
            return new AttemptOutcome(errMsg, true);
        }
        return new AttemptOutcome(startNewAttempt(response, assessmentAllDetail, assessmentIdentifier, userId,
                assessmentStartTime), false);
    }

    /**
     * The user already has a record: hand back the in-progress attempt while its window is open,
     * otherwise start a fresh one.
     */
    private AttemptOutcome resumeOrRestartAttempt(SBApiResponse response, Map<String, Object> assessmentAllDetail,
                                                  String assessmentIdentifier, String parentContextId, String userId,
                                                  Instant assessmentStartTime,
                                                  List<Map<String, Object>> existingDataList) {
        logger.info("Assessment read... user has details... ");
        Map<String, Object> existingData = existingDataList.get(0);
        Object endTimeObj = existingData.get(Constants.END_TIME);
        Date existingAssessmentEndTime = endTimeObj instanceof Instant instant
                ? Date.from(instant)
                : (Date) endTimeObj;
        boolean withinAssessmentWindow = assessmentStartTime.compareTo(existingAssessmentEndTime.toInstant()) < 0;
        String status = (String) existingData.get(Constants.STATUS);
        if (withinAssessmentWindow && Constants.NOT_SUBMITTED.equalsIgnoreCase(status)) {
            resumeAttempt(response, existingData, assessmentStartTime, existingAssessmentEndTime);
            return new AttemptOutcome("", false);
        }
        if ((withinAssessmentWindow && status.equalsIgnoreCase(Constants.SUBMITTED))
                || assessmentStartTime.compareTo(existingAssessmentEndTime.toInstant()) > 0) {
            logger.info(
                    "Incase the assessment is submitted before the end time, or the endtime has exceeded, read assessment freshly ");
            String lockErrMsg = assessUtilServ.validateContextLocking(assessmentAllDetail, parentContextId, response,
                    userId, assessmentIdentifier);
            if (StringUtils.isNotBlank(lockErrMsg)) {
                return new AttemptOutcome(lockErrMsg, true);
            }
            return new AttemptOutcome(restartAttempt(response, assessmentAllDetail, assessmentIdentifier, userId,
                    assessmentStartTime), false);
        }
        return new AttemptOutcome("", false);
    }

    /** In edit mode the hierarchy is always read live; otherwise the cached copy is used. */
    private Map<String, Object> fetchAssessmentHierarchy(String assessmentIdentifier, String token, boolean editMode) {
        if (editMode) {
            return assessUtilServ.fetchHierarchyFromAssessServc(assessmentIdentifier, token);
        }
        return assessUtilServ.readAssessmentHierarchyFromCache(assessmentIdentifier, editMode, token);
    }

    /**
     * First attempt for this user — build the question set, open the start/end window and persist it.
     * Returns the error message, or an empty string on success.
     */
    private String startNewAttempt(SBApiResponse response, Map<String, Object> assessmentAllDetail,
                                   String assessmentIdentifier, String userId, Instant assessmentStartTime) {
        int expectedDuration = (Integer) assessmentAllDetail.get(Constants.EXPECTED_DURATION);
        Instant assessmentEndTime = assessUtilServ.calculateAssessmentSubmitTime(expectedDuration, assessmentStartTime, 0);
        Map<String, Object> assessmentData = readAssessmentLevelData(assessmentAllDetail);
        assessmentData.put(Constants.START_TIME, assessmentStartTime);
        assessmentData.put(Constants.END_TIME, assessmentEndTime);
        response.getResult().put(Constants.QUESTION_SET, assessmentData);
        boolean isAssessmentUpdatedToDB = assessmentRepository.addUserAssesmentDataToDB(userId, assessmentIdentifier,
                assessmentStartTime, assessmentEndTime,
                (Map<String, Object>) (response.getResult().get(Constants.QUESTION_SET)), Constants.NOT_SUBMITTED);
        return isAssessmentUpdatedToDB ? "" : Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED;
    }

    /** Attempt still open — hand back the question set stored against the user's record. */
    private void resumeAttempt(SBApiResponse response, Map<String, Object> existingData, Instant assessmentStartTime,
                               Date existingAssessmentEndTime) {
        String questionSetFromAssessmentString = (String) existingData.get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
        Map<String, Object> questionSetFromAssessment = new Gson().fromJson(questionSetFromAssessmentString,
                new TypeToken<HashMap<String, Object>>() {
                }.getType());
        questionSetFromAssessment.put(Constants.START_TIME, assessmentStartTime.toEpochMilli());
        questionSetFromAssessment.put(Constants.END_TIME, existingAssessmentEndTime);
        response.getResult().put(Constants.QUESTION_SET, questionSetFromAssessment);
    }

    /**
     * Previous attempt was submitted, or its window expired — read the assessment freshly and open a
     * new window. Returns the error message, or an empty string on success.
     */
    private String restartAttempt(SBApiResponse response, Map<String, Object> assessmentAllDetail,
                                  String assessmentIdentifier, String userId, Instant assessmentStartTime) {
        Map<String, Object> assessmentData = readAssessmentLevelData(assessmentAllDetail);
        int expectedDuration = (Integer) assessmentAllDetail.get(Constants.EXPECTED_DURATION);
        Instant assessmentEndTime = assessUtilServ.calculateAssessmentSubmitTime(expectedDuration, assessmentStartTime, 0);
        assessmentData.put(Constants.START_TIME, assessmentStartTime.toEpochMilli());
        assessmentData.put(Constants.END_TIME, assessmentEndTime.toEpochMilli());
        response.getResult().put(Constants.QUESTION_SET, assessmentData);

        boolean isAssessmentUpdatedToDB = assessmentRepository.addUserAssesmentDataToDB(userId, assessmentIdentifier,
                assessmentStartTime, assessmentEndTime, assessmentData, Constants.NOT_SUBMITTED);
        return isAssessmentUpdatedToDB ? "" : Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED;
    }

    @Override
    public SBApiResponse readQuestionList(Map<String, Object> requestBody, String authUserToken,boolean editMode) {
        SBApiResponse response = createDefaultResponse(Constants.API_QUESTIONS_LIST);
        String errMsg;
        Map<String, String> result = new HashMap<>();
        try {
            List<String> identifierList = new ArrayList<>();
            List<Object> questionList = new ArrayList<>();
            result = assessUtilServ.validateQuestionListAPI(requestBody, authUserToken, identifierList, editMode,
                    accessTokenValidator);
            errMsg = result.get(Constants.ERROR_MESSAGE);
            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(response, errMsg, HttpStatus.BAD_REQUEST);
                return response;
            }

            String assessmentIdFromRequest = (String) requestBody.get(Constants.ASSESSMENT_ID_KEY);
            Map<String, Object> questionsMap = assessUtilServ.readQListfromCache(identifierList,assessmentIdFromRequest,editMode,authUserToken);
            for (String questionId : identifierList) {
                questionList.add(assessUtilServ.filterQuestionMapDetail((Map<String, Object>) questionsMap.get(questionId),
                        result.get(Constants.PRIMARY_CATEGORY), Boolean.parseBoolean(result.getOrDefault(Constants.SHUFFLE, Constants.TRUE))));
            }
            if (errMsg.isEmpty() && identifierList.size() == questionList.size()) {
                response.getResult().put(Constants.QUESTIONS, questionList);
            }
        } catch (Exception e) {
            errMsg = String.format("Failed to fetch the question list. Exception: %s", e.getMessage());
            logger.error(errMsg, e);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.BAD_REQUEST);
        }
        return response;
    }

    @Override
    public SBApiResponse submitAssessment(Map<String, Object> data, String userAuthToken) {
        SBApiResponse response = createDefaultResponse(Constants.API_SUBMIT_ASSESSMENT);
        updateErrorDetails(response, "Method not supported", HttpStatus.NOT_IMPLEMENTED);
        return response;
    }

    public SBApiResponse readAssessmentResultV4(Map<String, Object> request, String userAuthToken) {
        return assessUtilServ.readAssessmentResult(request, userAuthToken, accessTokenValidator);
    }

    public SBApiResponse submitAssessmentAsync(Map<String, Object> submitRequest, String userAuthToken,boolean editMode) {
        logger.info("AssessmentServiceV4Impl::submitAssessmentAsync.. started");
        SBApiResponse outgoingResponse = createDefaultResponse(Constants.API_SUBMIT_ASSESSMENT);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (ObjectUtils.isEmpty(userId)) {
                updateErrorDetails(outgoingResponse, Constants.USER_ID_DOESNT_EXIST, HttpStatus.BAD_REQUEST);
                return outgoingResponse;
            }
            String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
            SubmitAssessmentData submitData = new SubmitAssessmentData();

            String errMsg = validateSubmitAssessmentRequest(submitRequest, userId, submitData, userAuthToken,
                    editMode);
            if (StringUtils.isBlank(errMsg)) {
                errMsg = assessUtilServ.validateAssessmentLanguageAndNodes(submitRequest);
            }
            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(outgoingResponse, errMsg, HttpStatus.BAD_REQUEST);
                return outgoingResponse;
            }

            Map<String, Object> assessmentHierarchy = submitData.assessmentHierarchy;
            String assessmentPrimaryCategory = (String) assessmentHierarchy.get(Constants.PRIMARY_CATEGORY);
            SubmissionContext context = new SubmissionContext(submitRequest, userId, userAuthToken,
                    assessmentHierarchy, submitData.existingAssessmentData, assessUtilServ.resolveCourseCategory(submitRequest));
            String scoreCutOffType = ((String) assessmentHierarchy.get(Constants.SCORE_CUTOFF_TYPE)).toLowerCase();
            List<Map<String, Object>> sectionLevelsResults = new ArrayList<>();
            for (Map<String, Object> hierarchySection : submitData.hierarchySectionList) {
                UserSection userSection = findUserSectionData(submitData.sectionListFromSubmitRequest,
                        (String) hierarchySection.get(Constants.IDENTIFIER));
                hierarchySection.put(Constants.SCORE_CUTOFF_TYPE, scoreCutOffType);
                List<Map<String, Object>> questionsListFromSubmitRequest = sectionChildren(userSection.data());
                List<String> questionsListFromAssessmentHierarchy = questionIdsFrom(questionsListFromSubmitRequest);
                if (Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF.equals(scoreCutOffType)) {
                    Map<String, Object> finalRes = calculateAssessmentFinalResults(scoreSection(hierarchySection,
                            questionsListFromAssessmentHierarchy, questionsListFromSubmitRequest,
                            assessmentIdFromRequest, editMode, userAuthToken));
                    outgoingResponse.getResult().putAll(finalRes);
                    outgoingResponse.getResult().put(Constants.PRIMARY_CATEGORY, assessmentPrimaryCategory);
                    persistSubmissionOutcome(context, finalRes, editMode);
                    return outgoingResponse;
                }
                if (Constants.SECTION_LEVEL_SCORE_CUTOFF.equals(scoreCutOffType)) {
                    sectionLevelsResults.add(scoreSection(hierarchySection, questionsListFromAssessmentHierarchy,
                            questionsListFromSubmitRequest, assessmentIdFromRequest, editMode, userAuthToken));
                }
            }
            if (Constants.SECTION_LEVEL_SCORE_CUTOFF.equalsIgnoreCase(scoreCutOffType)) {
                finaliseSectionLevelSubmission(outgoingResponse, sectionLevelsResults, context,
                        assessmentPrimaryCategory);
                return outgoingResponse;
            }

        } catch (Exception e) {
            String errMsg = String.format("Failed to process assessment submit request. Exception: %s", e.getMessage());
            logger.error(errMsg, e);
            updateErrorDetails(outgoingResponse, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
            assessUtilServ.publishFailedAssessmentAuditEvent((String) submitRequest.get(Constants.USER_ID),
                    (String) submitRequest.get(Constants.IDENTIFIER), submitRequest, errMsg, Constants.METHOD_V4_SUBMIT_ASSESSMENT_ASYNC, outgoingResponse.getResult());
        }
        return outgoingResponse;
    }

    /** What the async submit path needs after its preconditions have been checked. */
    private record AsyncSubmitData(String assessmentIdFromRequest, Map<String, Object> assessmentHierarchy,
                                   List<Map<String, Object>> hierarchySectionList,
                                   List<Map<String, Object>> sectionListFromSubmitRequest,
                                   Map<String, Object> existingAssessmentData) {
    }

    /**
     * Reads the hierarchy and the user's submission record, checking the preconditions for an async
     * submit. Returns {@code null} when it cannot proceed — the reason has already been logged, which
     * is what the original inline checks did before returning.
     */
    private AsyncSubmitData loadAsyncSubmitData(Map<String, Object> submitRequest, String userId, boolean editMode,
                                                String token) {
        String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
        Map<String, Object> assessmentHierarchy = assessUtilServ
                .readAssessmentHierarchyFromCache(assessmentIdFromRequest, editMode, token);
        if (MapUtils.isEmpty(assessmentHierarchy)) {
            logger.error(Constants.READ_ASSESSMENT_FAILED, new Exception(Constants.READ_ASSESSMENT_FAILED));
            return null;
        }
        List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(userId,
                assessmentIdFromRequest);
        if (existingDataList.isEmpty()) {
            logger.error(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT,
                    new Exception(Constants.USER_ASSESSMENT_DATA_NOT_PRESENT));
            return null;
        }
        if (Constants.SUBMITTED.equalsIgnoreCase((String) existingDataList.get(0).get(Constants.STATUS))) {
            logger.error(Constants.ASSESSMENT_ALREADY_SUBMITTED,
                    new Exception(Constants.ASSESSMENT_ALREADY_SUBMITTED));
            return null;
        }
        return new AsyncSubmitData(assessmentIdFromRequest, assessmentHierarchy,
                (List<Map<String, Object>>) assessmentHierarchy.get(Constants.CHILDREN),
                (List<Map<String, Object>>) submitRequest.get(Constants.CHILDREN), existingDataList.get(0));
    }

    /** The stored question set for an async submit, and the question ids for the section being scored. */
    private record AsyncSectionQuestions(Map<String, Object> questionSet, List<String> questionIds) {
    }

    /**
     * Resolves the question ids to score a section against, from the question set stored on the user's
     * record. Returns {@code null} when that question set is missing — the caller logs and stops, as
     * the original inline {@code else} branch did. When no section matches, the ids carried in from the
     * previous iteration are kept, again matching the original.
     */
    private AsyncSectionQuestions resolveAsyncSectionQuestions(Map<String, Object> existingAssessmentData,
                                                               Map<String, Object> questionSetFromAssessment,
                                                               String userSectionId, List<String> currentQuestionIds)
            throws IOException {
        Map<String, Object> questionSet = questionSetFromAssessment;
        String questionSetFromAssessmentString = (String) existingAssessmentData
                .get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
        if (StringUtils.isNotBlank(questionSetFromAssessmentString)) {
            questionSet = mapper.readValue(questionSetFromAssessmentString, new TypeReference<Map<String, Object>>() {
            });
        }
        if (questionSet == null || questionSet.get(Constants.CHILDREN) == null) {
            return null;
        }
        List<Map<String, Object>> sections = (List<Map<String, Object>>) questionSet.get(Constants.CHILDREN);
        for (Map<String, Object> section : sections) {
            if (userSectionId.equalsIgnoreCase((String) section.get(Constants.IDENTIFIER))) {
                return new AsyncSectionQuestions(questionSet, (List<String>) section.get(Constants.CHILD_NODES));
            }
        }
        return new AsyncSectionQuestions(questionSet, currentQuestionIds);
    }

    /** Rolls the per-section scores into the response and persists the submission. */
    private void finaliseSectionLevelSubmission(SBApiResponse outgoingResponse,
                                                List<Map<String, Object>> sectionLevelsResults,
                                                SubmissionContext context, String assessmentPrimaryCategory)
            throws IOException {
        Map<String, Object> result = calculateSectionFinalResults(sectionLevelsResults);
        outgoingResponse.getResult().putAll(result);
        outgoingResponse.getParams().setStatus(Constants.SUCCESS);
        outgoingResponse.setResponseCode(HttpStatus.OK);
        outgoingResponse.getResult().put(Constants.PRIMARY_CATEGORY, assessmentPrimaryCategory);
        persistSubmissionOutcome(context, result, false);
    }

    /** Everything a submission needs to be persisted, grouped so it can be passed as one argument. */
    private record SubmissionContext(Map<String, Object> submitRequest, String userId, String userAuthToken,
                                     Map<String, Object> assessmentHierarchy,
                                     Map<String, Object> existingAssessmentData, String courseCategory) {
    }

    /** The submitted section matching a hierarchy section, plus the id last examined while looking. */
    private record UserSection(String id, Map<String, Object> data) {
    }

    /**
     * Finds the submitted section matching a hierarchy section. When none matches, {@code data} is an
     * empty map and {@code id} is the id of the last section examined — matching the original loop,
     * which left {@code userSectionId} holding that value rather than resetting it.
     */
    private UserSection findUserSectionData(List<Map<String, Object>> sectionListFromSubmitRequest,
                                            String hierarchySectionId) {
        String userSectionId = "";
        for (Map<String, Object> sectionFromSubmitRequest : sectionListFromSubmitRequest) {
            userSectionId = (String) sectionFromSubmitRequest.get(Constants.IDENTIFIER);
            if (userSectionId.equalsIgnoreCase(hierarchySectionId)) {
                return new UserSection(userSectionId, sectionFromSubmitRequest);
            }
        }
        return new UserSection(userSectionId, new HashMap<>());
    }

    /** The submitted children of a section, or an empty list when it carries none. */
    private List<Map<String, Object>> sectionChildren(Map<String, Object> userSectionData) {
        if (userSectionData.containsKey(Constants.CHILDREN)
                && !ObjectUtils.isEmpty(userSectionData.get(Constants.CHILDREN))) {
            return (List<Map<String, Object>>) userSectionData.get(Constants.CHILDREN);
        }
        return new ArrayList<>();
    }

    /** The identifiers of the questions carried by a submitted section. */
    private List<String> questionIdsFrom(List<Map<String, Object>> questionsListFromSubmitRequest) {
        List<String> desiredKeys = List.of(Constants.IDENTIFIER);
        List<Object> questionsList = questionsListFromSubmitRequest.stream()
                .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).toList();
        return questionsList.stream().map(object -> Objects.toString(object, null))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /** Scores one section of the submission against the assessment hierarchy. */
    private Map<String, Object> scoreSection(Map<String, Object> hierarchySection,
                                             List<String> questionsListFromAssessmentHierarchy,
                                             List<Map<String, Object>> questionsListFromSubmitRequest,
                                             String assessmentIdFromRequest, boolean editMode, String userAuthToken)
            throws IOException {
        Map<String, Object> result = new HashMap<>();
        result.putAll(createResponseMapWithProperStructure(hierarchySection,
                assessUtilServ.validateQumlAssessment(questionsListFromAssessmentHierarchy,
                        questionsListFromSubmitRequest,
                        assessUtilServ.readQListfromCache(questionsListFromAssessmentHierarchy,
                                assessmentIdFromRequest, editMode, userAuthToken))));
        return result;
    }

    /**
     * Writes the submission and fires its Kafka event. A practice question set is never persisted —
     * it only updates content progress. {@code editMode} suppresses the write entirely, matching the
     * original assessment-level behaviour; the section-level path passes {@code false}.
     */
    private void persistSubmissionOutcome(SubmissionContext context, Map<String, Object> result, boolean editMode)
            throws IOException {
        String assessmentPrimaryCategory = (String) context.assessmentHierarchy().get(Constants.PRIMARY_CATEGORY);
        boolean practiceQuestionSet = Constants.PRACTICE_QUESTION_SET.equalsIgnoreCase(assessmentPrimaryCategory);
        if (!practiceQuestionSet && !editMode) {
            String questionSetFromAssessmentString = (String) context.existingAssessmentData()
                    .get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
            Map<String, Object> questionSetFromAssessment = null;
            if (StringUtils.isNotBlank(questionSetFromAssessmentString)) {
                questionSetFromAssessment = mapper.readValue(questionSetFromAssessmentString,
                        new TypeReference<Map<String, Object>>() {
                        });
            }
            writeDataToDatabaseAndTriggerKafkaEvent(new KafkaEventContext(context.submitRequest(), context.userId(),
                            context.userAuthToken(), assessmentPrimaryCategory, context.courseCategory(),
                            (String) context.assessmentHierarchy().get(Constants.CONTEXT_CATEGORY_TAG), true),
                    questionSetFromAssessment, result);
        } else if (practiceQuestionSet) {
            updatePracticeAssessmentProgress(context);
        }
    }

    private void updatePracticeAssessmentProgress(SubmissionContext context) {
        SBApiResponse contentUpdateResponse = new SBApiResponse();
        String response = contentService.updateContentProgress(context.userAuthToken(), context.submitRequest(),
                context.userId(), contentUpdateResponse);
        if (!Constants.SUCCESS.equalsIgnoreCase(response)) {
            logger.error("AssessmentServiceV4Impl : submitAssessmentAsync : Update while updating the progress of practice assessment");
        }
    }

    public void handleAssessmentSubmitRequest(Map<String, Object> asyncRequest,boolean editMode,String token) {
        String userId = (String) asyncRequest.get(Constants.USER_ID_CONSTANT);
        Map<String, Object> submitRequest = (Map<String, Object>) asyncRequest.get(Constants.REQUEST);

        String errMsg = "";
        try {
            AsyncSubmitData data = loadAsyncSubmitData(submitRequest, userId, editMode, token);
            if (data == null) {
                return;
            }
            Map<String, Object> assessmentHierarchy = data.assessmentHierarchy();
            List<String> questionsListFromAssessmentHierarchy = new ArrayList<>();
            String scoreCutOffType = ((String) assessmentHierarchy.get(Constants.SCORE_CUTOFF_TYPE)).toLowerCase();
            List<Map<String, Object>> sectionLevelsResults = new ArrayList<>();
            Map<String, Object> questionSetFromAssessment = new HashMap<>();
            boolean practiceQuestionSet = ((String) (assessmentHierarchy.get(Constants.PRIMARY_CATEGORY)))
                    .equalsIgnoreCase(Constants.PRACTICE_QUESTION_SET);
            List<Map<String, Object>> sectionsToScore = practiceQuestionSet
                    ? Collections.emptyList()
                    : data.hierarchySectionList();
            for (Map<String, Object> hierarchySection : sectionsToScore) {
                UserSection userSection = findUserSectionData(data.sectionListFromSubmitRequest(),
                        (String) hierarchySection.get(Constants.IDENTIFIER));
                AsyncSectionQuestions sectionQuestions = resolveAsyncSectionQuestions(data.existingAssessmentData(),
                        questionSetFromAssessment, userSection.id(), questionsListFromAssessmentHierarchy);
                if (sectionQuestions == null) {
                    errMsg = "Question Set From The Database returns Null. Failed to calculate assessment score.";
                    logger.error(errMsg, new Exception(errMsg));
                    break;
                }
                questionSetFromAssessment = sectionQuestions.questionSet();
                questionsListFromAssessmentHierarchy = sectionQuestions.questionIds();

                hierarchySection.put(Constants.SCORE_CUTOFF_TYPE, scoreCutOffType);
                List<Map<String, Object>> questionsListFromSubmitRequest = sectionChildren(userSection.data());
                boolean assessmentLevel = Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF.equals(scoreCutOffType);
                if (assessmentLevel || Constants.SECTION_LEVEL_SCORE_CUTOFF.equals(scoreCutOffType)) {
                    Map<String, Object> result = scoreSection(hierarchySection, questionsListFromAssessmentHierarchy,
                            questionsListFromSubmitRequest, data.assessmentIdFromRequest(), editMode, token);
                    if (assessmentLevel) {
                        Map<String, Object> finalResult = calculateAssessmentFinalResults(result);
                        finalResult.put(Constants.STATUS_IS_IN_PROGRESS, false);
                        writeDataToDatabaseAndTriggerKafkaEvent(new KafkaEventContext(submitRequest, userId, null,
                                        (String) assessmentHierarchy.get(Constants.PRIMARY_CATEGORY), null,
                                        (String) assessmentHierarchy.get(Constants.CONTEXT_CATEGORY_TAG), false),
                                questionSetFromAssessment, finalResult);
                    } else {
                        sectionLevelsResults.add(result);
                    }
                }
            }
            if (errMsg.isEmpty() && !ObjectUtils.isEmpty(scoreCutOffType)
                    && scoreCutOffType.equalsIgnoreCase(Constants.SECTION_LEVEL_SCORE_CUTOFF)) {
                Map<String, Object> result = calculateSectionFinalResults(sectionLevelsResults);
                result.put(Constants.STATUS_IS_IN_PROGRESS, false);
                writeDataToDatabaseAndTriggerKafkaEvent(new KafkaEventContext(submitRequest, userId, null,
                                (String) assessmentHierarchy.get(Constants.PRIMARY_CATEGORY), null,
                                (String) assessmentHierarchy.get(Constants.CONTEXT_CATEGORY_TAG), false),
                        questionSetFromAssessment, result);
            }
        } catch (Exception e) {
            errMsg = String.format("Failed to process assessent submit request. Exception: %s", e.getMessage());
            logger.error(errMsg, e);
        }
    }


    private Map<String, Object> readAssessmentLevelData(Map<String, Object> assessmentAllDetail) {
        List<String> assessmentParams = serverProperties.getAssessmentLevelParams();
        Map<String, Object> assessmentFilteredDetail = new HashMap<>();
        for (String assessmentParam : assessmentParams) {
            if ((assessmentAllDetail.containsKey(assessmentParam))) {
                assessmentFilteredDetail.put(assessmentParam, assessmentAllDetail.get(assessmentParam));
            }
        }
        readSectionLevelParams(assessmentAllDetail, assessmentFilteredDetail);
        return assessmentFilteredDetail;
    }

    private void readSectionLevelParams(Map<String, Object> assessmentAllDetail,
            Map<String, Object> assessmentFilteredDetail) {
        List<Map<String, Object>> sectionResponse = new ArrayList<>();
        List<String> sectionIdList = new ArrayList<>();
        List<String> sectionParams = serverProperties.getAssessmentSectionParams();
        List<Map<String, Object>> sections = (List<Map<String, Object>>) assessmentAllDetail.get(Constants.CHILDREN);
        for (Map<String, Object> section : sections) {
            sectionIdList.add((String) section.get(Constants.IDENTIFIER));
            Map<String, Object> newSection = new HashMap<>();
            for (String sectionParam : sectionParams) {
                if (section.containsKey(sectionParam)) {
                    newSection.put(sectionParam, section.get(sectionParam));
                }
            }
            List<Map<String, Object>> questions = (List<Map<String, Object>>) section.get(Constants.CHILDREN);
            // Shuffle the list of questions
            Collections.shuffle(questions);
            int maxQuestions = (int) section.getOrDefault(Constants.MAX_QUESTIONS, questions.size());
            List<String> childNodeList = questions.stream()
                    .map(question -> (String) question.get(Constants.IDENTIFIER))
                    .limit(maxQuestions)
                    .collect(Collectors.toList());
            Collections.shuffle(childNodeList);
            newSection.put(Constants.CHILD_NODES, childNodeList);
            sectionResponse.add(newSection);
        }
        assessmentFilteredDetail.put(Constants.CHILDREN, sectionResponse);
        assessmentFilteredDetail.put(Constants.CHILD_NODES, sectionIdList);
    }

    /**
     * The four collections {@link #validateSubmitAssessmentRequest} fills in for its caller. Grouped
     * into one object so the method stays within the parameter limit; the collections are still
     * populated in place, exactly as when they were passed individually.
     */
    private static final class SubmitAssessmentData {
        private final Map<String, Object> assessmentHierarchy = new HashMap<>();
        private final List<Map<String, Object>> hierarchySectionList = new ArrayList<>();
        private final List<Map<String, Object>> sectionListFromSubmitRequest = new ArrayList<>();
        private final Map<String, Object> existingAssessmentData = new HashMap<>();
    }

    private String validateSubmitAssessmentRequest(Map<String, Object> submitRequest, String userId,
                                                   SubmitAssessmentData submitData, String token, boolean editMode)
            throws IOException {
        Map<String, Object> assessmentHierarchy = submitData.assessmentHierarchy;
        Map<String, Object> existingAssessmentData = submitData.existingAssessmentData;

        submitRequest.put(Constants.USER_ID, userId);

        if (StringUtils.isEmpty((String) submitRequest.get(Constants.IDENTIFIER))) {
            return Constants.INVALID_ASSESSMENT_ID;
        }

        String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
        assessmentHierarchy.putAll(assessUtilServ.readAssessmentHierarchyFromCache(assessmentIdFromRequest, editMode, token));

        if (MapUtils.isEmpty(assessmentHierarchy)) {
            return Constants.READ_ASSESSMENT_FAILED;
        }

        submitData.hierarchySectionList.addAll((List<Map<String, Object>>) assessmentHierarchy.get(Constants.CHILDREN));
        submitData.sectionListFromSubmitRequest.addAll((List<Map<String, Object>>) submitRequest.get(Constants.CHILDREN));

        if (((String) assessmentHierarchy.get(Constants.PRIMARY_CATEGORY)).equalsIgnoreCase(Constants.PRACTICE_QUESTION_SET) || editMode) {
            return "";
        }

        List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                userId, (String) submitRequest.get(Constants.IDENTIFIER));

        if (existingDataList.isEmpty()) {
            return Constants.USER_ASSESSMENT_DATA_NOT_PRESENT;
        }
        existingAssessmentData.putAll(existingDataList.get(0));

        Instant assessmentStartTime = assessUtilServ.resolveAssessmentStartTimeAsInstant(existingAssessmentData.get(Constants.START_TIME));
        if (assessmentStartTime == null) {
            return Constants.READ_ASSESSMENT_START_TIME_FAILED;
        }
        Integer expectedDuration = (Integer) assessmentHierarchy.get(Constants.EXPECTED_DURATION);
        int userSubmissionDuration = Integer.parseInt(serverProperties.getUserAssessmentSubmissionDuration());

        Instant allowedSubmitTime = assessmentStartTime
                .plus(Duration.ofMinutes((long) expectedDuration + userSubmissionDuration));

        Instant submissionTime = Instant.now();

        if (submissionTime.isAfter(allowedSubmitTime)) {
            return Constants.ASSESSMENT_SUBMIT_EXPIRED;
        }

        List<String> desiredKeys = List.of(Constants.IDENTIFIER);

        List<Object> hierarchySectionIds = submitData.hierarchySectionList.stream()
                .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).toList();

        List<Object> submitSectionIds = submitData.sectionListFromSubmitRequest.stream()
                .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).toList();

        if (!new HashSet<>(hierarchySectionIds).containsAll(submitSectionIds)) {
            return Constants.WRONG_SECTION_DETAILS;
        }
        return assessUtilServ.validateIfQuestionIdsAreSame(submitData.sectionListFromSubmitRequest, desiredKeys,
                existingAssessmentData);
    }

    public Map<String, Object> createResponseMapWithProperStructure(Map<String, Object> hierarchySection,
                                                                    Map<String, Object> resultMap) throws ApplicationLogicError {
        Map<String, Object> sectionLevelResult = new HashMap<>();
        sectionLevelResult.put(Constants.IDENTIFIER, hierarchySection.get(Constants.IDENTIFIER));
        sectionLevelResult.put(Constants.OBJECT_TYPE, hierarchySection.get(Constants.OBJECT_TYPE));
        sectionLevelResult.put(Constants.PRIMARY_CATEGORY, hierarchySection.get(Constants.PRIMARY_CATEGORY));
        sectionLevelResult.put(Constants.PASS_PERCENTAGE, hierarchySection.get(Constants.MINIMUM_PASS_PERCENTAGE));
        Double result;
        if (!ObjectUtils.isEmpty(resultMap)) {
            result = (Double) resultMap.get(Constants.RESULT);
            sectionLevelResult.put(Constants.RESULT, result);
            sectionLevelResult.put(Constants.TOTAL, resultMap.get(Constants.TOTAL));
            sectionLevelResult.put(Constants.BLANK, resultMap.get(Constants.BLANK));
            sectionLevelResult.put(Constants.CORRECT, resultMap.get(Constants.CORRECT));
            sectionLevelResult.put(Constants.INCORRECT, resultMap.get(Constants.INCORRECT));
            sectionLevelResult.put(Constants.CHILDREN,resultMap.get(Constants.CHILDREN));
        } else {
            result = 0.0;
            sectionLevelResult.put(Constants.RESULT, result);
            List<String> childNodes = (List<String>) hierarchySection.get(Constants.CHILDREN);
            sectionLevelResult.put(Constants.TOTAL, childNodes.size());
            sectionLevelResult.put(Constants.BLANK, childNodes.size());
            sectionLevelResult.put(Constants.CORRECT, 0);
            sectionLevelResult.put(Constants.INCORRECT, 0);
        }
        sectionLevelResult.put(Constants.PASS,
                result >= ((Integer) hierarchySection.get(Constants.MINIMUM_PASS_PERCENTAGE)));
        sectionLevelResult.put(Constants.OVERALL_RESULT, result);
        return sectionLevelResult;
    }

    private Map<String, Object> calculateAssessmentFinalResults(Map<String, Object> assessmentLevelResult) throws ApplicationLogicError {
        Map<String, Object> res = new HashMap<>();
        try {
            res.put(Constants.CHILDREN, Collections.singletonList(assessmentLevelResult));
            Double result = (Double) assessmentLevelResult.get(Constants.RESULT);
            res.put(Constants.OVERALL_RESULT, result);
            res.put(Constants.TOTAL, assessmentLevelResult.get(Constants.TOTAL));
            res.put(Constants.BLANK, assessmentLevelResult.get(Constants.BLANK));
            res.put(Constants.CORRECT, assessmentLevelResult.get(Constants.CORRECT));
            res.put(Constants.PASS_PERCENTAGE, assessmentLevelResult.get(Constants.PASS_PERCENTAGE));
            res.put(Constants.INCORRECT, assessmentLevelResult.get(Constants.INCORRECT));
            Integer minimumPassPercentage = (Integer) assessmentLevelResult.get(Constants.PASS_PERCENTAGE);
            res.put(Constants.PASS, result >= minimumPassPercentage);
        } catch (Exception e) {
            logger.error("Failed to calculate Assessment final results. Exception: ", e);
        }
        return res;
    }

    /**
     * Who submitted what, and how the submission should be reported. Grouped into one object so
     * {@link #writeDataToDatabaseAndTriggerKafkaEvent} stays within the parameter limit.
     */
    private record KafkaEventContext(Map<String, Object> submitRequest, String userId, String userAuthToken,
                                     String primaryCategory, String courseCategory, String contextCategory,
                                     boolean shouldUpdateContentProgress) {
    }

    private void writeDataToDatabaseAndTriggerKafkaEvent(KafkaEventContext eventContext,
                                                         Map<String, Object> questionSetFromAssessment,
                                                         Map<String, Object> result) throws ApplicationLogicError {
        Map<String, Object> submitRequest = eventContext.submitRequest();
        String contextCategory = eventContext.contextCategory();
        try {
            if (questionSetFromAssessment == null || questionSetFromAssessment.get(Constants.START_TIME) == null) {
                logger.error("AssessmentServiceV4Impl : writeDataToDatabaseAndTriggerKafkaEvent : "
                        + "questionSetFromAssessment is null or missing start time, skipping DB write and Kafka event");
                return;
            }
            Instant startTime = assessUtilServ.parseStartTimeToInstant(questionSetFromAssessment.get(Constants.START_TIME));
            Boolean isAssessmentUpdatedToDB = assessmentRepository.updateUserAssesmentDataToDB(eventContext.userId(),
                    (String) submitRequest.get(Constants.IDENTIFIER), submitRequest, result, Constants.SUBMITTED,
                    startTime,null);
            if (!Boolean.TRUE.equals(isAssessmentUpdatedToDB)
                    || !assessUtilServ.proceedWithContentUpdate(contextCategory, eventContext.courseCategory(),
                            (boolean) result.get(Constants.PASS))) {
                return;
            }
            if (eventContext.shouldUpdateContentProgress()) {
                assessUtilServ.updateContentProgressForContext(contextCategory, eventContext.userAuthToken(),
                        eventContext.submitRequest(), eventContext.userId());
            }
            kafkaProducer.push(serverProperties.getAssessmentSubmitTopic(),
                    assessUtilServ.buildSubmitEvent(submitRequest, eventContext.primaryCategory(), result));
        } catch (Exception e) {
            logger.error("Failed to write data for assessment submit response. Exception: ", e);
        }
    }

    private Map<String, Object> calculateSectionFinalResults(List<Map<String, Object>> sectionLevelResults) throws ApplicationLogicError {
        Map<String, Object> res = new HashMap<>();
        Double result;
        Integer correct = 0;
        Integer blank = 0;
        Integer inCorrect = 0;
        Integer total = 0;
        int pass = 0;
        Double totalResult = 0.0;
        try {
            for (Map<String, Object> sectionChildren : sectionLevelResults) {
                res.put(Constants.CHILDREN, sectionLevelResults);
                result = (Double) sectionChildren.get(Constants.RESULT);
                totalResult += result;
                total += (Integer) sectionChildren.get(Constants.TOTAL);
                blank += (Integer) sectionChildren.get(Constants.BLANK);
                correct += (Integer) sectionChildren.get(Constants.CORRECT);
                inCorrect += (Integer) sectionChildren.get(Constants.INCORRECT);
                Integer minimumPassPercentage = (Integer) sectionChildren.get(Constants.PASS_PERCENTAGE);
                if (result >= minimumPassPercentage) {
                    pass++;
                }
            }
            res.put(Constants.OVERALL_RESULT, totalResult / sectionLevelResults.size());
            res.put(Constants.TOTAL, total);
            res.put(Constants.BLANK, blank);
            res.put(Constants.CORRECT, correct);
            res.put(Constants.INCORRECT, inCorrect);
            res.put(Constants.PASS, (pass == sectionLevelResults.size()));
        } catch (Exception e) {
            logger.error("Failed to calculate assessment score. Exception: ", e);
        }
        return res;
    }

    public SBApiResponse readWheebox(String userAuthToken) {
        SBApiResponse response = createDefaultResponse(Constants.API_READ_ASSESSMENT_RESULT);
           try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            Map<String, Object> res =assessUtilServ.fetchWheebox(userId);
            if (res !=null && !res.isEmpty()) {
                response.getResult().put(RESPONSE,res);
            }
        } catch (Exception e) {
            String errMsg = String.format("Failed to process Assessment read response. Excption: %s", e.getMessage());
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }


}
