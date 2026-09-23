package com.igot.cb.assessment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.igot.cb.assessment.repo.AssessmentRepository;
import com.igot.cb.cassandra.utils.CassandraOperation;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.service.ContentService;
import com.igot.cb.common.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.common.util.AccessTokenValidator;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import com.igot.cb.core.exception.ApplicationLogicError;
import com.igot.cb.core.producer.Producer;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

import static com.igot.cb.common.util.ProjectUtil.createDefaultResponse;
import static com.igot.cb.common.util.ProjectUtil.updateErrorDetails;
import static java.util.stream.Collectors.toList;

@Service
@SuppressWarnings("unchecked")
public class AssessmentServiceV5Impl implements AssessmentServiceV5 {

    private static final String ASSESSMENT_READ_USER_HAS_DETAILS = "Assessment read... user has details... ";
    private static final String ERR_READING_ASSESSMENT = "Error while reading assessment. Exception: %s";
    private static final String ERR_PRACTICE_PROGRESS_UPDATE =
            "AssessmentServiceV5Impl : submitAssessmentAsync : Update while updating the progress of practice assessment";

    private final Logger logger = LoggerFactory.getLogger(AssessmentServiceV5Impl.class);
    CbExtAssessmentServerProperties serverProperties;

    Producer kafkaProducer;

    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    AssessmentUtilServiceV2 assessUtilServ;

    ObjectMapper mapper;

    AssessmentRepository assessmentRepository;

    AccessTokenValidator accessTokenValidator;

    ContentService contentService;
  
    CassandraOperation cassandraOperation;

    private Producer producer;

    public AssessmentServiceV5Impl(CbExtAssessmentServerProperties serverProperties, Producer kafkaProducer, OutboundRequestHandlerServiceImpl outboundRequestHandlerService, AssessmentUtilServiceV2 assessUtilServ, ObjectMapper mapper, AssessmentRepository assessmentRepository, AccessTokenValidator accessTokenValidator, ContentService contentService, CassandraOperation cassandraOperation, Producer producer) {
        this.serverProperties = serverProperties;
        this.kafkaProducer = kafkaProducer;
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.assessUtilServ = assessUtilServ;
        this.mapper = mapper;
        this.assessmentRepository = assessmentRepository;
        this.accessTokenValidator = accessTokenValidator;
        this.contentService = contentService;
        this.cassandraOperation = cassandraOperation;
        this.producer = producer;
    }

    @Override
    public SBApiResponse retakeAssessment(String assessmentIdentifier, String token, Boolean editMode) {
        logger.info("AssessmentServicev5Impl::retakeAssessment... Started");
        SBApiResponse response = createDefaultResponse(Constants.API_RETAKE_ASSESSMENT_GET);
        String errMsg = "";
        int retakeAttemptsAllowed = 0;
        int retakeAttemptsConsumed = 0;
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
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
            Object contextCategory = assessmentAllDetail.get(Constants.CONTEXT_CATEGORY_TAG);
            if (contextCategory != null && Constants.PRE_ENROLLED_ASSESSMENT_KEY.equals(contextCategory.toString())) {
                retakeAttemptsAllowed = 1;
                retakeAttemptsConsumed = 0;
            } else {
                if (assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS) != null) {
                    retakeAttemptsAllowed = (int) assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS);
                    if (retakeAttemptsAllowed == 0) {
                        retakeAttemptsConsumed = -1;
                        response.getResult().put(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED, retakeAttemptsAllowed);
                        response.getResult().put(Constants.RETAKE_ATTEMPTS_CONSUMED, retakeAttemptsConsumed);
                        return response;
                    }
                }
                List<Map<String, Object>> userAssessmentDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                        userId, assessmentIdentifier);
                retakeAttemptsConsumed = calculateRetakeAttemptsConsumedV5(userId, assessmentIdentifier,
                        assessmentAllDetail, retakeAttemptsAllowed, userAssessmentDataList, response);
                if (HttpStatus.BAD_REQUEST.equals(response.getResponseCode())) {
                    return response;
                }
            }
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
        logger.info("AssessmentServicev5Impl::retakeAssessment... Completed");
        return response;
    }

    @Override
    public SBApiResponse readAssessment(String assessmentIdentifier, String token,boolean editMode, String parentContextId) {
        logger.info("AssessmentServicev5Impl::readAssessment... Started");
        SBApiResponse response = createDefaultResponse(Constants.API_READ_ASSESSMENT);
        String errMsg = "";
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            logger.info("ReadAssessment... UserId: {}, AssessmentIdentifier: {}", userId, assessmentIdentifier);

            Map<String, Object> assessmentAllDetail = null ;

            // Step-1 : Read assessment using assessment Id from the Assessment Service
            if(editMode) {
                assessmentAllDetail = assessUtilServ.fetchHierarchyFromAssessServc(assessmentIdentifier,token);
            }
            else {
                assessmentAllDetail = assessUtilServ
                        .readAssessmentHierarchyFromCache(assessmentIdentifier,editMode,token);
            }

            if (MapUtils.isEmpty(assessmentAllDetail)) {
                updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED,
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            //Step-2 : If Practice Assessment return without saving
            if (Constants.PRACTICE_QUESTION_SET
                    .equalsIgnoreCase((String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY))||editMode) {
                response.getResult().put(Constants.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail));
                return response;
            }

            // Step-3 : If read user submitted assessment
            List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                    userId, assessmentIdentifier);
            Instant assessmentStartTime = Instant.now();

            ReadContext readContext = new ReadContext(assessmentAllDetail, parentContextId, response, userId,
                    assessmentIdentifier, assessmentStartTime);
            ReadOutcome outcome = existingDataList.isEmpty()
                    ? readAssessmentFirstTime(readContext)
                    : readAssessmentForReturningUser(readContext, existingDataList);
            if (outcome.returnImmediately()) {
                return response;
            }
            errMsg = outcome.errMsg();
        } catch (Exception e) {
            errMsg = String.format(ERR_READING_ASSESSMENT, e.getMessage());
            logger.error(errMsg, e);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Inputs shared by the read-assessment branches of {@link #readAssessment}.
     */
    private record ReadContext(Map<String, Object> assessmentAllDetail, String parentContextId, SBApiResponse response,
                               String userId, String assessmentIdentifier, Instant assessmentStartTime) {
    }

    /**
     * Result of a read-assessment branch. {@code returnImmediately} reproduces the original
     * early {@code return response;} statements, where the response has already been populated
     * and the error message must not be written again.
     */
    private record ReadOutcome(String errMsg, boolean returnImmediately) {
    }

    private ReadOutcome readAssessmentFirstTime(ReadContext ctx) {
        logger.info("Assessment read first time for user.");
        // Add Null check for expectedDuration.throw bad questionSet Assessment Exam
        if (null == ctx.assessmentAllDetail().get(Constants.EXPECTED_DURATION)) {
            return new ReadOutcome(Constants.ASSESSMENT_INVALID, false);
        }
        String errMsg = assessUtilServ.validateContextLocking(ctx.assessmentAllDetail(), ctx.parentContextId(),
                ctx.response(), ctx.userId(), ctx.assessmentIdentifier());
        if (StringUtils.isNotBlank(errMsg)) {
            return new ReadOutcome(errMsg, true);
        }
        return new ReadOutcome(startFreshAttempt(ctx), false);
    }

    private ReadOutcome readAssessmentForReturningUser(ReadContext ctx, List<Map<String, Object>> existingDataList) {
        logger.info(ASSESSMENT_READ_USER_HAS_DETAILS);
        Map<String, Object> latestAttempt = existingDataList.get(0);
        Object endTimeObj = latestAttempt.get(Constants.END_TIME);
        Date existingAssessmentEndTime = endTimeObj instanceof Instant instant
                ? Date.from(instant)
                : (Date) endTimeObj;
        Instant assessmentStartTime = ctx.assessmentStartTime();
        Instant existingEndInstant = existingAssessmentEndTime.toInstant();

        if (assessmentStartTime.isBefore(existingEndInstant)
                && Constants.NOT_SUBMITTED.equalsIgnoreCase((String) latestAttempt.get(Constants.STATUS))) {
            resumeInProgressAttempt(ctx, latestAttempt, existingAssessmentEndTime);
            return new ReadOutcome("", false);
        }

        boolean submittedBeforeEndTime = assessmentStartTime.compareTo(existingEndInstant) < 0
                && ((String) latestAttempt.get(Constants.STATUS)).equalsIgnoreCase(Constants.SUBMITTED);
        if (!submittedBeforeEndTime && assessmentStartTime.compareTo(existingEndInstant) <= 0) {
            return new ReadOutcome("", false);
        }
        return startRetakeAttempt(ctx, existingDataList);
    }

    private void resumeInProgressAttempt(ReadContext ctx, Map<String, Object> latestAttempt,
                                         Date existingAssessmentEndTime) {
        String questionSetFromAssessmentString = (String) latestAttempt
                .get(Constants.ASSESSMENT_READ_RESPONSE_KEY);

        Map<String, Object> questionSetFromAssessment = new Gson().fromJson(
                questionSetFromAssessmentString, new TypeToken<HashMap<String, Object>>() {
                }.getType());

        questionSetFromAssessment.put(Constants.START_TIME, ctx.assessmentStartTime().toEpochMilli());
        questionSetFromAssessment.put(Constants.END_TIME, existingAssessmentEndTime);

        ctx.response().getResult().put(Constants.QUESTION_SET, questionSetFromAssessment);
    }

    private ReadOutcome startRetakeAttempt(ReadContext ctx, List<Map<String, Object>> existingDataList) {
        logger.info(
                "Incase the assessment is submitted before the end time, or the endtime has exceeded, read assessment freshly ");
        Object maxRetakeAttempts = ctx.assessmentAllDetail().get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS);
        if (maxRetakeAttempts != null) {
            int retakeAttemptsAllowed = (int) maxRetakeAttempts + 1;
            if (retakeAttemptsAllowed > 0) {
                calculateRetakeAttemptsConsumed(ctx.userId(), ctx.assessmentIdentifier(), ctx.assessmentAllDetail(),
                        retakeAttemptsAllowed, existingDataList, ctx.response());
                if (ctx.response().getResponseCode() != HttpStatus.OK) {
                    return new ReadOutcome("", true);
                }
            }
        }
        String errMsg = assessUtilServ.validateContextLocking(ctx.assessmentAllDetail(), ctx.parentContextId(),
                ctx.response(), ctx.userId(), ctx.assessmentIdentifier());
        if (StringUtils.isNotBlank(errMsg)) {
            return new ReadOutcome(errMsg, true);
        }
        return new ReadOutcome(startFreshAttempt(ctx), false);
    }

    /**
     * Builds the assessment payload for a brand new attempt, publishes it on the response and
     * persists the attempt window. Returns the error message, or an empty string on success.
     */
    private String startFreshAttempt(ReadContext ctx) {
        Map<String, Object> assessmentData = readAssessmentLevelData(ctx.assessmentAllDetail());
        int expectedDuration = (Integer) ctx.assessmentAllDetail().get(Constants.EXPECTED_DURATION);
        Instant assessmentEndTime = calculateAssessmentSubmitTime(expectedDuration, ctx.assessmentStartTime(), 0);

        assessmentData.put(Constants.START_TIME, ctx.assessmentStartTime());
        assessmentData.put(Constants.END_TIME, assessmentEndTime);
        ctx.response().getResult().put(Constants.QUESTION_SET, assessmentData);
        Boolean isAssessmentUpdatedToDB = assessmentRepository.addUserAssesmentDataToDB(ctx.userId(),
                ctx.assessmentIdentifier(), ctx.assessmentStartTime(), assessmentEndTime,
                assessmentData, Constants.NOT_SUBMITTED);
        if (Boolean.FALSE.equals(isAssessmentUpdatedToDB)) {
            return Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED;
        }
        return "";
    }

    @Override
    public SBApiResponse readQuestionList(Map<String, Object> requestBody, String authUserToken,boolean editMode) {
        SBApiResponse response = createDefaultResponse(Constants.API_QUESTIONS_LIST);
        String errMsg;
        Map<String, String> result = new HashMap<>();
        try {
            List<String> identifierList = new ArrayList<>();
            List<Object> questionList = new ArrayList<>();
            result = validateQuestionListAPI(requestBody, authUserToken, identifierList,editMode);
            errMsg = result.get(Constants.ERROR_MESSAGE);
            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(response, errMsg, HttpStatus.BAD_REQUEST);
                return response;
            }

            String assessmentIdFromRequest = (String) requestBody.get(Constants.ASSESSMENT_ID_KEY);
            Map<String, Object> questionsMap = assessUtilServ.readQListfromCache(identifierList,assessmentIdFromRequest,editMode,authUserToken);
            for (String questionId : identifierList) {
                questionList.add(assessUtilServ.filterQuestionMapDetailV2((Map<String, Object>) questionsMap.get(questionId),
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

    public SBApiResponse readAssessmentResultV5(Map<String, Object> request, String userAuthToken) {
        SBApiResponse response = createDefaultResponse(Constants.API_READ_ASSESSMENT_RESULT);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            String errMsg = validateAssessmentReadResult(request);
            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(response, errMsg, HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> requestBody = (Map<String, Object>) request.get(Constants.REQUEST);
            String assessmentIdentifier = (String) requestBody.get(Constants.ASSESSMENT_ID_KEY);

            List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                    userId, assessmentIdentifier);

            if (existingDataList.isEmpty()) {
                updateErrorDetails(response, Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, HttpStatus.BAD_REQUEST);
                return response;
            }

            String statusOfLatestObject = (String) existingDataList.get(0).get(Constants.STATUS);
            if (!Constants.SUBMITTED.equalsIgnoreCase(statusOfLatestObject)) {
                response.getResult().put(Constants.STATUS_IS_IN_PROGRESS, true);
                return response;
            }

            String latestResponse = (String) existingDataList.get(0).get(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY);
            if (StringUtils.isNotBlank(latestResponse)) {
                response.putAll(mapper.readValue(latestResponse, new TypeReference<Map<String, Object>>() {
                }));
            }
        } catch (Exception e) {
            String errMsg = String.format("Failed to process Assessment read response. Excption: %s", e.getMessage());
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public SBApiResponse submitAssessmentAsync(Map<String, Object> submitRequest, String userAuthToken,boolean editMode) {
        return runSubmitAssessmentAsync(submitRequest, userAuthToken, editMode, "submitAssessmentAsync",
                Constants.METHOD_V5_SUBMIT_ASSESSMENT_ASYNC);
    }

    private SBApiResponse runSubmitAssessmentAsync(Map<String, Object> submitRequest, String userAuthToken,
            boolean editMode, String methodLabel, String auditMethodName) {
        logger.info("AssessmentServicev5Impl::{}.. started", methodLabel);
        SBApiResponse outgoingResponse = createDefaultResponse(Constants.API_SUBMIT_ASSESSMENT);
        long assessmentCompletionTime= Calendar.getInstance().getTime().getTime();
        try {
            // Step-1 fetch userid
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (ObjectUtils.isEmpty(userId)) {
                updateErrorDetails(outgoingResponse, Constants.USER_ID_DOESNT_EXIST, HttpStatus.BAD_REQUEST);
                return outgoingResponse;
            }
            String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
            SubmitAssessmentData data = new SubmitAssessmentData();
            //Confirm whether the submitted request sections and questions match.
            String errMsg = validateSubmitAssessmentRequest(submitRequest, userId, data, userAuthToken, editMode);
            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(outgoingResponse, errMsg, HttpStatus.BAD_REQUEST);
                return outgoingResponse;
            }

            errMsg = assessUtilServ.validateAssessmentLanguageAndNodes(submitRequest);

            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(outgoingResponse, errMsg, HttpStatus.BAD_REQUEST);
                return outgoingResponse;
            }

            RetakeCounts retakeCounts = resolveRetakeCounts(data.assessmentHierarchy, userId, assessmentIdFromRequest);
            String scoreCutOffType = resolveScoreCutOffType(data.assessmentHierarchy);
            String courseCategory = resolveCourseCategory(submitRequest);
            SubmitContext ctx = new SubmitContext(submitRequest, userId, userAuthToken, editMode,
                    assessmentIdFromRequest, courseCategory, scoreCutOffType);

            List<Map<String, Object>> sectionLevelsResults = new ArrayList<>();
            if (scoreAllSections(ctx, data, outgoingResponse, sectionLevelsResults)) {
                return outgoingResponse;
            }
            if (Constants.SECTION_LEVEL_SCORE_CUTOFF.equalsIgnoreCase(scoreCutOffType)) {
                finalizeSectionLevelResults(ctx, data, outgoingResponse, sectionLevelsResults, retakeCounts,
                        assessmentCompletionTime);
                return outgoingResponse;
            }

        } catch (Exception e) {
            String errMsg = String.format("Failed to process assessment submit request. Exception: %s", e.getMessage());
            logger.error(errMsg, e);
            updateErrorDetails(outgoingResponse, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
            assessUtilServ.publishFailedAssessmentAuditEvent((String) submitRequest.get(Constants.USER_ID),
                    (String) submitRequest.get(Constants.IDENTIFIER), submitRequest, errMsg, auditMethodName, outgoingResponse.getResult());
        }
        return outgoingResponse;
    }

    /**
     * Mutable holder for the four out-parameters that {@link #validateSubmitAssessmentRequest}
     * populates, so the submit helpers can be passed one object instead of four collections.
     */
    private static final class SubmitAssessmentData {
        private final Map<String, Object> assessmentHierarchy = new HashMap<>();
        private final List<Map<String, Object>> hierarchySectionList = new ArrayList<>();
        private final List<Map<String, Object>> sectionListFromSubmitRequest = new ArrayList<>();
        private final Map<String, Object> existingAssessmentData = new HashMap<>();
    }

    /** Request-scoped inputs shared by the submit-assessment helpers. */
    private record SubmitContext(Map<String, Object> submitRequest, String userId, String userAuthToken,
                                 boolean editMode, String assessmentIdFromRequest, String courseCategory,
                                 String scoreCutOffType) {
    }

    /** Retake allowance and consumption for the current user / assessment pair. */
    private record RetakeCounts(int maxAssessmentRetakeAttempts, int retakeAttemptsConsumed) {
    }

    private RetakeCounts resolveRetakeCounts(Map<String, Object> assessmentHierarchy, String userId,
                                             String assessmentIdFromRequest) {
        Object contextCategory = assessmentHierarchy.get(Constants.CONTEXT_CATEGORY_TAG);
        if (contextCategory != null && Constants.PRE_ENROLLED_ASSESSMENT_KEY.equals(contextCategory.toString())) {
            return new RetakeCounts(1, 0);
        }
        return new RetakeCounts((Integer) assessmentHierarchy.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS),
                calculateAssessmentRetakeCount(userId, assessmentIdFromRequest));
    }

    private String resolveScoreCutOffType(Map<String, Object> assessmentHierarchy) {
        String assessmentType = ((String) assessmentHierarchy.get(Constants.ASSESSMENT_TYPE)).toLowerCase();
        return assessmentType.equalsIgnoreCase(Constants.QUESTION_WEIGHTAGE)
                ? Constants.SECTION_LEVEL_SCORE_CUTOFF
                : Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF;
    }

    private String resolveCourseCategory(Map<String, Object> submitRequest) {
        Map<String, Object> courseCategoryMap = contentService
                .readContent((String) submitRequest.get(Constants.COURSE_ID));
        if (MapUtils.isNotEmpty(courseCategoryMap)) {
            return (String) courseCategoryMap.get(Constants.COURSE_CATEGORY);
        }
        return "";
    }

    private Map<String, Object> findUserSectionData(List<Map<String, Object>> sectionListFromSubmitRequest,
                                                    String hierarchySectionId) {
        for (Map<String, Object> sectionFromSubmitRequest : sectionListFromSubmitRequest) {
            String userSectionId = (String) sectionFromSubmitRequest.get(Constants.IDENTIFIER);
            if (userSectionId.equalsIgnoreCase(hierarchySectionId)) {
                return sectionFromSubmitRequest;
            }
        }
        return new HashMap<>();
    }

    /**
     * Scores every hierarchy section. Returns {@code true} when the assessment-level cut-off has
     * already produced the final response, which is where the original code returned early.
     */
    private boolean scoreAllSections(SubmitContext ctx, SubmitAssessmentData data, SBApiResponse outgoingResponse,
                                     List<Map<String, Object>> sectionLevelsResults) throws IOException {
        for (Map<String, Object> hierarchySection : data.hierarchySectionList) {
            String hierarchySectionId = (String) hierarchySection.get(Constants.IDENTIFIER);
            Map<String, Object> userSectionData = findUserSectionData(data.sectionListFromSubmitRequest,
                    hierarchySectionId);

            hierarchySection.put(Constants.SCORE_CUTOFF_TYPE, ctx.scoreCutOffType());
            List<Map<String, Object>> questionsListFromSubmitRequest = new ArrayList<>();
            if (userSectionData.containsKey(Constants.CHILDREN)
                    && !ObjectUtils.isEmpty(userSectionData.get(Constants.CHILDREN))) {
                questionsListFromSubmitRequest = (List<Map<String, Object>>) userSectionData
                        .get(Constants.CHILDREN);
            }
            List<String> desiredKeys = List.of(Constants.IDENTIFIER);
            List<Object> questionsList = questionsListFromSubmitRequest.stream()
                    .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).toList();
            List<String> questionsListFromAssessmentHierarchy = questionsList.stream()
                    .map(object -> Objects.toString(object, null)).toList();
            Map<String, Object> result = new HashMap<>();
            Map<String, Object> questionSetDetailsMap = getParamDetailsForQTypes(hierarchySection,
                    data.assessmentHierarchy, hierarchySectionId);
            switch (ctx.scoreCutOffType()) {
                case Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF: {
                    result.putAll(scoreSection(ctx, data, hierarchySection, questionSetDetailsMap,
                            questionsListFromAssessmentHierarchy, questionsListFromSubmitRequest));
                    finalizeAssessmentLevelResults(ctx, data, outgoingResponse, result);
                    return true;
                }
                case Constants.SECTION_LEVEL_SCORE_CUTOFF: {
                    result.putAll(scoreSection(ctx, data, hierarchySection, questionSetDetailsMap,
                            questionsListFromAssessmentHierarchy, questionsListFromSubmitRequest));
                    sectionLevelsResults.add(result);
                }
                    break;
                default:
                    break;
            }
        }
        return false;
    }

    private Map<String, Object> scoreSection(SubmitContext ctx, SubmitAssessmentData data,
                                             Map<String, Object> hierarchySection,
                                             Map<String, Object> questionSetDetailsMap,
                                             List<String> questionsListFromAssessmentHierarchy,
                                             List<Map<String, Object>> questionsListFromSubmitRequest)
            throws IOException {
        return createResponseMapWithProperStructure(hierarchySection,
                assessUtilServ.validateQumlAssessmentV3(questionSetDetailsMap, questionsListFromAssessmentHierarchy,
                        questionsListFromSubmitRequest,
                        assessUtilServ.readQListfromCache(questionsListFromAssessmentHierarchy,
                                ctx.assessmentIdFromRequest(), ctx.editMode(), ctx.userAuthToken())),
                (Integer) data.assessmentHierarchy.get(Constants.MINIMUM_PASS_PERCENTAGE));
    }

    private void finalizeAssessmentLevelResults(SubmitContext ctx, SubmitAssessmentData data,
                                                SBApiResponse outgoingResponse, Map<String, Object> result)
            throws JsonProcessingException {
        Map<String, Object> finalRes = calculateAssessmentFinalResults(result);
        outgoingResponse.getResult().putAll(finalRes);
        outgoingResponse.getResult().put(Constants.PRIMARY_CATEGORY,
                data.assessmentHierarchy.get(Constants.PRIMARY_CATEGORY));
        persistResultOrUpdatePracticeProgress(ctx, data, finalRes);
    }

    private void finalizeSectionLevelResults(SubmitContext ctx, SubmitAssessmentData data,
                                             SBApiResponse outgoingResponse,
                                             List<Map<String, Object>> sectionLevelsResults,
                                             RetakeCounts retakeCounts, long assessmentCompletionTime)
            throws JsonProcessingException {
        long assessmentStartTime = 0;
        if (data.existingAssessmentData.get(Constants.START_TIME) != null) {
            assessmentStartTime = assessUtilServ
                    .parseStartTimeToLong(data.existingAssessmentData.get(Constants.START_TIME));
        }
        Map<String, Object> result = calculateSectionFinalResults(sectionLevelsResults, assessmentStartTime,
                assessmentCompletionTime, retakeCounts.maxAssessmentRetakeAttempts(),
                retakeCounts.retakeAttemptsConsumed());
        outgoingResponse.getResult().putAll(result);
        outgoingResponse.getParams().setStatus(Constants.SUCCESS);
        outgoingResponse.setResponseCode(HttpStatus.OK);
        outgoingResponse.getResult().put(Constants.PRIMARY_CATEGORY,
                data.assessmentHierarchy.get(Constants.PRIMARY_CATEGORY));
        persistResultOrUpdatePracticeProgress(ctx, data, result);
    }

    /**
     * Persists a real attempt and raises the Kafka event, or, for a practice question set,
     * only pushes the content progress update.
     */
    private void persistResultOrUpdatePracticeProgress(SubmitContext ctx, SubmitAssessmentData data,
                                                       Map<String, Object> result) throws JsonProcessingException {
        String assessmentPrimaryCategory = (String) data.assessmentHierarchy.get(Constants.PRIMARY_CATEGORY);
        if (!Constants.PRACTICE_QUESTION_SET.equalsIgnoreCase(assessmentPrimaryCategory) && !ctx.editMode()) {
            String questionSetFromAssessmentString = (String) data.existingAssessmentData
                    .get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
            Map<String, Object> questionSetFromAssessment = null;
            if (StringUtils.isNotBlank(questionSetFromAssessmentString)) {
                questionSetFromAssessment = mapper.readValue(questionSetFromAssessmentString,
                        new TypeReference<Map<String, Object>>() {
                        });
            }
            writeDataToDatabaseAndTriggerKafkaEvent(new KafkaEventContext(ctx.submitRequest(), ctx.userId(),
                            ctx.userAuthToken(), assessmentPrimaryCategory, ctx.courseCategory(),
                            (String) data.assessmentHierarchy.get(Constants.CONTEXT_CATEGORY_TAG)),
                    questionSetFromAssessment, result);
        } else if (Constants.PRACTICE_QUESTION_SET.equalsIgnoreCase(assessmentPrimaryCategory)) {
            SBApiResponse contentUpdateResponse = new SBApiResponse();
            String response = contentService.updateContentProgress(ctx.userAuthToken(), ctx.submitRequest(),
                    ctx.userId(), contentUpdateResponse);
            if (!Constants.SUCCESS.equalsIgnoreCase(response)) {
                logger.error(ERR_PRACTICE_PROGRESS_UPDATE);
            }
        }
    }

    private int calculateAssessmentRetakeCount(String userId, String assessmentId) {
        List<Map<String, Object>> userAssessmentDataList = assessUtilServ.readUserSubmittedAssessmentRecords(userId,
                assessmentId);
        return (int) userAssessmentDataList.stream()
                .filter(userData -> userData.containsKey(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY)
                        && null != userData.get(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY))
                .count();
    }

    private Instant calculateAssessmentSubmitTime(int expectedDurationInSeconds, Instant assessmentStartTime,
                                                  int bufferTimeInSeconds) {
        int totalDurationInSeconds = expectedDurationInSeconds;

        if (bufferTimeInSeconds > 0) {
            totalDurationInSeconds += Integer.parseInt(serverProperties.getUserAssessmentSubmissionDuration());
        }

        return assessmentStartTime.plusSeconds(totalDurationInSeconds);
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
        String assessmentType = (String) assessmentAllDetail.get(Constants.ASSESSMENT_TYPE);
        for (Map<String, Object> section : sections) {
            sectionIdList.add((String) section.get(Constants.IDENTIFIER));
            Map<String, Object> newSection = new HashMap<>();
            for (String sectionParam : sectionParams) {
                if (section.containsKey(sectionParam)) {
                    newSection.put(sectionParam, section.get(sectionParam));
                }
            }
            List<Map<String, Object>> questions = (List<Map<String, Object>>) section.get(Constants.CHILDREN);
            List<String> childNodeList;
            if (assessmentType.equalsIgnoreCase(Constants.QUESTION_WEIGHTAGE)) {
                List<Map<String, Object>> selectedQuestionsList = processRandomizationForQuestions((Map<String, Map<String, Object>>) section.get(Constants.SECTION_LEVEL_DEFINITION), questions);
                childNodeList = selectedQuestionsList.stream()
                        .map(question -> (String) question.get(Constants.IDENTIFIER))
                        .collect(toList());
            } else {
                int maxQuestions = (int) section.getOrDefault(Constants.MAX_QUESTIONS, questions.size());
                List<Map<String, Object>> shuffledQuestionsList = shuffleQuestions(questions);
                childNodeList = shuffledQuestionsList.stream()
                        .map(question -> (String) question.get(Constants.IDENTIFIER))
                        .limit(maxQuestions)
                        .collect(toList());
            }
            Collections.shuffle(childNodeList);
            newSection.put(Constants.CHILD_NODES, childNodeList);
            sectionResponse.add(newSection);
        }
        assessmentFilteredDetail.put(Constants.CHILDREN, sectionResponse);
        assessmentFilteredDetail.put(Constants.CHILD_NODES, sectionIdList);
    }

    private Map<String, String> validateQuestionListAPI(Map<String, Object> requestBody, String authUserToken,
            List<String> identifierList,boolean editMode) throws IOException {
        Map<String, String> result = new HashMap<>();
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
        if (StringUtils.isBlank(userId)) {
            result.put(Constants.ERROR_MESSAGE, Constants.USER_ID_DOESNT_EXIST);
            return result;
        }
        String assessmentIdFromRequest = (String) requestBody.get(Constants.ASSESSMENT_ID_KEY);
        if (StringUtils.isBlank(assessmentIdFromRequest)) {
            result.put(Constants.ERROR_MESSAGE, Constants.ASSESSMENT_ID_KEY_IS_NOT_PRESENT_IS_EMPTY);
            return result;
        }
        identifierList.addAll(getQuestionIdList(requestBody));
        if (identifierList.isEmpty()) {
            result.put(Constants.ERROR_MESSAGE, Constants.IDENTIFIER_LIST_IS_EMPTY);
            return result;
        }

        Map<String, Object> assessmentAllDetail = assessUtilServ
                .readAssessmentHierarchyFromCache(assessmentIdFromRequest,editMode,authUserToken);

        if (MapUtils.isEmpty(assessmentAllDetail)) {
            result.put(Constants.ERROR_MESSAGE, Constants.ASSESSMENT_HIERARCHY_READ_FAILED);
            return result;
        }
        result.put(Constants.SHUFFLE, String.valueOf(getShuffleFlagFromHierarchy(assessmentAllDetail)));
        String primaryCategory = (String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY);
        if (Constants.PRACTICE_QUESTION_SET
                .equalsIgnoreCase(primaryCategory)||editMode) {
            result.put(Constants.PRIMARY_CATEGORY, primaryCategory);
            result.put(Constants.ERROR_MESSAGE, StringUtils.EMPTY);
            return result;
        }

        Map<String, Object> userAssessmentAllDetail = new HashMap<>();

        List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                userId, assessmentIdFromRequest);
        String questionSetFromAssessmentString = (!existingDataList.isEmpty())
                ? (String) existingDataList.get(0).get(Constants.ASSESSMENT_READ_RESPONSE_KEY)
                : "";
        if (StringUtils.isNotBlank(questionSetFromAssessmentString)) {
            userAssessmentAllDetail.putAll(mapper.readValue(questionSetFromAssessmentString,
                    new TypeReference<Map<String, Object>>() {
                    }));
        } else {
            result.put(Constants.ERROR_MESSAGE, Constants.USER_ASSESSMENT_DATA_NOT_PRESENT);
            return result;
        }

        applyQuestionIdMatch(result, userAssessmentAllDetail, identifierList);
        return result;
    }

    /**
     * Records whether every requested question id belongs to the user's latest assessment,
     * or flags the assessment id as invalid when there is no user assessment data.
     */
    private void applyQuestionIdMatch(Map<String, String> result, Map<String, Object> userAssessmentAllDetail,
                                      List<String> identifierList) {
        if (MapUtils.isEmpty(userAssessmentAllDetail)) {
            result.put(Constants.ERROR_MESSAGE, Constants.ASSESSMENT_ID_INVALID);
            return;
        }
        result.put(Constants.PRIMARY_CATEGORY, (String) userAssessmentAllDetail.get(Constants.PRIMARY_CATEGORY));
        List<String> questionsFromAssessment = new ArrayList<>();
        List<Map<String, Object>> sections = (List<Map<String, Object>>) userAssessmentAllDetail
                .get(Constants.CHILDREN);
        for (Map<String, Object> section : sections) {
            // Out of the list of questions received in the payload, checking if the request
            // has only those ids which are a part of the user's latest assessment
            // Fetching all the remaining questions details from the Redis
            questionsFromAssessment.addAll((List<String>) section.get(Constants.CHILD_NODES));
        }
        result.put(Constants.ERROR_MESSAGE, validateQuestionListRequest(identifierList, questionsFromAssessment)
                ? StringUtils.EMPTY
                : Constants.THE_QUESTIONS_IDS_PROVIDED_DONT_MATCH);
    }

    private List<String> getQuestionIdList(Map<String, Object> questionListRequest) {
        try {
            if (questionListRequest.containsKey(Constants.REQUEST)) {
                Map<String, Object> request = (Map<String, Object>) questionListRequest.get(Constants.REQUEST);
                if ((!ObjectUtils.isEmpty(request)) && request.containsKey(Constants.SEARCH)) {
                    Map<String, Object> searchObj = (Map<String, Object>) request.get(Constants.SEARCH);
                    if (!ObjectUtils.isEmpty(searchObj) && searchObj.containsKey(Constants.IDENTIFIER)
                            && !CollectionUtils.isEmpty((List<String>) searchObj.get(Constants.IDENTIFIER))) {
                        return (List<String>) searchObj.get(Constants.IDENTIFIER);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(String.format("Failed to process the questionList request body. %s", e.getMessage()));
        }
        return Collections.emptyList();
    }

    private boolean validateQuestionListRequest(List<String> identifierList, List<String> questionsFromAssessment) {
        return (new HashSet<>(questionsFromAssessment).containsAll(identifierList)) ? Boolean.TRUE : Boolean.FALSE;
    }

    private String validateSubmitAssessmentRequest(Map<String, Object> submitRequest, String userId,
            SubmitAssessmentData data, String token, boolean editMode) throws IOException {
        submitRequest.put(Constants.USER_ID, userId);
        if (StringUtils.isEmpty((String) submitRequest.get(Constants.IDENTIFIER))) {
            return Constants.INVALID_ASSESSMENT_ID;
        }
        String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
        data.assessmentHierarchy.putAll(assessUtilServ.readAssessmentHierarchyFromCache(assessmentIdFromRequest,editMode,token));
        if (MapUtils.isEmpty(data.assessmentHierarchy)) {
            return Constants.READ_ASSESSMENT_FAILED;
        }

        data.hierarchySectionList.addAll((List<Map<String, Object>>) data.assessmentHierarchy.get(Constants.CHILDREN));
        data.sectionListFromSubmitRequest.addAll((List<Map<String, Object>>) submitRequest.get(Constants.CHILDREN));
        if (((String) (data.assessmentHierarchy.get(Constants.PRIMARY_CATEGORY)))
                .equalsIgnoreCase(Constants.PRACTICE_QUESTION_SET) || editMode)
            return "";

        List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                userId, (String) submitRequest.get(Constants.IDENTIFIER));
        if (existingDataList.isEmpty()) {
            return Constants.USER_ASSESSMENT_DATA_NOT_PRESENT;
        }
        data.existingAssessmentData.putAll(existingDataList.get(0));

        Date assessmentStartTime = resolveAssessmentStartTime(
                data.existingAssessmentData.get(Constants.START_TIME));
        if (assessmentStartTime == null) {
            return Constants.READ_ASSESSMENT_START_TIME_FAILED;
        }
        int expectedDuration = (Integer) data.assessmentHierarchy.get(Constants.EXPECTED_DURATION);
        Instant later = calculateAssessmentSubmitTime(expectedDuration,
                assessmentStartTime.toInstant(),
                Integer.parseInt(serverProperties.getUserAssessmentSubmissionDuration()));
        Instant submissionTime = Instant.now();
        if (submissionTime.compareTo(later) > 0) {
            return Constants.ASSESSMENT_SUBMIT_EXPIRED;
        }

        List<String> desiredKeys = List.of(Constants.IDENTIFIER);
        List<Object> hierarchySectionIds = data.hierarchySectionList.stream()
                .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).toList();
        List<Object> submitSectionIds = data.sectionListFromSubmitRequest.stream()
                .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).toList();
        if (!new HashSet<>(hierarchySectionIds).containsAll(submitSectionIds)) {
            return Constants.WRONG_SECTION_DETAILS;
        }
        return validateIfQuestionIdsAreSame(data.sectionListFromSubmitRequest, desiredKeys,
                data.existingAssessmentData);
    }

    /**
     * The stored assessment start time, which may come back as an {@link Instant}, a {@link Date}
     * or an ISO-8601 string.
     *
     * @return the start time, or null when the stored value is absent or none of those types.
     */
    private Date resolveAssessmentStartTime(Object startTimeObj) {
        if (startTimeObj instanceof Instant instant) {
            return Date.from(instant);
        }
        if (startTimeObj instanceof Date date) {
            return date;
        }
        if (startTimeObj instanceof String str) {
            return Date.from(Instant.parse(str));
        }
        return null;
    }

    private String validateIfQuestionIdsAreSame(List<Map<String, Object>> sectionListFromSubmitRequest,
            List<String> desiredKeys, Map<String, Object> existingAssessmentData) throws IOException {
        String questionSetFromAssessmentString = (String) existingAssessmentData
                .get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
        if (StringUtils.isBlank(questionSetFromAssessmentString)) {
            return Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED;
        }
        Map<String, Object> questionSetFromAssessment = mapper.readValue(questionSetFromAssessmentString,
                new TypeReference<Map<String, Object>>() {
                });
        if (questionSetFromAssessment == null || questionSetFromAssessment.get(Constants.CHILDREN) == null) {
            return "";
        }
        List<Object> questionIdsFromAssessmentHierarchy = collectHierarchyQuestionIds(questionSetFromAssessment);
        List<Object> userQuestionIdsFromSubmitRequest =
                collectSubmittedQuestionIds(sectionListFromSubmitRequest, desiredKeys);
        if (!new HashSet<>(questionIdsFromAssessmentHierarchy).containsAll(userQuestionIdsFromSubmitRequest)) {
            return Constants.ASSESSMENT_SUBMIT_INVALID_QUESTION;
        }
        return "";
    }

    /** Flattens the child-node ids declared by every section of the stored question set. */
    private List<Object> collectHierarchyQuestionIds(Map<String, Object> questionSetFromAssessment) {
        List<Map<String, Object>> sections = (List<Map<String, Object>>) questionSetFromAssessment
                .get(Constants.CHILDREN);
        List<String> desiredKey = List.of(Constants.CHILD_NODES);
        List<Object> questionList = sections.stream()
                .flatMap(x -> desiredKey.stream().filter(x::containsKey).map(x::get)).toList();
        List<Object> questionIdsFromAssessmentHierarchy = new ArrayList<>();
        for (Object question : questionList) {
            questionIdsFromAssessmentHierarchy.addAll((List<String>) question);
        }
        return questionIdsFromAssessmentHierarchy;
    }

    /** Flattens the question ids the user actually submitted, across every section of the request. */
    private List<Object> collectSubmittedQuestionIds(List<Map<String, Object>> sectionListFromSubmitRequest,
            List<String> desiredKeys) {
        List<Map<String, Object>> questionsListFromSubmitRequest = new ArrayList<>();
        for (Map<String, Object> userSectionData : sectionListFromSubmitRequest) {
            if (userSectionData.containsKey(Constants.CHILDREN)
                    && !ObjectUtils.isEmpty(userSectionData.get(Constants.CHILDREN))) {
                questionsListFromSubmitRequest
                        .addAll((List<Map<String, Object>>) userSectionData.get(Constants.CHILDREN));
            }
        }
        return questionsListFromSubmitRequest.stream()
                .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get))
                .toList();
    }

    public Map<String, Object> createResponseMapWithProperStructure(Map<String, Object> hierarchySection,
                                                                    Map<String, Object> resultMap, Integer assessmentMinimumPassPercentage) throws ApplicationLogicError {
        Map<String, Object> sectionLevelResult = new HashMap<>();
        sectionLevelResult.put(Constants.IDENTIFIER, hierarchySection.get(Constants.IDENTIFIER));
        sectionLevelResult.put(Constants.OBJECT_TYPE, hierarchySection.get(Constants.OBJECT_TYPE));
        sectionLevelResult.put(Constants.PRIMARY_CATEGORY, hierarchySection.get(Constants.PRIMARY_CATEGORY));
        if (assessmentMinimumPassPercentage == null) {
            assessmentMinimumPassPercentage = 0;
        }
        // Use section's minimumPassPercentage if it exists and is not 0, otherwise use assessment's
        Integer finalMinimumPassPercentage = Optional.ofNullable((Integer) hierarchySection.get(Constants.MINIMUM_PASS_PERCENTAGE))
                .filter(percentage -> percentage > 0)
                .orElse(assessmentMinimumPassPercentage);
        sectionLevelResult.put(Constants.PASS_PERCENTAGE, finalMinimumPassPercentage);
        sectionLevelResult.put(Constants.NAME, hierarchySection.get(Constants.NAME));
        Double result;
        if (!ObjectUtils.isEmpty(resultMap)) {
            result = (Double) resultMap.get(Constants.RESULT);
            sectionLevelResult.put(Constants.RESULT, result);
            sectionLevelResult.put(Constants.BLANK, resultMap.get(Constants.BLANK));
            sectionLevelResult.put(Constants.CORRECT, resultMap.get(Constants.CORRECT));
            sectionLevelResult.put(Constants.INCORRECT, resultMap.get(Constants.INCORRECT));
            sectionLevelResult.put(Constants.CHILDREN,resultMap.get(Constants.CHILDREN));
            sectionLevelResult.put(Constants.SECTION_RESULT,resultMap.get(Constants.SECTION_RESULT));
            sectionLevelResult.put(Constants.TOTAL_MARKS,resultMap.get(Constants.TOTAL_MARKS));
            sectionLevelResult.put(Constants.SECTION_MARKS,resultMap.get(Constants.SECTION_MARKS));


        } else {
            result = 0.0;
            sectionLevelResult.put(Constants.RESULT, result);
            List<String> childNodes = (List<String>) hierarchySection.get(Constants.CHILDREN);
            sectionLevelResult.put(Constants.TOTAL, childNodes.size());
            sectionLevelResult.put(Constants.BLANK, childNodes.size());
            sectionLevelResult.put(Constants.CORRECT, 0);
            sectionLevelResult.put(Constants.INCORRECT, 0);
        }
        sectionLevelResult.put(Constants.PASS,result >= finalMinimumPassPercentage);
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
            res.put(Constants.NAME, assessmentLevelResult.get(Constants.NAME));
            Integer minimumPassPercentage = (Integer) assessmentLevelResult.get(Constants.PASS_PERCENTAGE);
            res.put(Constants.PASS, result >= minimumPassPercentage);
        } catch (Exception e) {
            logger.error("Failed to calculate Assessment final results. Exception: ", e);
        }
        return res;
    }

    /**
     * Inputs for the assessment-submit Kafka event, grouped so that
     * {@link #writeDataToDatabaseAndTriggerKafkaEvent} stays within the parameter limit.
     */
    private record KafkaEventContext(Map<String, Object> submitRequest, String userId, String userAuthToken,
                                     String primaryCategory, String courseCategory, String contextCategory) {
    }

    private void writeDataToDatabaseAndTriggerKafkaEvent(KafkaEventContext eventContext,
                                                         Map<String, Object> questionSetFromAssessment,
                                                         Map<String, Object> result) throws ApplicationLogicError {
        Map<String, Object> submitRequest = eventContext.submitRequest();
        String contextCategory = eventContext.contextCategory();
        try {
            if (questionSetFromAssessment == null || questionSetFromAssessment.get(Constants.START_TIME) == null) {
                logger.error("AssessmentServiceV5Impl : writeDataToDatabaseAndTriggerKafkaEvent : "
                        + "questionSetFromAssessment is null or missing start time, skipping DB write and Kafka event");
                return;
            }
            Instant startTime = assessUtilServ.parseStartTimeToInstant(questionSetFromAssessment.get(Constants.START_TIME));
            Boolean isAssessmentUpdatedToDB = assessmentRepository.updateUserAssesmentDataToDB(eventContext.userId(),
                    (String) submitRequest.get(Constants.IDENTIFIER), submitRequest, result, Constants.SUBMITTED,
                    startTime,null);
            //If the assessment is of the type of Standalone assessment it should be mandatory to pass to generate the certificate and updateContentProgess
            if (!Boolean.TRUE.equals(isAssessmentUpdatedToDB)
                    || !proceedWithContentUpdate(contextCategory, eventContext.courseCategory(),
                            (boolean) result.get(Constants.PASS))) {
                return;
            }
            updateContentProgressForContext(eventContext);
            kafkaProducer.push(serverProperties.getAssessmentSubmitTopic(), buildSubmitEvent(eventContext, result));
        } catch (Exception e) {
            logger.error("Failed to write data for assessment submit response. Exception: ", e);
        }
    }

    /** Routes the progress update to the pre-enrolled endpoint or the standard content endpoint. */
    private void updateContentProgressForContext(KafkaEventContext eventContext) {
        SBApiResponse contentUpdateResponse = new SBApiResponse();
        String contextCategory = eventContext.contextCategory();
        if (StringUtils.isNotBlank(contextCategory)
                && contextCategory.equalsIgnoreCase(Constants.PRE_ENROLLED_ASSESSMENT_KEY)) {
            contentService.updatePreEnrolledAssessment(eventContext.userAuthToken(), eventContext.submitRequest(),
                    eventContext.userId(), contentUpdateResponse);
        } else {
            contentService.updateContentProgress(eventContext.userAuthToken(), eventContext.submitRequest(),
                    eventContext.userId(), contentUpdateResponse);
        }
    }

    /** Builds the assessment-submit payload published to Kafka. */
    private Map<String, Object> buildSubmitEvent(KafkaEventContext eventContext, Map<String, Object> result)
            throws JsonProcessingException {
        Map<String, Object> submitRequest = eventContext.submitRequest();
        String primaryCategory = eventContext.primaryCategory();
        Map<String, Object> kafkaResult = new HashMap<>();
        kafkaResult.put(Constants.CONTENT_ID_KEY, submitRequest.get(Constants.IDENTIFIER));
        kafkaResult.put(Constants.COURSE_ID,
                submitRequest.get(Constants.COURSE_ID) != null ? submitRequest.get(Constants.COURSE_ID)
                        : "");
        kafkaResult.put(Constants.BATCH_ID,
                submitRequest.get(Constants.BATCH_ID) != null ? submitRequest.get(Constants.BATCH_ID) : "");
        kafkaResult.put(Constants.USER_ID, submitRequest.get(Constants.USER_ID));
        kafkaResult.put(Constants.ASSESSMENT_ID_KEY, submitRequest.get(Constants.IDENTIFIER));
        kafkaResult.put(Constants.PRIMARY_CATEGORY, primaryCategory);
        kafkaResult.put(Constants.TOTAL_SCORE, result.get(Constants.OVERALL_RESULT));
        if (("Competency Assessment".equalsIgnoreCase(primaryCategory)
                && submitRequest.containsKey(Constants.COMPETENCIES_V3)
                && submitRequest.get(Constants.COMPETENCIES_V3) != null)) {
            ObjectMapper objectMapper = new ObjectMapper(); //Updated for Json Import
            List<Map<String, Object>> competencyList = objectMapper.readValue(
                    (String) submitRequest.get(Constants.COMPETENCIES_V3),
                    new TypeReference<List<Map<String, Object>>>() {
                    }
            );
            // First competency map, or an empty marker when none were submitted
            kafkaResult.put(Constants.COMPETENCY, competencyList.isEmpty() ? "" : competencyList.get(0));
        }
        return kafkaResult;
    }

    private Map<String, Object> calculateSectionFinalResults(List<Map<String, Object>> sectionLevelResults, long assessmentStartTime, long assessmentCompletionTime, int maxAssessmentRetakeAttempts, int retakeAttemptsConsumed)
            throws ApplicationLogicError {
        Map<String, Object> res = new HashMap<>();
        Double result;
        Integer correct = 0;
        Integer blank = 0;
        Integer inCorrect = 0;
        Double totalSectionMarks = 0.0;
        Integer totalMarks = 0;
        int pass = 0;
        try {
            for (Map<String, Object> sectionChildren : sectionLevelResults) {
                res.put(Constants.CHILDREN, sectionLevelResults);
                result = (Double) sectionChildren.get(Constants.RESULT);
                blank += (Integer) sectionChildren.get(Constants.BLANK);
                correct += (Integer) sectionChildren.get(Constants.CORRECT);
                inCorrect += (Integer) sectionChildren.get(Constants.INCORRECT);
                Integer minimumPassPercentage = (Integer) sectionChildren.get(Constants.PASS_PERCENTAGE);
                if (result >= minimumPassPercentage) {
                    pass++;
                }
                if(sectionChildren.get(Constants.SECTION_MARKS)!=null){
                    totalSectionMarks += (Double) sectionChildren.get(Constants.SECTION_MARKS);
                }
                if(sectionChildren.get(Constants.TOTAL_MARKS)!=null){
                    totalMarks += (Integer) sectionChildren.get(Constants.TOTAL_MARKS);
                }
            }
            if (correct > 0 && inCorrect > 0) {
                res.put(Constants.OVERALL_RESULT, ((double) correct / (double) (correct + inCorrect)) * 100);
            } else {
                res.put(Constants.OVERALL_RESULT, 0);
            }
            res.put(Constants.BLANK, blank);
            res.put(Constants.CORRECT, correct);
            res.put(Constants.INCORRECT, inCorrect);
            res.put(Constants.PASS, (pass == sectionLevelResults.size()));
            res.put(Constants.TIME_TAKEN_FOR_ASSESSMENT,assessmentCompletionTime-assessmentStartTime);
            res.put(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS,maxAssessmentRetakeAttempts);
            res.put(Constants.RETAKE_ATTEMPT_CONSUMED,retakeAttemptsConsumed);
            double totalPercentage = (totalSectionMarks / (double)totalMarks) * 100;
            res.put(Constants.TOTAL_PERCENTAGE, totalPercentage);
            res.put(Constants.TOTAL_SECTION_MARKS, totalSectionMarks);
            res.put(Constants.TOTAL_MARKS, totalMarks);
        } catch (Exception e) {
            logger.error("Failed to calculate assessment score. Exception: ", e);
        }
        return res;
    }

    private String validateAssessmentReadResult(Map<String, Object> request) {
        String errMsg = "";
        if (MapUtils.isEmpty(request) || !request.containsKey(Constants.REQUEST)) {
            return Constants.INVALID_REQUEST;
        }

        Map<String, Object> requestBody = (Map<String, Object>) request.get(Constants.REQUEST);
        if (MapUtils.isEmpty(requestBody)) {
            return Constants.INVALID_REQUEST;
        }
        List<String> missingAttribs = new ArrayList<>();
        if (!requestBody.containsKey(Constants.ASSESSMENT_ID_KEY)
                || StringUtils.isBlank((String) requestBody.get(Constants.ASSESSMENT_ID_KEY))) {
            missingAttribs.add(Constants.ASSESSMENT_ID_KEY);
        }

        if (!requestBody.containsKey(Constants.COURSE_ID)
                || StringUtils.isBlank((String) requestBody.get(Constants.COURSE_ID))) {
            missingAttribs.add(Constants.COURSE_ID);
        }

        if (!missingAttribs.isEmpty()) {
            errMsg = "One or more mandatory fields are missing in Request. Mandatory fields are : "
                    + missingAttribs.toString();
        }

        return errMsg;
    }


    /**
     * Generates a map containing marks for each question.
     * The input is a map where each key is a section name, and the value is another map.
     * This inner map has proficiency keys, and each proficiency key maps to a map containing various attributes including "marksForQuestion".
     * The output map's keys are of the format "sectionKey|proficiencyKey" and values are the corresponding marks for that question.
     *
     * @param qSectionSchemeMap a map representing sections and their respective proficiency maps
     * @return a map where each key is a combination of section and proficiency, and each value is the marks for that question
     */
    public Map<String, Integer> generateMarkMap(Map<String, Map<String, Object>> qSectionSchemeMap) {
        Map<String, Integer> markMap = new HashMap<>();
        logger.info("Starting to generate mark map from qSectionSchemeMap");
        qSectionSchemeMap.keySet().forEach(sectionKey -> {
            Map<String, Object> proficiencyMap = qSectionSchemeMap.get(sectionKey);
            proficiencyMap.forEach((key, value) -> {
                if (key.equalsIgnoreCase("marksForQuestion")) {
                    markMap.put(sectionKey, (Integer) value);
                }
            });
        });
        logger.info("Completed generating mark map");
        return markMap;
    }


    /**
     * Retrieves the parameter details for question types based on the given assessment hierarchy.
     *
     * @param assessmentHierarchy a map containing the assessment hierarchy details.
     * @return a map containing the parameter details for the question types.
     * @throws IOException if there is an error processing the question section schema.
     */
    private Map<String, Object> getParamDetailsForQTypes(Map<String, Object> hierarchySection,Map<String, Object> assessmentHierarchy,String hierarchySectionId) throws ApplicationLogicError {
        logger.info("Starting getParamDetailsForQTypes with assessmentHierarchy: {}", assessmentHierarchy);
        Map<String, Object> questionSetDetailsMap = new HashMap<>();
        String assessmentType = (String) assessmentHierarchy.get(Constants.ASSESSMENT_TYPE);
        questionSetDetailsMap.put(Constants.ASSESSMENT_TYPE, assessmentType);
        questionSetDetailsMap.put(Constants.MINIMUM_PASS_PERCENTAGE, assessmentHierarchy.getOrDefault(Constants.MINIMUM_PASS_PERCENTAGE, 0));
        questionSetDetailsMap.put(Constants.TOTAL_MARKS, hierarchySection.get(Constants.TOTAL_MARKS));
        if (assessmentType.equalsIgnoreCase(Constants.QUESTION_WEIGHTAGE)) {
            Map<String,Map<String, Object>> questionSectionSchema= (Map<String,Map<String, Object>>) hierarchySection.get(Constants.SECTION_LEVEL_DEFINITION);
            questionSetDetailsMap.put(Constants.QUESTION_SECTION_SCHEME, generateMarkMap(questionSectionSchema));
            questionSetDetailsMap.put(Constants.NEGATIVE_MARKING_PERCENTAGE, assessmentHierarchy.get(Constants.NEGATIVE_MARKING_PERCENTAGE));
            questionSetDetailsMap.put("hierarchySectionId",hierarchySectionId);
        }
        logger.info("Completed getParamDetailsForQTypes with result: {}", questionSetDetailsMap);
        return questionSetDetailsMap;
    }

    public SBApiResponse saveAssessmentAsync(Map<String, Object> submitRequest, String token,boolean editMode) {
        logger.info("AssessmentServicev5Impl::saveAssessmentAsync... Started");
        SBApiResponse response = createDefaultResponse(Constants.API_READ_ASSESSMENT);
        String assessmentIdentifier = (String) submitRequest.get(Constants.IDENTIFIER);
        String errMsg = "";
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            logger.info("saveAssessmentAsync... UserId: {}, AssessmentIdentifier: {}", userId, assessmentIdentifier);
            Map<String, Object> assessmentAllDetail = null ;
            // Step-1 : Read assessment using assessment Id from the Assessment Service
            if(editMode) {
                assessmentAllDetail = assessUtilServ.fetchHierarchyFromAssessServc(assessmentIdentifier,token);
            }
            else {
                assessmentAllDetail = assessUtilServ
                        .readAssessmentHierarchyFromCache(assessmentIdentifier,editMode,token);
            }
            if (MapUtils.isEmpty(assessmentAllDetail)) {
                updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED,
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            //Step-2 : If Practice Assessment return without saving
            if (Constants.PRACTICE_QUESTION_SET
                    .equalsIgnoreCase((String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY))||editMode) {
                response.getResult().put(Constants.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail));
                return response;
            }
            // Step-3 : If read user submitted assessment
            List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                    userId, assessmentIdentifier);

            //Confirm whether the submitted request sections and questions match.
            if (existingDataList.isEmpty()) {
                updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED,
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            SaveOutcome outcome = resumeSavedAttempt(response, submitRequest, userId, existingDataList, errMsg);
            if (outcome.respondNow()) {
                return response;
            }
            errMsg = outcome.errMsg();
        } catch (Exception e) {
            errMsg = String.format(ERR_READING_ASSESSMENT, e.getMessage());
            logger.error(errMsg, e);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /** Outcome of resuming a saved attempt: the error message, and whether {@code response} is already final. */
    private record SaveOutcome(String errMsg, boolean respondNow) {
    }

    /**
     * Re-opens the user's in-progress attempt: replays the stored question set with the original
     * start time and re-stamps the save point. Fails the request when the attempt has already
     * ended or been submitted.
     */
    private SaveOutcome resumeSavedAttempt(SBApiResponse response, Map<String, Object> submitRequest, String userId,
                                           List<Map<String, Object>> existingDataList, String errMsg) {
        logger.info(ASSESSMENT_READ_USER_HAS_DETAILS);
        Date existingAssessmentStartTime = (Date) (existingDataList.get(0).get(Constants.START_TIME));
        Date existingAssessmentEndTime = (Date) (existingDataList.get(0).get(Constants.END_TIME));
        Timestamp existingAssessmentEndTimeTimestamp = new Timestamp(existingAssessmentEndTime.getTime());
        Timestamp existingAssessmentStarTimeTimestamp = new Timestamp(existingAssessmentStartTime.getTime());
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED,
                    HttpStatus.INTERNAL_SERVER_ERROR);
            return new SaveOutcome(errMsg, true);
        }
        if (existingAssessmentStarTimeTimestamp.compareTo(existingAssessmentEndTimeTimestamp) >= 0
                || !Constants.NOT_SUBMITTED.equalsIgnoreCase((String) existingDataList.get(0).get(Constants.STATUS))) {
            updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED,
                    HttpStatus.INTERNAL_SERVER_ERROR);
            return new SaveOutcome(errMsg, true);
        }
        String questionSetFromAssessmentString = (String) existingDataList.get(0)
                .get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
        Map<String, Object> questionSetFromAssessment = new Gson().fromJson(
                questionSetFromAssessmentString, new TypeToken<HashMap<String, Object>>() {
                }.getType());
        questionSetFromAssessment.put(Constants.START_TIME, existingAssessmentStarTimeTimestamp.getTime());
        questionSetFromAssessment.put(Constants.END_TIME, existingAssessmentStarTimeTimestamp.getTime());
        response.getResult().put(Constants.QUESTION_SET, questionSetFromAssessment);
        Boolean isAssessmentUpdatedToDB = assessmentRepository.updateUserAssesmentDataToDB(userId,
                (String) submitRequest.get(Constants.IDENTIFIER), null, null, null,
                existingAssessmentStarTimeTimestamp.toInstant(), submitRequest);
        if (Boolean.FALSE.equals(isAssessmentUpdatedToDB)) {
            response.getResult().put("ASSESSMENT_UPDATE", false);
            return new SaveOutcome(Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED, false);
        }
        response.getResult().put("ASSESSMENT_UPDATE", true);
        return new SaveOutcome(errMsg, false);
    }

    public SBApiResponse readAssessmentSavePoint(String assessmentIdentifier, String token,boolean editMode) {
        logger.info("AssessmentServicev5Impl::readSaveAssessment... Started");
        SBApiResponse response = createDefaultResponse(Constants.API_READ_ASSESSMENT);
        String errMsg = "";
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            logger.info("ReadSaveAssessment... UserId: {}, AssessmentIdentifier: {}", userId, assessmentIdentifier);
            Map<String, Object> assessmentAllDetail = null ;
            // Step-1 : Read assessment using assessment Id from the Assessment Service
            if(editMode) {
                assessmentAllDetail = assessUtilServ.fetchHierarchyFromAssessServc(assessmentIdentifier,token);
            }
            else {
                assessmentAllDetail = assessUtilServ
                        .readAssessmentHierarchyFromCache(assessmentIdentifier,editMode,token);
            }
            if (MapUtils.isEmpty(assessmentAllDetail)) {
                updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED,
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            //Step-2 : If Practice Assessment return without saving
            if (Constants.PRACTICE_QUESTION_SET
                    .equalsIgnoreCase((String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY))||editMode) {
                response.getResult().put(Constants.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail));
                return response;
            }
            // Step-3 : If read user submitted assessment
            List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(
                    userId, assessmentIdentifier);
            Timestamp assessmentStartTime = new Timestamp(new Date().getTime());
            if (existingDataList.isEmpty()) {
                updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_SAVE_NOT_AVBL,
                        HttpStatus.BAD_REQUEST);
                return response;
            } else {
                logger.info(ASSESSMENT_READ_USER_HAS_DETAILS);
                Date existingAssessmentEndTime = (Date) (existingDataList.get(0)
                        .get(Constants.END_TIME));
                Timestamp existingAssessmentEndTimeTimestamp = new Timestamp(
                        existingAssessmentEndTime.getTime());
                if (assessmentStartTime.compareTo(existingAssessmentEndTimeTimestamp) > 0
                        && Constants.NOT_SUBMITTED.equalsIgnoreCase((String) existingDataList.get(0).get(Constants.STATUS))) {
                    String questionSetFromAssessmentString = (String) existingDataList.get(0)
                            .get(Constants.ASSESSMENT_SAVE_READ_RESPONSE_KEY);
                    Map<String, Object> questionSetFromAssessment = new Gson().fromJson(
                            questionSetFromAssessmentString, new TypeToken<HashMap<String, Object>>() {
                            }.getType());
                    response.getResult().put(Constants.QUESTION_SET, questionSetFromAssessment);
                }
                else {
                    updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_SAVE_NOT_AVBL,
                            HttpStatus.BAD_REQUEST);
                    return response;
                }
            }
        } catch (Exception e) {
            errMsg = String.format(ERR_READING_ASSESSMENT, e.getMessage());
            logger.error(errMsg, e);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }


    /**
     * Process randomization for selecting questions based on section level definitions and limits.
     *
     * @param sectionLevelDefinitionMap Map containing section level definitions with 'noOfQuestions' and 'noOfMaxQuestions'.
     * @param questions                 List of questions to be processed.
     * @return List of selected questions based on randomization and limits.
     */
    private List<Map<String, Object>> processRandomizationForQuestions(Map<String, Map<String, Object>> sectionLevelDefinitionMap, List<Map<String, Object>> questions) {
        List<Map<String, Object>> shuffledQuestionsList = shuffleQuestions(questions);
        List<Map<String, Object>> selectedQuestionsList = new ArrayList<>();
        Map<String, Integer> noOfQuestionsMap = new HashMap<>();
        Map<String, Integer> dupNoOfQuestionsMap = new HashMap<>();     // Duplicate map for tracking selected questions
        boolean result = sectionLevelDefinitionMap.values().stream()
                .anyMatch(proficiencyMap -> {
                    Object maxNoOfQuestionsValue = proficiencyMap.get(Constants.NO_OF_QUESTIONS);
                    if (maxNoOfQuestionsValue instanceof Integer maxNoOfQuestions) {
                        return maxNoOfQuestions > 0;
                    }
                    return false;
                });

        if (!result) {
            return questions;
        } else {
            // Populate noOfQuestionsMap and noOfMaxQuestionsMap from sectionLevelDefinitionMap
            sectionLevelDefinitionMap.forEach((sectionLevelDefinitionKey, proficiencyMap) -> proficiencyMap.forEach((key, value) -> {
                if (key.equalsIgnoreCase(Constants.NO_OF_QUESTIONS)) {
                    noOfQuestionsMap.put(sectionLevelDefinitionKey, (Integer) value);
                    dupNoOfQuestionsMap.put(sectionLevelDefinitionKey,0);
                }
            }));

            // Process each question for randomization and limit checking
            for (Map<String, Object> question : shuffledQuestionsList) {
                String questionLevel = (String) question.get(Constants.QUESTION_LEVEL);
                // Check if adding one more question of this level is within limits
                if (dupNoOfQuestionsMap.getOrDefault(questionLevel, 0) < noOfQuestionsMap.getOrDefault(questionLevel, 0)) {
                    // Add the question to selected list
                    selectedQuestionsList.add(question);
                    // Update dupNoOfQuestionsMap to track the count of selected questions for this level
                    dupNoOfQuestionsMap.put(questionLevel, dupNoOfQuestionsMap.getOrDefault(questionLevel, 0) + 1);
                }
            }
            return selectedQuestionsList;
        }
    }



    /**
     * Shuffles the list of questions maps.
     *
     * @param questions The list of questions maps to be shuffled.
     * @return A new list containing the shuffled questions maps.
     */
    public static List<Map<String, Object>> shuffleQuestions(List<Map<String, Object>> questions) {
        // Create a copy of the original list to avoid modifying the input list
        List<Map<String, Object>> shuffledQnsList = new ArrayList<>(questions);
        // Shuffle the list using Collections.shuffle()
        Collections.shuffle(shuffledQnsList);
        return shuffledQnsList;
    }

    @Override
    public SBApiResponse autoPublish(String assessmentIdentifier, String token) {
        SBApiResponse response = createDefaultResponse(Constants.QUESTION_SET_AUTO_PUBLISH);
        if (StringUtils.isBlank(assessmentIdentifier)) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setErrmsg(Constants.INVALID_ASSESSMENT_ID);
            response.getParams().setStatus(Constants.FAILED);
            return response;
        }
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
            if (StringUtils.isBlank(userId)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg(Constants.INVALID_USER_TOKEN);
                return response;
            }
            logger.info(Constants.QUESTION_SET_SENT_FOR_PUBLISH);

            Map<String, Object> publishResponse = publish(assessmentIdentifier, token);
            if (MapUtils.isEmpty(publishResponse) || !publishResponse.get(Constants.RESPONSE_CODE).equals(Constants.OK)) {
                logger.info(Constants.FAILED_TO_PUBLISH);
                updateErrorDetails(response, Constants.PUBLISH_QUESTION_SET_FAILED,
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            Map<String, Object> properyMap = new HashMap<>();
            properyMap.put(Constants.USERID, userId);
            List<String> fields = new ArrayList<>();
            fields.add(Constants.ROOT_ORG_ID);
            List<Map<String, Object>> cassandraResponse = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER, properyMap, fields);
            Map<String, Object> orgMap = cassandraResponse.get(0);
            String rootOrgId = (String) orgMap.get(Constants.ROOT_ORG_ID);
            Map<String, Object> updateRequest = new HashMap<>();
            Map<String, Object> request = new HashMap<>();
            Map<String, String> headerValues = new HashMap<>();
            headerValues.put(Constants.X_AUTH_TOKEN, token);
            request.put(Constants.ORGANIZATION_ID, rootOrgId);
            request.put(Constants.CQF_ID, assessmentIdentifier);
            updateRequest.put(Constants.REQUEST, request);
            StringBuilder url = new StringBuilder(serverProperties.getSbUrl());
            url.append(serverProperties.getUpdateOrgPath());
            Object updateOrgResponse = outboundRequestHandlerService.fetchResultUsingPatch(
                    String.valueOf(url), updateRequest, headerValues);
            Map<String, Object> data = new ObjectMapper().convertValue(updateOrgResponse, Map.class);
            if (MapUtils.isEmpty(data) || !data.get(Constants.RESPONSE_CODE).equals(Constants.OK)) {
                updateErrorDetails(response, Constants.UPDATE_ORG_WITH_CQF_ID_FAILED,
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            response.setResponseCode(HttpStatus.OK);
            response.setResult((Map<String, Object>) publishResponse.get(Constants.RESULT));
            response.getParams().setStatus(Constants.SUCCESS);
            logger.info("Post publishing the assessment updating the data {} to elastic search using the topic {}",
                    assessmentIdentifier, serverProperties.getCqfAssessmentPostPublishTopic());
            producer.push(serverProperties.getCqfAssessmentPostPublishTopic(), assessmentIdentifier);
        } catch (Exception e) {
            logger.error(Constants.AUTO_PUBLISH_FAILED, e);
            updateErrorDetails(response, Constants.AUTO_PUBLISH_FAILED, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public Map<String, Object> publish(String assessmentIdentifier, String token) {
        Map<String, Object> questionMap = new HashMap<>();
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.QUESTION, questionMap);
        Map<String, Object> updateRequest = new HashMap<>();
        updateRequest.put(Constants.REQUEST, requestMap);
        Map<String, String> headerValues = new HashMap<>();
        headerValues.put(Constants.X_AUTH_TOKEN, token);
        StringBuilder serviceUrl = new StringBuilder();
        serviceUrl.append(serverProperties.getQuestionSetPublish()).append(Constants.SLASH).append(assessmentIdentifier);
        Object reviewResponse = outboundRequestHandlerService.fetchResultUsingPost(
                serverProperties.getAssessmentHost() + serviceUrl, updateRequest, headerValues);
        return new ObjectMapper().convertValue(reviewResponse, Map.class);
    }
    
    public SBApiResponse submitAssessmentAsyncV6(Map<String, Object> submitRequest, String userAuthToken,boolean editMode) {
        return runSubmitAssessmentAsync(submitRequest, userAuthToken, editMode, "submitAssessmentAsyncV6",
                Constants.METHOD_V5_SUBMIT_ASSESSMENT_ASYNC_V6);
    }

    /**
     * Checks if the given contextCategory requires mandatory passing.
     * Context categories like "Preliminary Assessment" and "Final Milestone Assessment" require passing.
     *
     * @param contextCategory the context category to check
     * @return true if passing is mandatory for this context category
     */
    private boolean isMandatoryPassContextCategory(String contextCategory) {
        if (StringUtils.isBlank(contextCategory)) {
            return false;
        }
        List<String> mandatoryContextCategories = serverProperties.getMandatoryContextCategoriesForPassRequirement();
        return mandatoryContextCategories.stream()
                .anyMatch(category -> category.equalsIgnoreCase(contextCategory));
    }

    /**
     * Determines if content progress update should proceed based on assessment results.
     * Two scenarios are checked:
     * 1. Context category validation: If contextCategory is in mandatory list, user MUST pass
     * 2. Course category validation: If courseCategory is in mandatory list, user MUST pass
     * 
     * For other cases: user must pass OR conditions don't apply
     *
     * @param contextCategory the assessment context category
     * @param courseCategory the course category
     * @param hasPassed whether the user passed the assessment
     * @return true if content update should proceed
     */
    private boolean proceedWithContentUpdate(String contextCategory, String courseCategory, boolean hasPassed) {
        // Scenario 1: Check if contextCategory exists and requires mandatory passing
        if (StringUtils.isNotBlank(contextCategory) && isMandatoryPassContextCategory(contextCategory)) {
            // Must pass for mandatory context categories - return immediately
            return hasPassed;
        }
        
        // Scenario 2: Check if courseCategory requires mandatory passing
        List<String> mandatoryCourseCategoriesList = serverProperties.getMandatoryCourseCategoriesForCertificateGeneration();
        boolean isMandatoryCourseCategory = mandatoryCourseCategoriesList.stream()
                .anyMatch(c -> c.equalsIgnoreCase(courseCategory));
        
        if (isMandatoryCourseCategory) {
            // Must pass for mandatory course categories
            return hasPassed;
        }
        
        // For non-mandatory categories: always allow if passed, or allow even if not passed
        return true;
    }


    /**
     * Calculates retake attempts consumed, handling both cyclical and non-cyclical cooloff modes.
     * For cyclical mode: Counts current cycle attempts and validates cooloff period.
     * For non-cyclical mode: Counts total historical attempts.
     *
     * @param userId                User identifier
     * @param assessmentIdentifier  Assessment identifier
     * @param assessmentAllDetail   Assessment configuration details
     * @param retakeAttemptsAllowed Maximum attempts allowed per cycle (or lifetime)
     * @param existingDataList      User's assessment history
     * @param response              Response object to update with error details if needed
     */
    private void calculateRetakeAttemptsConsumed(String userId, String assessmentIdentifier,
                                                 Map<String, Object> assessmentAllDetail,
                                                 int retakeAttemptsAllowed,
                                                 List<Map<String, Object>> existingDataList,
                                                 SBApiResponse response) {
        // Check if this assessment has cyclical cooloff configured
        boolean hasCyclicalCooloff = assessUtilServ.hasCoolOffPeriod(assessmentAllDetail);
        if (hasCyclicalCooloff) {
            // For cyclical cooloff: calculate current cycle attempts
            int currentCycleCount = assessUtilServ.calculateCyclicalRetakeAttempts(
                    userId, assessmentIdentifier, assessmentAllDetail, existingDataList);
            // Only check cooloff if user has exhausted current cycle attempts
            if (currentCycleCount >= retakeAttemptsAllowed) {
                String coolOffValidationError = assessUtilServ.validateCoolOffPeriod(userId, assessmentIdentifier,
                        assessmentAllDetail, existingDataList);
                if (StringUtils.isNotBlank(coolOffValidationError)) {
                    logger.warn("Cool-off period active - User: {}, Assessment: {}", userId, assessmentIdentifier);
                    updateErrorDetails(response, coolOffValidationError, HttpStatus.INTERNAL_SERVER_ERROR);
                    return; // Return current count, caller checks response code
                }
                // Cooloff period has expired - reset counter for new cycle
                logger.info("Cool-off period completed - User: {} starting new cycle for assessment: {}",
                        userId, assessmentIdentifier);
            } else {
                logger.info("Cyclical cooloff mode - Current cycle attempts: {}, Allowed: {}",
                        currentCycleCount, retakeAttemptsAllowed);
            }
        } else {
            // For non-cyclical: count all historical attempts (permanent limit)
            int totalAttemptsMade = calculateAssessmentRetakeCount(userId, assessmentIdentifier);
            logger.info("Non-cyclical mode - Total attempts made: {}, Allowed: {}",
                    totalAttemptsMade, retakeAttemptsAllowed);
            if (totalAttemptsMade >= retakeAttemptsAllowed) {
                updateErrorDetails(response, Constants.ASSESSMENT_RETRY_ATTEMPTS_CROSSED, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
    }

    /**
     * Calculates the number of retake attempts consumed for an assessment (V5 implementation).
     * Handles both cyclical cooloff (renewable attempts) and non-cyclical (permanent limit) modes.
     *
     * @param userId                 User identifier
     * @param assessmentIdentifier   Assessment identifier
     * @param assessmentAllDetail    Assessment configuration details
     * @param retakeAttemptsAllowed  Maximum allowed retake attempts
     * @param userAssessmentDataList User's assessment history
     * @param response               Response object to set error details if cooloff validation fails
     * @return Number of attempts consumed (0 if new cycle starts after cooloff)
     */
    private int calculateRetakeAttemptsConsumedV5(String userId, String assessmentIdentifier,
                                                   Map<String, Object> assessmentAllDetail,
                                                   int retakeAttemptsAllowed,
                                                   List<Map<String, Object>> userAssessmentDataList,
                                                   SBApiResponse response) {
        // Check if this assessment has cyclical cooloff configured
        boolean hasCyclicalCooloff = assessUtilServ.hasCoolOffPeriod(assessmentAllDetail);
        if (hasCyclicalCooloff) {
            // For cyclical cooloff: first calculate current cycle attempts
            int currentCycleCount = assessUtilServ.calculateCyclicalRetakeAttempts(
                    userId, assessmentIdentifier, assessmentAllDetail, userAssessmentDataList);
            // Only check cooloff if user has exhausted current cycle attempts
            if (currentCycleCount >= retakeAttemptsAllowed) {
                String coolOffValidationError = assessUtilServ.validateCoolOffPeriod(userId, assessmentIdentifier,
                        assessmentAllDetail, userAssessmentDataList);
                if (StringUtils.isNotBlank(coolOffValidationError)) {
                    updateErrorDetails(response, coolOffValidationError, HttpStatus.BAD_REQUEST);
                    logger.info("AssessmentServiceV5Impl::retakeAssessment... Cooloff active - Current cycle exhausted with {} attempts", 
                            currentCycleCount);
                    return currentCycleCount;
                }
                // Cooloff period has expired - reset counter for new cycle
                logger.info("Cool-off period completed - User: {} starting new cycle for assessment: {}", 
                        userId, assessmentIdentifier);
                return 0;
            } else {
                logger.info("Cyclical cooloff mode - Current cycle attempts: {}, Allowed: {}", 
                        currentCycleCount, retakeAttemptsAllowed);
                return currentCycleCount;
            }
        } else {
            // For non-cyclical: count all historical attempts
            int totalAttemptsMade = (int) userAssessmentDataList.stream()
                    .filter(userData -> userData.containsKey(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY)
                            && null != userData.get(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY))
                    .count();
            logger.info("Non-cyclical mode - Total attempts made: {}, Allowed: {}", 
                    totalAttemptsMade, retakeAttemptsAllowed);
            return totalAttemptsMade;
        }
    }


    /**
     * Extracts the shuffle flag from the current assessment hierarchy object.
     * Returns the shuffle configuration set at the assessment level.
     *
     * @param assessmentAllDetail the complete assessment hierarchy containing shuffle config
     * @return the shuffle flag from the assessment object, or true if not found or cannot be cast to Boolean
     */
    private boolean getShuffleFlagFromHierarchy(Map<String, Object> assessmentAllDetail) {
        if (MapUtils.isEmpty(assessmentAllDetail)) {
            return true;
        }
        Object shuffleValue = assessmentAllDetail.get(Constants.SHUFFLE);
        if (shuffleValue instanceof Boolean shuffle) {
            return shuffle;
        }
        return true;
    }

}
