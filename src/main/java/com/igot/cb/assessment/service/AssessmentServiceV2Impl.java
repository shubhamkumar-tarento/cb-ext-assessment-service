package com.igot.cb.assessment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.igot.cb.assessment.repo.AssessmentRepository;
import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.service.OutboundRequestHandlerServiceImpl;
import com.igot.cb.common.util.AccessTokenValidator;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import com.igot.cb.core.producer.Producer;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.igot.cb.common.util.ProjectUtil.updateErrorDetails;


@Service
@SuppressWarnings("unchecked")
public class AssessmentServiceV2Impl implements AssessmentServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(AssessmentServiceV2Impl.class);

    AssessmentUtilServiceV2 assessUtilServ;

    CbExtAssessmentServerProperties serverProperties;

    Producer kafkaProducer;

    AssessmentRepository assessmentRepository;

    RedisCacheMgr redisCacheMgr;

    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    ObjectMapper mapper;

    AccessTokenValidator accessTokenValidator;

    public AssessmentServiceV2Impl(AssessmentUtilServiceV2 assessUtilServ, CbExtAssessmentServerProperties serverProperties, Producer kafkaProducer, AssessmentRepository assessmentRepository, RedisCacheMgr redisCacheMgr, OutboundRequestHandlerServiceImpl outboundRequestHandlerService, ObjectMapper mapper, AccessTokenValidator accessTokenValidator) {
        this.assessUtilServ = assessUtilServ;
        this.serverProperties = serverProperties;
        this.kafkaProducer = kafkaProducer;
        this.assessmentRepository = assessmentRepository;
        this.redisCacheMgr = redisCacheMgr;
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.mapper = mapper;
        this.accessTokenValidator = accessTokenValidator;
    }

    public SBApiResponse readAssessment(String assessmentIdentifier, String token) {
        logger.info("AssessmentServiceV2Impl::readAssessment... Started");
        SBApiResponse response = createDefaultResponse(Constants.API_QUESTIONSET_HIERARCHY_GET);
        String errMsg;
        try {
            String userId = validateAuthTokenAndFetchUserId(token);
            if (userId == null) {
                errMsg = Constants.USER_ID_DOESNT_EXIST;
            } else {
                logger.info("readAssessment.. userId :{}", userId);
                Map<String, Object> assessmentAllDetail = new HashMap<>();
                errMsg = fetchReadHierarchyDetails(assessmentAllDetail, token, assessmentIdentifier);
                if (errMsg.isEmpty()) {
                    errMsg = populateAssessmentQuestionSet(response, assessmentAllDetail, assessmentIdentifier, token,
                            userId);
                }
            }
        } catch (Exception e) {
            logger.error(String.format("Exception in %s : %s", "read Assessment", e.getMessage()), e);
            errMsg = "Failed to read Assessment. Exception: " + e.getMessage();
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private String userAssessmentCacheKey(String assessmentIdentifier, String token) {
        return Constants.USER_ASSESS_REQ + assessmentIdentifier + "_" + token;
    }

    private Map<String, Object> readJsonToMap(String json) throws IOException {
        return mapper.readValue(json, new TypeReference<Map<String, Object>>() {
        });
    }

    private Map<String, Object> parseJsonToMap(String json) {
        return new Gson().fromJson(json, new TypeToken<HashMap<String, Object>>() {
        }.getType());
    }

    /** The value of {@code key} from every map in {@code maps} that carries it. */
    private List<Object> extractFieldValues(List<Map<String, Object>> maps, String key) {
        return maps.stream().filter(map -> map.containsKey(key)).map(map -> map.get(key)).toList();
    }

    /**
     * Puts the question set for this user onto the response, and returns the error message
     * ({@link StringUtils#EMPTY} when there is none).
     */
    private String populateAssessmentQuestionSet(SBApiResponse response, Map<String, Object> assessmentAllDetail,
                                                 String assessmentIdentifier, String token, String userId)
            throws IOException {
        String primaryCategory = (String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY);
        if (primaryCategory.equalsIgnoreCase(Constants.PRACTICE_QUESTION_SET)) {
            response.getResult().put(Constants.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail));
            redisCacheMgr.putCache(userAssessmentCacheKey(assessmentIdentifier, token),
                    response.getResult().get(Constants.QUESTION_SET));
            return StringUtils.EMPTY;
        }

        logger.info("Fetched assessment Details... for : {}", assessmentIdentifier);
        List<Map<String, Object>> existingDataList = assessmentRepository.fetchUserAssessmentDataFromDB(userId,
                assessmentIdentifier);
        Instant assessmentStartTime = Instant.now();
        if (existingDataList.isEmpty()) {
            return startNewAssessment(response, assessmentAllDetail, assessmentIdentifier, token, userId,
                    assessmentStartTime);
        }
        return resumeOrRestartAssessment(response, assessmentAllDetail, assessmentIdentifier, token, userId,
                assessmentStartTime, existingDataList);
    }

    /** First attempt for this user — build the question set and record the start/end window. */
    private String startNewAssessment(SBApiResponse response, Map<String, Object> assessmentAllDetail,
                                      String assessmentIdentifier, String token, String userId,
                                      Instant assessmentStartTime) {
        logger.info("Assessment read first time for user.");
        int expectedDuration = (Integer) assessmentAllDetail.get(Constants.EXPECTED_DURATION);
        Instant assessmentEndTime = calculateAssessmentSubmitTime(expectedDuration, assessmentStartTime, 0);
        Map<String, Object> assessmentData = readAssessmentLevelData(assessmentAllDetail);
        assessmentData.put(Constants.START_TIME, assessmentStartTime);
        assessmentData.put(Constants.END_TIME, assessmentEndTime);
        response.getResult().put(Constants.QUESTION_SET, assessmentData);
        redisCacheMgr.putCache(userAssessmentCacheKey(assessmentIdentifier, token),
                response.getResult().get(Constants.QUESTION_SET));
        boolean isAssessmentUpdatedToDB = assessmentRepository.addUserAssesmentDataToDB(userId, assessmentIdentifier,
                assessmentStartTime, assessmentEndTime,
                (Map<String, Object>) response.getResult().get(Constants.QUESTION_SET), Constants.NOT_SUBMITTED);
        return isAssessmentUpdatedToDB ? StringUtils.EMPTY : Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED;
    }

    /**
     * The user already has a record: hand back the in-progress attempt if the window is still open,
     * otherwise start a fresh one.
     */
    private String resumeOrRestartAssessment(SBApiResponse response, Map<String, Object> assessmentAllDetail,
                                             String assessmentIdentifier, String token, String userId,
                                             Instant assessmentStartTime,
                                             List<Map<String, Object>> existingDataList) throws IOException {
        logger.info("Assessment read... user has details... ");
        Map<String, Object> existingData = existingDataList.get(0);
        Object endTimeObj = existingData.get(Constants.END_TIME);
        Date existingAssessmentEndTime = endTimeObj instanceof Instant instant
                ? Date.from(instant)
                : (Date) endTimeObj;
        boolean withinAssessmentWindow = assessmentStartTime.compareTo(existingAssessmentEndTime.toInstant()) < 0;
        String status = (String) existingData.get(Constants.STATUS);

        if (withinAssessmentWindow && status.equalsIgnoreCase(Constants.NOT_SUBMITTED)) {
            resumeAssessment(response, assessmentIdentifier, token, existingData, assessmentStartTime,
                    existingAssessmentEndTime);
            return StringUtils.EMPTY;
        }
        if ((withinAssessmentWindow && status.equalsIgnoreCase(Constants.SUBMITTED))
                || assessmentStartTime.compareTo(existingAssessmentEndTime.toInstant()) > 0) {
            return restartAssessment(response, assessmentAllDetail, assessmentIdentifier, token, userId,
                    assessmentStartTime);
        }
        return StringUtils.EMPTY;
    }

    /** Attempt still open — serve it from cache, falling back to the copy stored on the DB record. */
    private void resumeAssessment(SBApiResponse response, String assessmentIdentifier, String token,
                                  Map<String, Object> existingData, Instant assessmentStartTime,
                                  Date existingAssessmentEndTime) throws IOException {
        Map<String, Object> questionSetFromAssessment;
        String userQuestionSet = redisCacheMgr.getCache(userAssessmentCacheKey(assessmentIdentifier, token));
        if (!ObjectUtils.isEmpty(userQuestionSet)) {
            questionSetFromAssessment = readJsonToMap(userQuestionSet);
        } else {
            String questionSetFromAssessmentString = (String) existingData.get(Constants.ASSESSMENT_READ_RESPONSE);
            questionSetFromAssessment = parseJsonToMap(questionSetFromAssessmentString);
            questionSetFromAssessment.put(Constants.START_TIME, assessmentStartTime.toEpochMilli());
            questionSetFromAssessment.put(Constants.END_TIME, existingAssessmentEndTime);
            response.getResult().put(Constants.QUESTION_SET, questionSetFromAssessment);
            redisCacheMgr.putCache(userAssessmentCacheKey(assessmentIdentifier, token), questionSetFromAssessment);
        }
        response.getResult().put(Constants.QUESTION_SET, questionSetFromAssessment);
    }

    /** Previous attempt was submitted, or its window has expired — read the assessment freshly. */
    private String restartAssessment(SBApiResponse response, Map<String, Object> assessmentAllDetail,
                                     String assessmentIdentifier, String token, String userId,
                                     Instant assessmentStartTime) {
        logger.info("Incase the assessment is submitted before the end time, or the endtime has exceeded, read assessment freshly ");
        Map<String, Object> assessmentData = readAssessmentLevelData(assessmentAllDetail);
        int expectedDuration = (Integer) assessmentAllDetail.get(Constants.EXPECTED_DURATION);
        Instant assessmentEndTime = calculateAssessmentSubmitTime(expectedDuration, assessmentStartTime, 0);
        assessmentData.put(Constants.START_TIME, assessmentStartTime.toEpochMilli());
        assessmentData.put(Constants.END_TIME, assessmentEndTime);
        response.getResult().put(Constants.QUESTION_SET, assessmentData);
        boolean isAssessmentUpdatedToDB = assessmentRepository.addUserAssesmentDataToDB(userId, assessmentIdentifier,
                assessmentStartTime, calculateAssessmentSubmitTime(expectedDuration, assessmentStartTime, 0),
                (Map<String, Object>) response.getResult().get(Constants.QUESTION_SET), Constants.NOT_SUBMITTED);
        redisCacheMgr.putCache(userAssessmentCacheKey(assessmentIdentifier, token),
                response.getResult().get(Constants.QUESTION_SET));
        return isAssessmentUpdatedToDB ? StringUtils.EMPTY : Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED;
    }

    public SBApiResponse readQuestionList(Map<String, Object> requestBody, String authUserToken) {
        SBApiResponse response = createDefaultResponse(Constants.API_SUBMIT_ASSESSMENT);
        String errMsg;
        try {
            List<String> identifierList = new ArrayList<>();
            List<Object> questionList = new ArrayList<>();
            Map<String, String> result = validateQuestionListAPI(requestBody, authUserToken, identifierList);
            errMsg = result.get(Constants.ERROR_MESSAGE);
            if (errMsg.isEmpty()) {
                String primaryCategory = result.get(Constants.PRIMARY_CATEGORY);
                List<String> newIdentifierList = collectCachedQuestions(identifierList, primaryCategory, questionList);
                if (!newIdentifierList.isEmpty()) {
                    errMsg = fetchAndCacheMissingQuestions(newIdentifierList, primaryCategory, questionList);
                }
                if (errMsg.isEmpty() && identifierList.size() == questionList.size()) {
                    response.getResult().put(Constants.QUESTIONS, questionList);
                }
            }
        } catch (Exception e) {
            errMsg = "Failed to fetch the question list. Exception: " + e.getMessage();
            logger.error(errMsg, e);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.BAD_REQUEST);
        }
        return response;
    }

    /**
     * Appends every question already held in Redis to {@code questionList}.
     *
     * @return the identifiers that were not cached and still have to be fetched.
     */
    private List<String> collectCachedQuestions(List<String> identifierList, String primaryCategory,
            List<Object> questionList) throws IOException {
        List<String> newIdentifierList = new ArrayList<>();
        List<String> map = redisCacheMgr.mget(identifierList);
        for (int i = 0; i < map.size(); i++) {
            if (ObjectUtils.isEmpty(map.get(i))) {
                newIdentifierList.add(identifierList.get(i));
            } else {
                Map<String, Object> questionString = readJsonToMap(map.get(i));
                questionList.add(assessUtilServ.filterQuestionMapDetail(questionString, primaryCategory, true));
            }
        }
        return newIdentifierList;
    }

    /**
     * Fetches the questions missing from the cache, stores each one in Redis and appends it to
     * {@code questionList}.
     *
     * @return the error message, or an empty string when the fetch succeeded.
     */
    private String fetchAndCacheMissingQuestions(List<String> newIdentifierList, String primaryCategory,
            List<Object> questionList) {
        List<Map<String, Object>> newQuestionList = assessUtilServ.readQuestionDetails(newIdentifierList);
        if (newQuestionList.isEmpty()) {
            String errMsg = Constants.FAILED_TO_GET_QUESTION_DETAILS;
            logger.error(errMsg, new Exception());
            return errMsg;
        }
        for (Map<String, Object> questionMap : newQuestionList) {
            cacheQuestionsFromResponse(questionMap, primaryCategory, questionList);
        }
        return "";
    }

    /** Stores each question of one question-list response in Redis and appends it to the result. */
    private void cacheQuestionsFromResponse(Map<String, Object> questionMap, String primaryCategory,
            List<Object> questionList) {
        if (ObjectUtils.isEmpty(questionMap) || ObjectUtils.isEmpty(((Map<String, Object>) questionMap.get(Constants.RESULT)).get(Constants.QUESTIONS))) {
            return;
        }
        List<Map<String, Object>> questions = (List<Map<String, Object>>) ((Map<String, Object>) questionMap.get(Constants.RESULT)).get(Constants.QUESTIONS);
        for (Map<String, Object> question : questions) {
            if (!question.isEmpty()) {
                redisCacheMgr.putCache(Constants.QUESTION_ID + question.get(Constants.IDENTIFIER), question);
                questionList.add(assessUtilServ.filterQuestionMapDetail(question, primaryCategory, true));
            }
        }
    }

    private String validateAuthTokenAndFetchUserId(String authUserToken) {
        return accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
    }

    private String fetchReadHierarchyDetails(Map<String, Object> assessmentAllDetail, String token, String assessmentIdentifier) throws IOException {
        try {
            String assessmentData = redisCacheMgr.getCache(Constants.ASSESSMENT_ID + assessmentIdentifier);
            if (!ObjectUtils.isEmpty(assessmentData)) {
                assessmentAllDetail.putAll(readJsonToMap(assessmentData));
            } else {
                Map<String, Object> readHierarchyApiResponse = assessUtilServ.getReadHierarchyApiResponse(assessmentIdentifier, token);
                if (!readHierarchyApiResponse.isEmpty()
                        && (ObjectUtils.isEmpty(readHierarchyApiResponse) || !Constants.OK.equalsIgnoreCase((String) readHierarchyApiResponse.get(Constants.RESPONSE_CODE)))) {
                    return Constants.ASSESSMENT_HIERARCHY_READ_FAILED;
                }
                assessmentAllDetail.putAll((Map<String, Object>) ((Map<String, Object>) readHierarchyApiResponse.get(Constants.RESULT)).get(Constants.QUESTION_SET));
                redisCacheMgr.putCache(Constants.ASSESSMENT_ID + assessmentIdentifier, ((Map<String, Object>) readHierarchyApiResponse.get(Constants.RESULT)).get(Constants.QUESTION_SET));
            }
        } catch (Exception e) {
            logger.info("Error while fetching or mapping read hierarchy data{}", e.getMessage());
            return Constants.ASSESSMENT_HIERARCHY_READ_FAILED;
        }
        return StringUtils.EMPTY;
    }

    private Map<String, String> validateQuestionListAPI(Map<String, Object> requestBody, String authUserToken, List<String> identifierList) throws IOException {
        Map<String, String> result = new HashMap<>();
        String userId = validateAuthTokenAndFetchUserId(authUserToken);
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
        Map<String, Object> assessmentAllDetail = new HashMap<>();
        String errMsg = fetchReadHierarchyDetails(assessmentAllDetail, authUserToken, assessmentIdFromRequest);
        if (!errMsg.isEmpty()) {
            result.put(Constants.ERROR_MESSAGE, errMsg);
            return result;
        }
        Map<String, Object> userAssessmentAllDetail = new HashMap<>();
        errMsg = loadUserAssessmentDetail(userAssessmentAllDetail, assessmentAllDetail, userId,
                assessmentIdFromRequest, authUserToken);
        if (!errMsg.isEmpty()) {
            result.put(Constants.ERROR_MESSAGE, errMsg);
            return result;
        }
        applyQuestionIdMatch(result, userAssessmentAllDetail, identifierList, assessmentIdFromRequest);
        return result;
    }

    /**
     * Loads the user's own copy of the assessment into {@code userAssessmentAllDetail}: from the
     * Redis session when one is held, otherwise from the database. A practice question set has no
     * stored copy and is left empty, exactly as before.
     *
     * <p>The original chain was {@code if (!isEmpty(userQuestionSet)) / else if (isEmpty(...)) /
     * else}; those two conditions are exact complements, so the trailing {@code else} branch could
     * never run and is not carried over.</p>
     *
     * @return the error message, or an empty string when the load succeeded.
     */
    private String loadUserAssessmentDetail(Map<String, Object> userAssessmentAllDetail,
            Map<String, Object> assessmentAllDetail, String userId, String assessmentIdFromRequest,
            String authUserToken) throws IOException {
        String userQuestionSet = redisCacheMgr.getCache(userAssessmentCacheKey(assessmentIdFromRequest, authUserToken));
        if (!ObjectUtils.isEmpty(userQuestionSet)) {
            userAssessmentAllDetail.putAll(readJsonToMap(userQuestionSet));
            return "";
        }
        if (((String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY)).equalsIgnoreCase(Constants.PRACTICE_QUESTION_SET)) {
            return "";
        }
        List<Map<String, Object>> existingDataList = assessmentRepository.fetchUserAssessmentDataFromDB(userId, assessmentIdFromRequest);
        String questionSetFromAssessmentString = CollectionUtils.isNotEmpty(existingDataList)
                ? (String) existingDataList.get(0).get(Constants.ASSESSMENT_READ_RESPONSE)
                : "";
        if (StringUtils.isBlank(questionSetFromAssessmentString)) {
            return Constants.USER_ASSESSMENT_DATA_NOT_PRESENT;
        }
        userAssessmentAllDetail.putAll(parseJsonToMap(questionSetFromAssessmentString));
        return "";
    }

    /**
     * Confirms the requested question ids are part of the user's latest assessment and records the
     * outcome on {@code result}.
     */
    private void applyQuestionIdMatch(Map<String, String> result, Map<String, Object> userAssessmentAllDetail,
            List<String> identifierList, String assessmentIdFromRequest) {
        String assessmentIdFromDatabase = (String) (userAssessmentAllDetail.get(Constants.IDENTIFIER));
        if (!assessmentIdFromDatabase.equalsIgnoreCase(assessmentIdFromRequest)) {
            result.put(Constants.ERROR_MESSAGE, Constants.ASSESSMENT_ID_INVALID);
            return;
        }
        result.put(Constants.PRIMARY_CATEGORY, (String) userAssessmentAllDetail.get(Constants.PRIMARY_CATEGORY));
        List<String> questionsFromAssessment = new ArrayList<>();
        List<Map<String, Object>> sections = (List<Map<String, Object>>) userAssessmentAllDetail.get(Constants.CHILDREN);
        for (Map<String, Object> section : sections) {
            // Out of the list of questions received in the payload, checking if the request
            // has only those ids which are a part of the user's latest assessment
            // Fetching all the remaining questions details from the Redis
            questionsFromAssessment.addAll((List<String>) section.get(Constants.CHILD_NODES));
        }
        if (validateQuestionListRequest(identifierList, questionsFromAssessment)) {
            result.put(Constants.ERROR_MESSAGE, StringUtils.EMPTY);
        } else {
            result.put(Constants.ERROR_MESSAGE, Constants.THE_QUESTIONS_IDS_PROVIDED_DONT_MATCH);
        }
    }

    @Override
    public SBApiResponse submitAssessment(Map<String, Object> submitRequest, String authUserToken,boolean editMode) throws IOException {
        SBApiResponse outgoingResponse = createDefaultResponse(Constants.API_SUBMIT_ASSESSMENT);
        String errMsg;
        List<Map<String, Object>> sectionListFromSubmitRequest = new ArrayList<>();
        List<Map<String, Object>> hierarchySectionList = new ArrayList<>();
        Map<String, Object> assessmentHierarchy = new HashMap<>();
        errMsg = validateSubmitAssessmentRequest(submitRequest, authUserToken, hierarchySectionList, sectionListFromSubmitRequest, assessmentHierarchy);
        if (errMsg.isEmpty()) {
            errMsg = scoreSubmission(submitRequest, authUserToken, editMode, hierarchySectionList,
                    sectionListFromSubmitRequest, assessmentHierarchy, outgoingResponse);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(outgoingResponse, errMsg, HttpStatus.BAD_REQUEST);
        }
        return outgoingResponse;
    }

    /**
     * Scores every section of a validated submission and folds the results into
     * {@code outgoingResponse}. An assessment-level cut-off is settled on the first scored section
     * and finishes there; a section-level cut-off aggregates all sections after the loop.
     *
     * @return the error message, or an empty string when the response carries the final result.
     */
    private String scoreSubmission(Map<String, Object> submitRequest, String authUserToken, boolean editMode,
            List<Map<String, Object>> hierarchySectionList, List<Map<String, Object>> sectionListFromSubmitRequest,
            Map<String, Object> assessmentHierarchy, SBApiResponse outgoingResponse) throws IOException {
        String errMsg = "";
        String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
        String userId = validateAuthTokenAndFetchUserId(authUserToken);
        String scoreCutOffType = ((String) assessmentHierarchy.get(Constants.SCORE_CUTOFF_TYPE)).toLowerCase();
        List<Map<String, Object>> sectionLevelsResults = new ArrayList<>();
        List<String> questionsListFromAssessmentHierarchy = new ArrayList<>();
        Map<String, Object> questionSetFromAssessment = new HashMap<>();
        boolean practiceQuestionSet = ((String) (assessmentHierarchy.get(Constants.PRIMARY_CATEGORY)))
                .equalsIgnoreCase(Constants.PRACTICE_QUESTION_SET);
        String primaryCategory = (String) assessmentHierarchy.get(Constants.PRIMARY_CATEGORY);
        for (Map<String, Object> hierarchySection : hierarchySectionList) {
            UserSection userSection = findUserSection(sectionListFromSubmitRequest,
                    (String) hierarchySection.get(Constants.IDENTIFIER));
            SectionQuestions sectionQuestions = resolveSectionQuestions(practiceQuestionSet, submitRequest,
                    authUserToken, userId, questionSetFromAssessment, questionsListFromAssessmentHierarchy,
                    userSection);
            if (sectionQuestions == null) {
                errMsg = "Question Set From The Database returns Null";
                outgoingResponse.getResult().clear();
                break;
            }
            questionSetFromAssessment = sectionQuestions.questionSet();
            questionsListFromAssessmentHierarchy = sectionQuestions.questionIds();
            List<Map<String, Object>> questionsListFromSubmitRequest = sectionQuestions.submittedQuestions();
            hierarchySection.put(Constants.SCORE_CUTOFF_TYPE, scoreCutOffType);

            // The two cut-off types are mutually exclusive, so a section is scored at most once here.
            if (Constants.ASSESSMENT_LEVEL_SCORE_CUTOFF.equals(scoreCutOffType)) {
                Map<String, Object> result = scoreSection(hierarchySection, questionsListFromAssessmentHierarchy,
                        questionsListFromSubmitRequest, assessmentIdFromRequest, editMode, authUserToken);
                outgoingResponse.getResult().putAll(calculateAssessmentFinalResults(result));
                if (!practiceQuestionSet) {
                    writeDataToDatabaseAndTriggerKafkaEvent(submitRequest, userId, questionSetFromAssessment,
                            result, primaryCategory);
                }
                return "";
            }
            if (Constants.SECTION_LEVEL_SCORE_CUTOFF.equals(scoreCutOffType)) {
                sectionLevelsResults.add(scoreSection(hierarchySection, questionsListFromAssessmentHierarchy,
                        questionsListFromSubmitRequest, assessmentIdFromRequest, editMode, authUserToken));
            }
        }
        if (errMsg.isEmpty() && !ObjectUtils.isEmpty(scoreCutOffType)
                && scoreCutOffType.equalsIgnoreCase(Constants.SECTION_LEVEL_SCORE_CUTOFF)) {
            finaliseSectionLevelResults(outgoingResponse, sectionLevelsResults, submitRequest, userId,
                    questionSetFromAssessment, primaryCategory);
        }
        return errMsg;
    }

    /** Rolls the per-section results into the overall response and persists the submission. */
    private void finaliseSectionLevelResults(SBApiResponse outgoingResponse,
                                             List<Map<String, Object>> sectionLevelsResults,
                                             Map<String, Object> submitRequest, String userId,
                                             Map<String, Object> questionSetFromAssessment, String primaryCategory) {
        Map<String, Object> result = calculateSectionFinalResults(sectionLevelsResults);
        outgoingResponse.getResult().putAll(result);
        writeDataToDatabaseAndTriggerKafkaEvent(submitRequest, userId, questionSetFromAssessment, result,
                primaryCategory);
    }

    /** The section of the submit request matching a hierarchy section, and the id last examined. */
    private record UserSection(String id, Map<String, Object> data) {
    }

    /** The question ids to score a section against, and the questions the user actually submitted. */
    private record SectionQuestions(Map<String, Object> questionSet, List<String> questionIds,
                                    List<Map<String, Object>> submittedQuestions) {
    }

    /**
     * Works out which questions a section should be scored against. For a practice set the ids come
     * straight from the submission; otherwise they come from the stored question set. Returns
     * {@code null} when that stored question set could not be read.
     */
    private SectionQuestions resolveSectionQuestions(boolean practiceQuestionSet, Map<String, Object> submitRequest,
                                                     String authUserToken, String userId,
                                                     Map<String, Object> questionSetFromAssessment,
                                                     List<String> currentQuestionIds, UserSection userSection)
            throws IOException {
        List<Map<String, Object>> submittedQuestions = childrenOf(userSection.data());
        if (practiceQuestionSet) {
            return new SectionQuestions(questionSetFromAssessment, questionIdsFrom(submittedQuestions),
                    submittedQuestions);
        }
        Map<String, Object> questionSet = loadQuestionSetFromAssessment(submitRequest, authUserToken, userId,
                questionSetFromAssessment);
        if (questionSet == null || questionSet.get(Constants.CHILDREN) == null) {
            return null;
        }
        List<String> sectionChildNodes = findSectionChildNodes(questionSet, userSection.id(), currentQuestionIds);
        return new SectionQuestions(questionSet, sectionChildNodes, submittedQuestions);
    }

    private UserSection findUserSection(List<Map<String, Object>> sectionListFromSubmitRequest,
                                        String hierarchySectionId) {
        String userSectionId = "";
        Map<String, Object> userSectionData = new HashMap<>();
        for (Map<String, Object> sectionFromSubmitRequest : sectionListFromSubmitRequest) {
            userSectionId = (String) sectionFromSubmitRequest.get(Constants.IDENTIFIER);
            if (userSectionId.equalsIgnoreCase(hierarchySectionId)) {
                userSectionData = sectionFromSubmitRequest;
                break;
            }
        }
        return new UserSection(userSectionId, userSectionData);
    }

    /** The identifiers of the questions carried by a submitted section. */
    private List<String> questionIdsFrom(List<Map<String, Object>> questionsListFromSubmitRequest) {
        List<Object> questionsList = extractFieldValues(questionsListFromSubmitRequest, Constants.IDENTIFIER);
        return questionsList.stream().map(object -> Objects.toString(object, null))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /** The submitted children for a section, or an empty list when the section carries none. */
    private List<Map<String, Object>> childrenOf(Map<String, Object> userSectionData) {
        if (userSectionData.containsKey(Constants.CHILDREN)
                && !ObjectUtils.isEmpty(userSectionData.get(Constants.CHILDREN))) {
            return (List<Map<String, Object>>) userSectionData.get(Constants.CHILDREN);
        }
        return new ArrayList<>();
    }

    /**
     * The question set for this attempt, preferring the cached copy and falling back to the one
     * stored against the user's DB record. Returns {@code fallback} when neither is available.
     */
    private Map<String, Object> loadQuestionSetFromAssessment(Map<String, Object> submitRequest, String authUserToken,
                                                              String userId, Map<String, Object> fallback)
            throws IOException {
        String assessmentData = redisCacheMgr.getCache(
                userAssessmentCacheKey((String) submitRequest.get(Constants.IDENTIFIER), authUserToken));
        if (!ObjectUtils.isEmpty(assessmentData)) {
            fallback.putAll(readJsonToMap(assessmentData));
            return fallback;
        }
        List<Map<String, Object>> existingDataList = assessmentRepository.fetchUserAssessmentDataFromDB(userId,
                (String) submitRequest.get(Constants.IDENTIFIER));
        String questionSetFromAssessmentString = (!existingDataList.isEmpty())
                ? (String) existingDataList.get(0).get(Constants.ASSESSMENT_READ_RESPONSE_KEY)
                : "";
        if (!questionSetFromAssessmentString.isEmpty()) {
            return parseJsonToMap(questionSetFromAssessmentString);
        }
        return fallback;
    }

    /** Child node ids of the section matching {@code userSectionId}, or {@code fallback} when there is no match. */
    private List<String> findSectionChildNodes(Map<String, Object> questionSetFromAssessment, String userSectionId,
                                               List<String> fallback) {
        List<Map<String, Object>> sections = (List<Map<String, Object>>) questionSetFromAssessment
                .get(Constants.CHILDREN);
        for (Map<String, Object> section : sections) {
            if (userSectionId.equalsIgnoreCase((String) section.get(Constants.IDENTIFIER))) {
                List<String> childNodes = (List<String>) section.get(Constants.CHILD_NODES);
                return childNodes != null ? childNodes : fallback;
            }
        }
        return fallback;
    }

    /** Scores one section of the submission against the assessment hierarchy. */
    private Map<String, Object> scoreSection(Map<String, Object> hierarchySection,
                                             List<String> questionsListFromAssessmentHierarchy,
                                             List<Map<String, Object>> questionsListFromSubmitRequest,
                                             String assessmentIdFromRequest, boolean editMode, String authUserToken)
            throws IOException {
        Map<String, Object> result = new HashMap<>();
        result.putAll(createResponseMapWithProperStructure(hierarchySection,
                assessUtilServ.validateQumlAssessment(questionsListFromAssessmentHierarchy,
                        questionsListFromSubmitRequest,
                        assessUtilServ.readQListfromCache(questionsListFromAssessmentHierarchy,
                                assessmentIdFromRequest, editMode, authUserToken))));
        return result;
    }

    private void writeDataToDatabaseAndTriggerKafkaEvent(Map<String, Object> submitRequest, String userId, Map<String, Object> questionSetFromAssessment, Map<String, Object> result, String primaryCategory) {
        try {
            if (questionSetFromAssessment == null || questionSetFromAssessment.get(Constants.START_TIME) == null) {
                return;
            }
            Instant startTime = assessUtilServ.parseStartTimeToInstant(questionSetFromAssessment.get(Constants.START_TIME));
            Boolean isAssessmentUpdatedToDB = assessmentRepository.updateUserAssesmentDataToDB(userId, (String) submitRequest.get(Constants.IDENTIFIER), submitRequest, result, Constants.SUBMITTED, startTime,null);
            if (Boolean.TRUE.equals(isAssessmentUpdatedToDB)) {
                kafkaProducer.push(serverProperties.getAssessmentSubmitTopic(),
                        buildSubmitEvent(submitRequest, result, primaryCategory));
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    /**
     * The assessment-submitted event payload. A competency assessment additionally carries the first
     * competency from the request, or an empty value when the request lists none.
     */
    private Map<String, Object> buildSubmitEvent(Map<String, Object> submitRequest, Map<String, Object> result,
            String primaryCategory) throws JsonProcessingException {
        Map<String, Object> kafkaResult = new HashMap<>();
        kafkaResult.put(Constants.CONTENT_ID_KEY, submitRequest.get(Constants.IDENTIFIER));
        kafkaResult.put(Constants.COURSE_ID, submitRequest.get(Constants.COURSE_ID) != null ? submitRequest.get(Constants.COURSE_ID) : "");
        kafkaResult.put(Constants.BATCH_ID, submitRequest.get(Constants.BATCH_ID) != null ? submitRequest.get(Constants.BATCH_ID) : "");
        kafkaResult.put(Constants.USER_ID, submitRequest.get(Constants.USER_ID));
        kafkaResult.put(Constants.ASSESSMENT_ID_KEY, submitRequest.get(Constants.IDENTIFIER));
        kafkaResult.put(Constants.PRIMARY_CATEGORY, primaryCategory);
        kafkaResult.put(Constants.TOTAL_SCORE, result.get(Constants.OVERALL_RESULT));
        if ("Competency Assessment".equalsIgnoreCase(primaryCategory)
                && submitRequest.containsKey(Constants.COMPETENCIES_V3)
                && submitRequest.get(Constants.COMPETENCIES_V3) != null) {
            ObjectMapper objectMapper = new ObjectMapper(); //Updated for Json Import
            List<Map<String, Object>> competencyList = objectMapper.readValue(
                    (String) submitRequest.get(Constants.COMPETENCIES_V3),
                    new TypeReference<List<Map<String, Object>>>() {
                    }
            );
            // First competency map, or an empty value when none were submitted
            kafkaResult.put(Constants.COMPETENCY, competencyList.isEmpty() ? "" : competencyList.get(0));
        }
        return kafkaResult;
    }

    private String validateSubmitAssessmentRequest(Map<String, Object> submitRequest, String authUserToken, List<Map<String, Object>> hierarchySectionList, List<Map<String, Object>> sectionListFromSubmitRequest, Map<String, Object> assessmentHierarchy) throws IOException {
        String userId = validateAuthTokenAndFetchUserId(authUserToken);
        if (ObjectUtils.isEmpty(userId)) {
            return Constants.USER_ID_DOESNT_EXIST;
        }
        submitRequest.put(Constants.USER_ID, userId);
        if (StringUtils.isEmpty((String) submitRequest.get(Constants.IDENTIFIER))) {
            return Constants.INVALID_ASSESSMENT_ID;
        }
        String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
        String errMsg = fetchReadHierarchyDetails(assessmentHierarchy, authUserToken, assessmentIdFromRequest);
        if (!errMsg.isEmpty()) {
            return errMsg;
        }
        if (ObjectUtils.isEmpty(assessmentHierarchy)) {
            return Constants.READ_ASSESSMENT_FAILED;
        }
        hierarchySectionList.addAll((List<Map<String, Object>>) assessmentHierarchy.get(Constants.CHILDREN));
        sectionListFromSubmitRequest.addAll((List<Map<String, Object>>) submitRequest.get(Constants.CHILDREN));
        if (((String) (assessmentHierarchy.get(Constants.PRIMARY_CATEGORY))).equalsIgnoreCase(Constants.PRACTICE_QUESTION_SET))
            return "";
        List<Map<String, Object>> existingDataList = assessmentRepository.fetchUserAssessmentDataFromDB(userId, assessmentIdFromRequest);
        if (existingDataList.isEmpty()) {
            return Constants.USER_ASSESSMENT_DATA_NOT_PRESENT;
        }
        if (((String) existingDataList.get(0).get(Constants.STATUS)).equalsIgnoreCase(Constants.SUBMITTED)) {
            return Constants.ASSESSMENT_ALREADY_SUBMITTED;
        }
        // The list is known to be non-empty here, so the record is always present.
        Date assessmentStartTime = (Date) existingDataList.get(0).get(Constants.START_TIME);
        if (assessmentStartTime == null) {
            return Constants.READ_ASSESSMENT_START_TIME_FAILED;
        }
        int expectedDuration = (Integer) assessmentHierarchy.get(Constants.EXPECTED_DURATION);
        if (serverProperties.getUserAssessmentSubmissionDuration().isEmpty()) {
            serverProperties.setUserAssessmentSubmissionDuration("120");
        }
        Instant later = calculateAssessmentSubmitTime(expectedDuration, assessmentStartTime.toInstant(), Integer.parseInt(serverProperties.getUserAssessmentSubmissionDuration()));
        Instant submissionTime = Instant.now();
        if (submissionTime.compareTo(later) > 0) {
            return Constants.ASSESSMENT_SUBMIT_EXPIRED;
        }
        List<Object> hierarchySectionIds = extractFieldValues(hierarchySectionList, Constants.IDENTIFIER);
        List<Object> submitSectionIds = extractFieldValues(sectionListFromSubmitRequest, Constants.IDENTIFIER);
        if (!new HashSet<>(hierarchySectionIds).containsAll(submitSectionIds)) {
            return Constants.WRONG_SECTION_DETAILS;
        }
        return validateIfQuestionIdsAreSame(submitRequest, sectionListFromSubmitRequest, userId);
    }

    private String validateIfQuestionIdsAreSame(Map<String, Object> submitRequest, List<Map<String, Object>> sectionListFromSubmitRequest, String userId) {
        List<Map<String, Object>> existingDataList = assessmentRepository.fetchUserAssessmentDataFromDB(userId, (String) submitRequest.get(Constants.IDENTIFIER));
        String questionSetFromAssessmentString = (!existingDataList.isEmpty()) ? (String) existingDataList.get(0).get(Constants.ASSESSMENT_READ_RESPONSE_KEY) : "";
        if (StringUtils.isBlank(questionSetFromAssessmentString)) {
            return Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED;
        }
        Map<String, Object> questionSetFromAssessment = parseJsonToMap(questionSetFromAssessmentString);
        if (questionSetFromAssessment == null || questionSetFromAssessment.get(Constants.CHILDREN) == null) {
            return "";
        }
        List<Object> questionIdsFromAssessmentHierarchy = collectHierarchyQuestionIds(questionSetFromAssessment);
        List<Object> userQuestionIdsFromSubmitRequest =
                collectSubmittedQuestionIds(sectionListFromSubmitRequest);
        if (!new HashSet<>(questionIdsFromAssessmentHierarchy).containsAll(userQuestionIdsFromSubmitRequest)) {
            return Constants.ASSESSMENT_SUBMIT_INVALID_QUESTION;
        }
        return "";
    }

    /** Flattens the child-node ids declared by every section of the stored question set. */
    private List<Object> collectHierarchyQuestionIds(Map<String, Object> questionSetFromAssessment) {
        List<Map<String, Object>> sections = (List<Map<String, Object>>) questionSetFromAssessment.get(Constants.CHILDREN);
        List<Object> questionList = extractFieldValues(sections, Constants.CHILD_NODES);
        List<Object> questionIdsFromAssessmentHierarchy = new ArrayList<>();
        for (Object question : questionList) {
            questionIdsFromAssessmentHierarchy.addAll((List<String>) question);
        }
        return questionIdsFromAssessmentHierarchy;
    }

    /** Flattens the question ids the user actually submitted, across every section of the request. */
    private List<Object> collectSubmittedQuestionIds(List<Map<String, Object>> sectionListFromSubmitRequest) {
        List<Map<String, Object>> questionsListFromSubmitRequest = new ArrayList<>();
        for (Map<String, Object> userSectionData : sectionListFromSubmitRequest) {
            if (userSectionData.containsKey(Constants.CHILDREN) && !ObjectUtils.isEmpty(userSectionData.get(Constants.CHILDREN))) {
                questionsListFromSubmitRequest.addAll((List<Map<String, Object>>) userSectionData.get(Constants.CHILDREN));
            }
        }
        return extractFieldValues(questionsListFromSubmitRequest, Constants.IDENTIFIER);
    }

    private Instant calculateAssessmentSubmitTime(int expectedDurationInSeconds, Instant assessmentStartTime,
                                                  int bufferTimeInSeconds) {
        int totalDurationInSeconds = expectedDurationInSeconds;

        if (bufferTimeInSeconds > 0) {
            totalDurationInSeconds += Integer.parseInt(serverProperties.getUserAssessmentSubmissionDuration());
        }

        return assessmentStartTime.plusSeconds(totalDurationInSeconds);
    }

    private Map<String, Object> calculateAssessmentFinalResults(Map<String, Object> assessmentLevelResult) {
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
            logger.info(e.getMessage());
        }
        return res;
    }

    private Map<String, Object> calculateSectionFinalResults(List<Map<String, Object>> sectionLevelResults) {
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
            logger.info(e.getMessage());
        }
        return res;
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

    private void readSectionLevelParams(Map<String, Object> assessmentAllDetail, Map<String, Object> assessmentFilteredDetail) {
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
            List<String> allQuestionIdList = new ArrayList<>();
            List<Map<String, Object>> questions = (List<Map<String, Object>>) section.get(Constants.CHILDREN);
            for (Map<String, Object> question : questions) {
                allQuestionIdList.add((String) question.get(Constants.IDENTIFIER));
            }
            Collections.shuffle(allQuestionIdList);
            List<String> childNodeList = new ArrayList<>();
            if (!ObjectUtils.isEmpty(section.get(Constants.MAX_QUESTIONS))) {
                int maxQuestions = (int) section.get(Constants.MAX_QUESTIONS);
                childNodeList = allQuestionIdList.stream().limit(maxQuestions).collect(Collectors.toCollection(ArrayList::new));
            }
            newSection.put(Constants.CHILD_NODES, childNodeList);
            sectionResponse.add(newSection);
        }
        assessmentFilteredDetail.put(Constants.CHILDREN, sectionResponse);
        assessmentFilteredDetail.put(Constants.CHILD_NODES, sectionIdList);
    }

    private List<String> getQuestionIdList(Map<String, Object> questionListRequest) {
        try {
            if (questionListRequest.containsKey(Constants.REQUEST)) {
                Map<String, Object> request = (Map<String, Object>) questionListRequest.get(Constants.REQUEST);
                if ((!ObjectUtils.isEmpty(request)) && request.containsKey(Constants.SEARCH)) {
                    Map<String, Object> searchObj = (Map<String, Object>) request.get(Constants.SEARCH);
                    if (!ObjectUtils.isEmpty(searchObj) && searchObj.containsKey(Constants.IDENTIFIER) && !CollectionUtils.isEmpty((List<String>) searchObj.get(Constants.IDENTIFIER))) {
                        return (List<String>) searchObj.get(Constants.IDENTIFIER);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(String.format("Failed to process the questionList request body. %s", e.getMessage()), e);
        }
        return Collections.emptyList();
    }

    public Map<String, Object> createResponseMapWithProperStructure(Map<String, Object> hierarchySection, Map<String, Object> resultMap) {
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
        } else {
            result = 0.0;
            sectionLevelResult.put(Constants.RESULT, result);
            List<String> childNodes = (List<String>) hierarchySection.get(Constants.CHILDREN);
            sectionLevelResult.put(Constants.TOTAL, childNodes.size());
            sectionLevelResult.put(Constants.BLANK, childNodes.size());
            sectionLevelResult.put(Constants.CORRECT, 0);
            sectionLevelResult.put(Constants.INCORRECT, 0);
        }
        sectionLevelResult.put(Constants.PASS, result >= ((Integer) hierarchySection.get(Constants.MINIMUM_PASS_PERCENTAGE)));
        sectionLevelResult.put(Constants.OVERALL_RESULT, result);
        return sectionLevelResult;
    }

    private SBApiResponse createDefaultResponse(String api) {
        SBApiResponse response = new SBApiResponse();
        response.setId(api);
        response.setVer(Constants.VER);
        response.getParams().setResmsgid(UUID.randomUUID().toString());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.setTs(DateTime.now().toString());
        return response;
    }

    private boolean validateQuestionListRequest(List<String> identifierList, List<String> questionsFromAssessment) {
        return new HashSet<>(questionsFromAssessment).containsAll(identifierList);
    }

    public SBApiResponse retakeAssessment(String assessmentIdentifier, String token) {
        logger.info("AssessmentServiceV2Impl::retakeAssessment... Started");
        SBApiResponse response = createDefaultResponse(Constants.API_RETAKE_ASSESSMENT_GET);
        String errMsg = "";
        int retakeAttemptsAllowed = 0;
        int retakeAttemptsConsumed = 0;
        try {
            String userId = validateAuthTokenAndFetchUserId(token);
            if (userId != null) {
                List<Map<String, Object>> existingDataList = assessmentRepository.fetchUserAssessmentDataFromDB(userId, assessmentIdentifier);
                Map<String, Object> assessmentAllDetail = new HashMap<>();
                errMsg = fetchReadHierarchyDetails(assessmentAllDetail, token, assessmentIdentifier);
                if (Constants.PRE_ENROLLED_ASSESSMENT_KEY.equals(assessmentAllDetail.get(Constants.CONTEXT_CATEGORY_TAG))) {
                    retakeAttemptsAllowed = 1;
                    retakeAttemptsConsumed = 0;
                } else {
                    if (assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS) != null) {
                        retakeAttemptsAllowed = (int) assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS);
                    }
                    retakeAttemptsConsumed = calculateAssessmentRetakeCount(existingDataList);
                }
            } else {
                errMsg = Constants.USER_ID_DOESNT_EXIST;
            }
        } catch (Exception e) {
            logger.error(String.format("Exception in %s : %s", "read Assessment", e.getMessage()), e);
            errMsg = "Failed to read Assessment. Exception: " + e.getMessage();
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        } else {
            response.getResult().put(Constants.TOTAL_RETAKE_ATTEMPTS_ALLOWED, retakeAttemptsAllowed);
            response.getResult().put(Constants.RETAKE_ATTEMPTS_CONSUMED, retakeAttemptsConsumed);
        }
        return response;
    }

    private int calculateAssessmentRetakeCount(List<Map<String, Object>> userAssessmentData) {
        List<Object> values = new ArrayList<>(extractFieldValues(userAssessmentData, Constants.SUBMIT_ASSESSMENT_RESPONSE));
        Iterables.removeIf(values, Predicates.isNull());
        return values.size();
    }
}