package com.igot.cb.assessment.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.AccessTokenValidator;
import com.igot.cb.core.exception.ApplicationLogicError;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public interface AssessmentUtilServiceV2 {
	public Map<String, Object> validateQumlAssessment(List<String> originalQuestionList,
													  List<Map<String, Object>> userQuestionList, Map<String, Object> questionMap) throws ApplicationLogicError;

	public String fetchQuestionIdentifierValue(List<String> identifierList, List<Object> questionList, String primaryCategory);

	Map<String, Object> filterQuestionMapDetail(Map<String, Object> questionMapResponse, String primaryCategory, boolean shuffle);

	List<Map<String, Object>> readQuestionDetails(List<String> identifiers);

	public Map<String, Object> getReadHierarchyApiResponse(String assessmentIdentifier, String token);

	public Map<String, Object> readAssessmentHierarchyFromCache(String assessmentIdentifier,boolean editMode,String token);

	public List<Map<String, Object>> readUserSubmittedAssessmentRecords(String userId, String assessmentId);

	public Map<String, Object> readQListfromCache(List<String> questionIds, String assessmentIdentifier,boolean editMode,String token) throws IOException;

	public Map<String,Object> fetchHierarchyFromAssessServc(String qSetId,String token);

	public Map<String, Object> fetchWheebox(String userId);

	/**
	 * Validates a Quml assessment by comparing the original list of questions with the user's provided list of questions.
	 *
	 * @param questionSetDetailsMap a map containing details about the question set.
	 * @param originalQuestionList  a list of original question identifiers.
	 * @param userQuestionList      a list of maps where each map represents a user's question with its details.
	 * @param questionMap           a map containing additional question-related information.
	 * @return a map with validation results and resultMap.
	 */
	public Map<String, Object> validateQumlAssessmentV2(Map<String, Object> questionSetDetailsMap, List<String> originalQuestionList,
													   List<Map<String, Object>> userQuestionList, Map<String,Object> questionMap);

	Map<String, Object> filterQuestionMapDetailV2(Map<String, Object> questionMapResponse, String primaryCategory, boolean shuffle);

	/**
	 * Validates a Quml assessment by comparing the original list of questions with the user's provided list of questions.
	 *
	 * @param questionSetDetailsMap a map containing details about the question set.
	 * @param originalQuestionList  a list of original question identifiers.
	 * @param userQuestionList      a list of maps where each map represents a user's question with its details.
	 * @param questionMap           a map containing additional question-related information.
	 * @return a map with validation results and resultMap.
	 */
	Map<String, Object> validateQumlAssessmentV3(Map<String, Object> questionSetDetailsMap, List<String> originalQuestionList,
												 List<Map<String, Object>> userQuestionList, Map<String, Object> questionMap) throws ApplicationLogicError;

	String validateContextLocking(Map<String, Object> assessmentAllDetail, String parentContextId,
								  SBApiResponse response, String userId, String assessmentIdentifier);

	String readAssessmentRecord(String assessmentIdentifier,List<String> fields);

	Instant parseStartTimeToInstant(Object startTimeObj);

	Long parseStartTimeToLong(Object startTimeObj);

	String readContentRecord(String courseId, List<String> fields);

    String validateAssessmentLanguageAndNodes(Map<String, Object> submitRequest) throws ApplicationLogicError;

	/**
	 * Checks if cool-off period is configured and valid for an assessment.
	 *
	 * @param assessmentAllDetail the complete assessment hierarchy containing cool-off configuration
	 * @return true if cool-off period exists and is a valid Integer greater than 0, false otherwise
	 */
	boolean hasCoolOffPeriod(Map<String, Object> assessmentAllDetail);

	/**
	 * Validates if the user is within the cool-off period for retaking an assessment.
	 * The cool-off period prevents immediate retakes after exhausting retry attempts.
	 *
	 * @param userId                  the user's unique identifier
	 * @param assessmentIdentifier    the assessment's unique identifier
	 * @param assessmentAllDetail     the complete assessment hierarchy containing cool-off configuration
	 * @param userAssessmentDataList  list of user's previous assessment attempts, ordered by most recent first
	 * @return empty string if validation passes, error message if cool-off period is active or validation fails
	 */
	String 	validateCoolOffPeriod(String userId, String assessmentIdentifier,
								 Map<String, Object> assessmentAllDetail,
								 List<Map<String, Object>> userAssessmentDataList);

	/**
	 * Calculates current cycle attempts for cyclical cooloff. Cycle boundary: gap >= coolOffPeriod
	 * AND older attempts >= retakeAttemptsAllowed. Counts submitted assessments only, starts from 1.
	 *
	 * @param userId                  user identifier
	 * @param assessmentIdentifier    assessment identifier
	 * @param assessmentAllDetail     config with coolOffPeriod and maxAssessmentRetakeAttempts
	 * @param userAssessmentDataList  attempts ordered by starttime
	 * @return current cycle count (1-based, 0 if none)
	 */
	int calculateCyclicalRetakeAttempts(String userId, String assessmentIdentifier,
										Map<String, Object> assessmentAllDetail,
										List<Map<String, Object>> userAssessmentDataList);

	/**
	 * Publishes a failed assessment audit event to a Kafka error topic for monitoring purposes.
	 * No consumer is attached to this topic — events are retained for future log dump/analysis.
	 *
	 * @param userId        the user identifier
	 * @param assessmentId  the assessment identifier
	 * @param submitRequest the original submit request payload
	 * @param errMessage    the error message describing the failure
	 * @param methodName    the method name where the failure occurred
	 */
	void publishFailedAssessmentAuditEvent(String userId, String assessmentId,
										   Map<String, Object> submitRequest, String errMessage, String methodName,
										   Map<String, Object> submitAssessmentResponse);

	/**
	 * Extracts the shuffle flag from the current assessment hierarchy object.
	 * Returns the shuffle configuration set at the assessment level.
	 *
	 * @param assessmentAllDetail the complete assessment hierarchy containing shuffle config
	 * @return the shuffle flag from the assessment object, or true if not found or cannot be cast to Boolean
	 */
	boolean getShuffleFlagFromHierarchy(Map<String, Object> assessmentAllDetail);

	List<String> getQuestionIdList(Map<String, Object> questionListRequest);

	boolean validateQuestionListRequest(List<String> identifierList, List<String> questionsFromAssessment);

	/**
	 * Records whether every requested question id belongs to the user's latest assessment,
	 * or flags the assessment id as invalid when there is no user assessment data.
	 */
	void applyQuestionIdMatch(Map<String, String> result, Map<String, Object> userAssessmentAllDetail,
							  List<String> identifierList);

	Map<String, String> validateQuestionListAPI(Map<String, Object> requestBody, String authUserToken,
												List<String> identifierList, boolean editMode,
												AccessTokenValidator accessTokenValidator) throws IOException;

	/**
	 * Checks if the given contextCategory requires mandatory passing.
	 * Context categories like "Preliminary Assessment" and "Final Milestone Assessment" require passing.
	 *
	 * @param contextCategory the context category to check
	 * @return true if passing is mandatory for this context category
	 */
	boolean isMandatoryPassContextCategory(String contextCategory);

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
	boolean proceedWithContentUpdate(String contextCategory, String courseCategory, boolean hasPassed);

	String validateAssessmentReadResult(Map<String, Object> request);

	String resolveCourseCategory(Map<String, Object> submitRequest);

	Instant calculateAssessmentSubmitTime(int expectedDurationInSeconds, Instant assessmentStartTime,
										  int bufferTimeInSeconds);

	/** Builds the assessment-submit payload published to Kafka. */
	Map<String, Object> buildSubmitEvent(Map<String, Object> submitRequest, String primaryCategory,
										 Map<String, Object> result) throws JsonProcessingException;

	/** Routes the progress update to the pre-enrolled endpoint or the standard content endpoint. */
	void updateContentProgressForContext(String contextCategory, String userAuthToken,
										 Map<String, Object> submitRequest, String userId);

	/**
	 * Controls how a blocked retake is reported by {@link #calculateRetakeAttemptsConsumed}.
	 *
	 * @param coolOffErrorStatus      HTTP status to set when the cooloff period is still active
	 * @param enforceNonCyclicalLimit Whether reaching the non-cyclical (permanent) limit is itself an error
	 */
	record RetakeValidationOptions(HttpStatus coolOffErrorStatus, boolean enforceNonCyclicalLimit) {
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
	 * @param options               Caller-specific error reporting behaviour
	 * @return Number of attempts consumed (0 if a new cycle starts after cooloff)
	 */
	int calculateRetakeAttemptsConsumed(String userId, String assessmentIdentifier,
										Map<String, Object> assessmentAllDetail,
										int retakeAttemptsAllowed,
										List<Map<String, Object>> existingDataList,
										SBApiResponse response,
										RetakeValidationOptions options);

	/**
	 * Validates that every question id the user submitted belongs to the stored question set for
	 * the user's existing assessment attempt.
	 *
	 * @return empty string when valid, otherwise an error message
	 */
	String validateIfQuestionIdsAreSame(List<Map<String, Object>> sectionListFromSubmitRequest,
										List<String> desiredKeys, Map<String, Object> existingAssessmentData)
			throws IOException;

	/** Flattens the child-node ids declared by every section of the stored question set. */
	List<Object> collectHierarchyQuestionIds(Map<String, Object> questionSetFromAssessment);

	/** Flattens the question ids the user actually submitted, across every section of the request. */
	List<Object> collectSubmittedQuestionIds(List<Map<String, Object>> sectionListFromSubmitRequest,
											 List<String> desiredKeys);

	/**
	 * The stored assessment start time, which Cassandra may hand back as a {@link java.util.Date}, an
	 * {@link Instant} or an ISO-8601 string.
	 *
	 * @return the start time, or null when the stored value is none of those.
	 */
	Instant resolveAssessmentStartTimeAsInstant(Object startTimeObj);

	/**
	 * Reads a user's latest submitted assessment result, common to the read-result APIs of both
	 * V4 and V5.
	 */
	SBApiResponse readAssessmentResult(Map<String, Object> request, String userAuthToken,
									   AccessTokenValidator accessTokenValidator);

	/**
	 * Builds the assessment-level filtered detail map shared by the read-assessment flows of V2,
	 * V4 and V5. Section-level resolution differs per API version, so the caller supplies its own
	 * {@code readSectionLevelParams} as {@code sectionLevelParamsResolver}.
	 */
	Map<String, Object> readAssessmentLevelData(Map<String, Object> assessmentAllDetail,
			BiConsumer<Map<String, Object>, Map<String, Object>> sectionLevelParamsResolver);

	/**
	 * Populates {@code res} with the assessment-level final result fields, common to V2 and V4.
	 * Mutates {@code res} in place so a partially built map is preserved if the caller's own
	 * catch block needs to return it after a mid-build failure.
	 */
	void populateAssessmentFinalResults(Map<String, Object> assessmentLevelResult, Map<String, Object> res);

	/**
	 * Populates {@code res} with the section-level final result fields, common to V2 and V4.
	 * Mutates {@code res} in place so a partially built map is preserved if the caller's own
	 * catch block needs to return it after a mid-build failure.
	 */
	void populateSectionFinalResults(List<Map<String, Object>> sectionLevelResults, Map<String, Object> res);

	/**
	 * Builds the section-level result map for a scored section, common to V2 and V4.
	 */
	Map<String, Object> createResponseMapWithProperStructure(Map<String, Object> hierarchySection,
			Map<String, Object> resultMap);
}
