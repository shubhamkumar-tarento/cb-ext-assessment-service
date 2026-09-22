package com.igot.cb.assessment.service;


import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.core.exception.ApplicationLogicError;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

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
}
