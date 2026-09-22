package com.igot.cb.assessment.service;


import java.io.IOException;
import java.util.Map;
import com.igot.cb.common.model.SBApiResponse;

public interface AssessmentServiceV2 {
	/**
	 * submits an assessment
	 *
	 * @param data
	 * @return
	 * @throws IOException if the assessment hierarchy cannot be read
	 */
	SBApiResponse submitAssessment(Map<String, Object> data, String userEmail,boolean editMode) throws IOException;

	SBApiResponse readAssessment(String assessmentIdentifier, String token);

	SBApiResponse readQuestionList(Map<String, Object> requestBody, String authUserToken);

	SBApiResponse retakeAssessment(String assessmentIdentifier, String token);
}
