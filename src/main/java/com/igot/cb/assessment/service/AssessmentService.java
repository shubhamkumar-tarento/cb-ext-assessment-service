package com.igot.cb.assessment.service;

import com.igot.cb.assessment.dto.AssessmentSubmissionDTO;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

public interface AssessmentService {

    Map<String, Object> submitAssessment(String rootOrg, AssessmentSubmissionDTO data, String userEmail)
            throws IOException, ParseException;

    Map<String, Object> getAssessmentByContentUser(String rootOrg, String courseId, String userId);

    Map<String, Object> submitAssessmentByIframe(String rootOrg, Map<String, Object> request);

    Map<String, Object> getAssessmentContent(String courseId, String assessmentContentId);
}
