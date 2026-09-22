package com.igot.cb.assessment.repo;

import java.text.ParseException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface AssessmentRepository {

    boolean addUserAssesmentDataToDB(String userId, String assessmentId, Instant startTime, Instant endTime,
                                     Map<String, Object> questionSet, String status);

    Boolean updateUserAssesmentDataToDB(String userId, String assessmentIdentifier,
                                        Map<String, Object> submitAssessmentRequest, Map<String, Object> submitAssessmentResponse, String status,
                                        Instant startTime, Map<String, Object> saveSubmitAssessmentRequest);


    /**
     * inserts quiz or assessments for a user
     *
     * @param persist
     * @param isAssessment
     * @return
     * @throws ParseException if the creation timestamp cannot be parsed
     */
    Map<String, Object> insertQuizOrAssessment(Map<String, Object> persist, Boolean isAssessment)
            throws ParseException;

    /**
     * gets assessment for a user given a content id
     *
     * @param courseId
     * @param userId
     * @return
     */
    List<Map<String, Object>> getAssessmentbyContentUser(String rootOrg, String courseId, String userId);

    List<Map<String, Object>> fetchUserAssessmentDataFromDB(String userId, String assessmentIdentifier);


}
