package com.igot.cb.assessment.repo;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.igot.cb.cassandra.utils.CassandraOperation;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.Constants;
import com.igot.cb.common.util.InstantTypeAdapter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
@Service
public class AssessmentRepositoryImpl implements AssessmentRepository {

    public static final String ROOT_ORG = "rootOrg";
    public static final String RESULT = "result";
    public static final String SOURCE_ID = "sourceId";
    public static final String USER_ID = "userId";

    CassandraOperation cassandraOperation;

    UserAssessmentSummaryRepository userAssessmentSummaryRepo;

    UserAssessmentMasterRepository userAssessmentMasterRepo;

    UserQuizMasterRepository userQuizMasterRepo;

    public AssessmentRepositoryImpl(CassandraOperation cassandraOperation, UserAssessmentSummaryRepository userAssessmentSummaryRepo, UserAssessmentMasterRepository userAssessmentMasterRepo, UserQuizMasterRepository userQuizMasterRepo) {
        this.cassandraOperation = cassandraOperation;
        this.userAssessmentSummaryRepo = userAssessmentSummaryRepo;
        this.userAssessmentMasterRepo = userAssessmentMasterRepo;
        this.userQuizMasterRepo = userQuizMasterRepo;
    }

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public boolean addUserAssesmentDataToDB(String userId, String assessmentIdentifier, Instant startTime,
                                            Instant endTime, Map<String, Object> questionSet, String status) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(Instant.class, new InstantTypeAdapter())
                .create();
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.USER_ID, userId);
        request.put(Constants.ASSESSMENT_ID_KEY, assessmentIdentifier);
        request.put(Constants.START_TIME, startTime);
        request.put(Constants.END_TIME, endTime);
        request.put(Constants.ASSESSMENT_READ_RESPONSE, gson.toJson(questionSet));
        request.put(Constants.STATUS, status);
        SBApiResponse resp = cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_USER_ASSESSMENT_DATA, request);
        Map<String, Object> result = (resp == null) ? null : resp.getResult();
        Object responseVal = MapUtils.isEmpty(result) ? null : result.get("STATUS");
        return Constants.SUCCESS.equalsIgnoreCase(Objects.toString(responseVal, null));
    }


    @Override
    public Boolean updateUserAssesmentDataToDB(String userId, String assessmentIdentifier,
                                               Map<String, Object> submitAssessmentRequest, Map<String, Object> submitAssessmentResponse, String status,
                                               Instant startTime,Map<String, Object> saveSubmitAssessmentRequest) {
        Map<String, Object> compositeKeys = new HashMap<>();
        compositeKeys.put(Constants.USER_ID, userId);
        compositeKeys.put(Constants.ASSESSMENT_ID_KEY, assessmentIdentifier);
        compositeKeys.put(Constants.START_TIME, startTime);
        Map<String, Object> fieldsToBeUpdated = new HashMap<>();
        if (MapUtils.isNotEmpty(submitAssessmentRequest)) {
            fieldsToBeUpdated.put("submitassessmentrequest", new Gson().toJson(submitAssessmentRequest));
        }
        if (MapUtils.isNotEmpty(submitAssessmentResponse)) {
            fieldsToBeUpdated.put("submitassessmentresponse", new Gson().toJson(submitAssessmentResponse));
        }
        if (StringUtils.isNotBlank(status)) {
            fieldsToBeUpdated.put(Constants.STATUS, status);
        }
        if (MapUtils.isNotEmpty(saveSubmitAssessmentRequest)) {
            fieldsToBeUpdated.put("savepointsubmitreq", new Gson().toJson(saveSubmitAssessmentRequest));
        }
        if (submitAssessmentRequest.get(Constants.LANGUAGE) instanceof String language &&
                StringUtils.isNotBlank(language)) {
            fieldsToBeUpdated.put(Constants.LANGUAGE, language.toLowerCase());
        }
        cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_ASSESSMENT_DATA,
                fieldsToBeUpdated, compositeKeys);
        return true;
    }

    @Override
    public Map<String, Object> insertQuizOrAssessment(Map<String, Object> persist, Boolean isAssessment)
            throws ParseException {
        Map<String, Object> response = new HashMap<>();
        Date date = new Date();

        // insert assessment and assessment summary
        if (Boolean.TRUE.equals(isAssessment)) {
            UserAssessmentMasterModel assessment = UserAssessmentMasterModel.builder()
                    .primaryKey(new UserAssessmentMasterPrimaryKeyModel(persist.get(ROOT_ORG).toString(), date,
                            persist.get("parent").toString(), BigDecimal.valueOf((Double) persist.get(RESULT)),
                            UuidCreator.getTimeBased()))
                    .correctCount(Integer.parseInt(persist.get("correct").toString()))
                    .dateCreated(formatter.parse(formatter.format(date)))
                    .incorrectCount(Integer.parseInt(persist.get("incorrect").toString()))
                    .notAnsweredCount(Integer.parseInt(persist.get("blank").toString()))
                    .parentContentType(persist.get("parentContentType").toString())
                    .passPercent(new BigDecimal(60))
                    .sourceId(persist.get(SOURCE_ID).toString())
                    .sourceTitle(persist.get("title").toString())
                    .userId(persist.get(USER_ID).toString())
                    .build();
            UserAssessmentSummaryModel summary = new UserAssessmentSummaryModel();
            UserAssessmentSummaryModel data = userAssessmentSummaryRepo
                    .findById(new UserAssessmentSummaryPrimaryKeyModel(persist.get(ROOT_ORG).toString(),
                            persist.get(USER_ID).toString(), persist.get(SOURCE_ID).toString()))
                    .orElse(null);

            if (persist.get("parentContentType").toString().equalsIgnoreCase("course")) {
                if (data != null) {
                    if (data.getFirstMaxScore() < Float.parseFloat(persist.get(RESULT).toString())) {
                        summary = new UserAssessmentSummaryModel(
                                new UserAssessmentSummaryPrimaryKeyModel(persist.get(ROOT_ORG).toString(),
                                        persist.get(USER_ID).toString(), persist.get(SOURCE_ID).toString()),
                                Float.parseFloat(persist.get(RESULT).toString()), date, data.getFirstPassesScore(),
                                data.getFirstPassesScoreDate());
                    }
                } else if (Float.parseFloat(persist.get(RESULT).toString()) > Constants.ASSESSMENT_PASS_SCORE) {
                    summary = new UserAssessmentSummaryModel(
                            new UserAssessmentSummaryPrimaryKeyModel(persist.get(ROOT_ORG).toString(),
                                    persist.get(USER_ID).toString(), persist.get(SOURCE_ID).toString()),
                            Float.parseFloat(persist.get(RESULT).toString()), date,
                            Float.parseFloat(persist.get(RESULT).toString()), date);
                } else {
                    summary = new UserAssessmentSummaryModel(
                            new UserAssessmentSummaryPrimaryKeyModel(persist.get(ROOT_ORG).toString(),
                                    persist.get(USER_ID).toString(), persist.get(SOURCE_ID).toString()),
                            Float.parseFloat(persist.get(RESULT).toString()), date, null, null);
                    userAssessmentSummaryRepo.save(summary);

                }
            }
            userAssessmentMasterRepo.updateAssessment(assessment, summary);
        }
        // insert quiz and quiz summary
        else {
            UserQuizMasterModel quiz = UserQuizMasterModel.builder()
                    .primaryKey(new UserQuizMasterPrimaryKeyModel(persist.get(ROOT_ORG).toString(), date,
                            BigDecimal.valueOf((Double) persist.get(RESULT)), UuidCreator.getTimeBased()))
                    .correctCount(Integer.parseInt(persist.get("correct").toString()))
                    .dateCreated(formatter.parse(formatter.format(date)))
                    .incorrectCount(Integer.parseInt(persist.get("incorrect").toString()))
                    .notAnsweredCount(Integer.parseInt(persist.get("blank").toString()))
                    .passPercent(new BigDecimal(60))
                    .sourceId(persist.get(SOURCE_ID).toString())
                    .sourceTitle(persist.get("title").toString())
                    .userId(persist.get(USER_ID).toString())
                    .build();
            UserQuizSummaryModel summary = new UserQuizSummaryModel(
                    new UserQuizSummaryPrimaryKeyModel(persist.get(ROOT_ORG).toString(),
                            persist.get(USER_ID).toString(), persist.get(SOURCE_ID).toString()),
                    date);

            userQuizMasterRepo.updateQuiz(quiz, summary);
        }

        response.put("response", "SUCCESS");
        return response;
    }

    @Override
    public List<Map<String, Object>> getAssessmentbyContentUser(String rootOrg, String courseId, String userId) {
        return Collections.emptyList();
    }

    @Override
    public List<Map<String, Object>> fetchUserAssessmentDataFromDB(String userId, String assessmentIdentifier) {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.USER_ID, userId);
        request.put(Constants.ASSESSMENT_ID_KEY, assessmentIdentifier);
        return cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_ASSESSMENT_DATA, request, null);
    }

}
