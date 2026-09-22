package com.igot.cb.assessment.repo;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class UserQuizMasterModelTest {

    @Test
    void testNoArgsConstructorAndSettersGetters() {
        UserQuizMasterPrimaryKeyModel pk = new UserQuizMasterPrimaryKeyModel();
        Integer correct = 5, incorrect = 2, notAnswered = 1;
        Date date = new Date();
        BigDecimal passPercent = new BigDecimal("75.5");
        String sourceId = "src1", sourceTitle = "Quiz Title", userId = "user1";

        UserQuizMasterModel model = new UserQuizMasterModel();
        model.setPrimaryKey(pk);
        model.setCorrectCount(correct);
        model.setDateCreated(date);
        model.setIncorrectCount(incorrect);
        model.setNotAnsweredCount(notAnswered);
        model.setPassPercent(passPercent);
        model.setSourceId(sourceId);
        model.setSourceTitle(sourceTitle);
        model.setUserId(userId);

        assertEquals(pk, model.getPrimaryKey());
        assertEquals(correct, model.getCorrectCount());
        assertEquals(date, model.getDateCreated());
        assertEquals(incorrect, model.getIncorrectCount());
        assertEquals(notAnswered, model.getNotAnsweredCount());
        assertEquals(passPercent, model.getPassPercent());
        assertEquals(sourceId, model.getSourceId());
        assertEquals(sourceTitle, model.getSourceTitle());
        assertEquals(userId, model.getUserId());
    }

    @Test
    void testAllArgsConstructorAndToString() {
        UserQuizMasterPrimaryKeyModel pk = new UserQuizMasterPrimaryKeyModel();
        Integer correct = 3, incorrect = 1, notAnswered = 0;
        Date date = new Date();
        BigDecimal passPercent = new BigDecimal("80.0");
        String sourceId = "src2", sourceTitle = "Another Quiz", userId = "user2";

        UserQuizMasterModel model = UserQuizMasterModel.builder()
                .primaryKey(pk)
                .correctCount(correct)
                .dateCreated(date)
                .incorrectCount(incorrect)
                .notAnsweredCount(notAnswered)
                .passPercent(passPercent)
                .sourceId(sourceId)
                .sourceTitle(sourceTitle)
                .userId(userId)
                .build();

        assertEquals(pk, model.getPrimaryKey());
        assertEquals(correct, model.getCorrectCount());
        assertEquals(date, model.getDateCreated());
        assertEquals(incorrect, model.getIncorrectCount());
        assertEquals(notAnswered, model.getNotAnsweredCount());
        assertEquals(passPercent, model.getPassPercent());
        assertEquals(sourceId, model.getSourceId());
        assertEquals(sourceTitle, model.getSourceTitle());
        assertEquals(userId, model.getUserId());
        assertTrue(model.toString().contains("UserQuizMasterModel"));
    }
}
