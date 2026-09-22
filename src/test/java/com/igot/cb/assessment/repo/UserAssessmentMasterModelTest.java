package com.igot.cb.assessment.repo;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class UserAssessmentMasterModelTest {

    @Test
    void testNoArgsConstructorAndSettersAndGetters() {
        UserAssessmentMasterModel model = new UserAssessmentMasterModel();

        UserAssessmentMasterPrimaryKeyModel pk = new UserAssessmentMasterPrimaryKeyModel();
        model.setPrimaryKey(pk);
        model.setCorrectCount(5);
        Date now = new Date();
        model.setDateCreated(now);
        model.setIncorrectCount(2);
        model.setNotAnsweredCount(1);
        model.setParentContentType("course");
        model.setPassPercent(new BigDecimal("75.5"));
        model.setSourceId("src123");
        model.setSourceTitle("Assessment Title");
        model.setUserId("user1");

        assertEquals(pk, model.getPrimaryKey());
        assertEquals(5, model.getCorrectCount());
        assertEquals(now, model.getDateCreated());
        assertEquals(2, model.getIncorrectCount());
        assertEquals(1, model.getNotAnsweredCount());
        assertEquals("course", model.getParentContentType());
        assertEquals(new BigDecimal("75.5"), model.getPassPercent());
        assertEquals("src123", model.getSourceId());
        assertEquals("Assessment Title", model.getSourceTitle());
        assertEquals("user1", model.getUserId());
    }

    @Test
    void testAllArgsConstructor() {
        UserAssessmentMasterPrimaryKeyModel pk = new UserAssessmentMasterPrimaryKeyModel();
        Date now = new Date();
        BigDecimal percent = new BigDecimal("80.0");

        UserAssessmentMasterModel model = UserAssessmentMasterModel.builder()
                .primaryKey(pk)
                .correctCount(10)
                .dateCreated(now)
                .incorrectCount(3)
                .notAnsweredCount(0)
                .parentContentType("module")
                .passPercent(percent)
                .sourceId("srcId")
                .sourceTitle("Title")
                .userId("userX")
                .build();

        assertEquals(pk, model.getPrimaryKey());
        assertEquals(10, model.getCorrectCount());
        assertEquals(now, model.getDateCreated());
        assertEquals(3, model.getIncorrectCount());
        assertEquals(0, model.getNotAnsweredCount());
        assertEquals("module", model.getParentContentType());
        assertEquals(percent, model.getPassPercent());
        assertEquals("srcId", model.getSourceId());
        assertEquals("Title", model.getSourceTitle());
        assertEquals("userX", model.getUserId());
    }

    @Test
    void testToString() {
        UserAssessmentMasterPrimaryKeyModel pk = new UserAssessmentMasterPrimaryKeyModel();
        UserAssessmentMasterModel model = UserAssessmentMasterModel.builder()
                .primaryKey(pk)
                .correctCount(1)
                .dateCreated(new Date(0))
                .incorrectCount(2)
                .notAnsweredCount(3)
                .parentContentType("type")
                .passPercent(new BigDecimal("50"))
                .sourceId("sid")
                .sourceTitle("stitle")
                .userId("uid")
                .build();
        String str = model.toString();
        assertTrue(str.contains("primaryKey="));
        assertTrue(str.contains("correctCount=1"));
        assertTrue(str.contains("incorrectCount=2"));
        assertTrue(str.contains("notAnsweredCount=3"));
        assertTrue(str.contains("parentContentType=type"));
        assertTrue(str.contains("passPercent=50"));
        assertTrue(str.contains("sourceId=sid"));
        assertTrue(str.contains("sourceTitle=stitle"));
        assertTrue(str.contains("userId=uid"));
    }
}
