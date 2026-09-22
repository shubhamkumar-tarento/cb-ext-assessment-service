package com.igot.cb.assessment.service;

import com.igot.cb.assessment.model.QuestionSet;
import com.igot.cb.assessment.model.Questions;
import com.igot.cb.core.exception.ApplicationLogicError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.util.ObjectUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class AssessmentUtilServiceImplTest {

    private AssessmentUtilServiceImpl utilService;

    @BeforeEach
    void setUp() {
        utilService = new AssessmentUtilServiceImpl();
    }

    @Test
    void testValidateAssessment_MCQ_SCA_Correct() {
        Map<String, Object> option = new HashMap<>();
        option.put("optionId", "opt1");
        option.put("isCorrect", true);
        option.put("userSelected", true);

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q1");
        question.put("questionType", "mcq-sca");
        question.put("options", List.of(option));

        List<Map<String, Object>> questions = List.of(question);

        Map<String, Object> result = utilService.validateAssessment(questions);
        assertEquals(1, result.get("correct"));
        assertEquals(0, result.get("incorrect"));
        assertEquals(0, result.get("blank"));
        assertEquals(100.0, (Double) result.get("result"), 0.01);
    }

    @Test
    void testValidateAssessment_MCQ_MCA_Incorrect() {
        Map<String, Object> option1 = new HashMap<>();
        option1.put("optionId", "opt1");
        option1.put("isCorrect", true);
        option1.put("userSelected", false);

        Map<String, Object> option2 = new HashMap<>();
        option2.put("optionId", "opt2");
        option2.put("isCorrect", false);
        option2.put("userSelected", true);

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q2");
        question.put("questionType", "mcq-mca");
        question.put("options", List.of(option1, option2));

        List<Map<String, Object>> questions = List.of(question);

        Map<String, Object> result = utilService.validateAssessment(questions);
        assertEquals(0, result.get("correct"));
        assertEquals(1, result.get("incorrect"));
        assertEquals(0, result.get("blank"));
        assertEquals(0.0, (Double) result.get("result"), 0.01);
    }

    @Test
    void testValidateAssessment_FITB_Blank() {
        Map<String, Object> option = new HashMap<>();
        option.put("optionId", "opt1");
        option.put("isCorrect", true);
        option.put("text", "answer");
        // No response

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q3");
        question.put("questionType", "fitb");
        question.put("options", List.of(option));

        List<Map<String, Object>> questions = List.of(question);

        Map<String, Object> result = utilService.validateAssessment(questions);
        assertEquals(0, result.get("correct"));
        assertEquals(0, result.get("incorrect"));
        assertEquals(1, result.get("blank"));
        assertEquals(0.0, (Double) result.get("result"), 0.01);
    }

    @Test
    void testValidateAssessment_MTF_Correct() {
        Map<String, Object> option = new HashMap<>();
        option.put("optionId", "opt1");
        option.put("isCorrect", true);
        option.put("text", "left");
        option.put("match", "right");
        option.put("response", "right");

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q4");
        question.put("questionType", "mtf");
        question.put("options", List.of(option));

        List<Map<String, Object>> questions = List.of(question);

        Map<String, Object> result = utilService.validateAssessment(questions);
        assertEquals(1, result.get("correct"));
        assertEquals(0, result.get("incorrect"));
        assertEquals(0, result.get("blank"));
        assertEquals(100.0, (Double) result.get("result"), 0.01);
    }

    @Test
    void testValidateAssessment_MTF_Incorrect() {
        Map<String, Object> option = new HashMap<>();
        option.put("optionId", "opt1");
        option.put("isCorrect", true);
        option.put("text", "left");
        option.put("match", "right");
        option.put("response", "wrong");

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q5");
        question.put("questionType", "mtf");
        question.put("options", List.of(option));

        List<Map<String, Object>> questions = List.of(question);

        Map<String, Object> result = utilService.validateAssessment(questions);
        assertEquals(0, result.get("correct"));
        assertEquals(1, result.get("incorrect"));
        assertEquals(0, result.get("blank"));
        assertEquals(0.0, (Double) result.get("result"), 0.01);
    }

    @Test
    void testValidateAssessment_MissingQuestionType() {
        Map<String, Object> option = new HashMap<>();
        option.put("optionId", "opt1");
        option.put("isCorrect", true);
        option.put("userSelected", true);

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q6");
        question.put("options", List.of(option));

        List<Map<String, Object>> questions = List.of(question);

        Map<String, Object> result = utilService.validateAssessment(questions);
        assertEquals(1, result.get("correct"));
        assertEquals(0, result.get("incorrect"));
        assertEquals(0, result.get("blank"));
    }

    @Test
    void testValidateAssessment_EmptyQuestions() {
        List<Map<String, Object>> questions = new ArrayList<>();
        Map<String, Object> result = utilService.validateAssessment(questions);
        assertEquals(0, result.get("correct"));
        assertEquals(0, result.get("incorrect"));
        assertEquals(0, result.get("blank"));
        assertTrue(Double.isNaN((Double) result.get("result")));
    }

    @Test
    void testValidateAssessment_Exception() {
        // Malformed question to trigger exception
        List<Map<String, Object>> questions = new ArrayList<>();
        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q7");
        question.put("questionType", "mcq-sca");
        // options is not a list
        question.put("options", "notalist");
        questions.add(question);

        assertThrows(ApplicationLogicError.class, () -> utilService.validateAssessment(questions));
    }

    @Test
    void testValidateAssessment_WithAnswers_Overload() {
        Map<String, Object> option = new HashMap<>();
        option.put("optionId", "opt1");
        option.put("userSelected", true);

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q8");
        question.put("questionType", "mcq-sca");
        question.put("options", List.of(option));

        List<Map<String, Object>> questions = List.of(question);
        Map<String, Object> answers = new HashMap<>();
        answers.put("q8", List.of("opt1"));

        Map<String, Object> result = utilService.validateAssessment(questions, answers);
        assertEquals(1, result.get("correct"));
    }

    @Test
    void testValidateAssessment_WithAnswers_Blank() {
        Map<String, Object> option = new HashMap<>();
        option.put("optionId", "opt1");
        option.put("userSelected", false);

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q9");
        question.put("questionType", "mcq-sca");
        question.put("options", List.of(option));

        List<Map<String, Object>> questions = List.of(question);
        Map<String, Object> answers = new HashMap<>();
        answers.put("q9", List.of("opt1"));

        Map<String, Object> result = utilService.validateAssessment(questions, answers);
        assertEquals(0, result.get("correct"));
        assertEquals(0, result.get("incorrect"));
        assertEquals(1, result.get("blank"));
    }

    @Test
    void testValidateAssessment_WithAnswers_Exception() {
        List<Map<String, Object>> questions = new ArrayList<>();
        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q10");
        question.put("questionType", "mcq-sca");
        question.put("options", "notalist");
        questions.add(question);

        Map<String, Object> answers = new HashMap<>();
        answers.put("q10", List.of("opt1"));

        assertThrows(ApplicationLogicError.class, () -> utilService.validateAssessment(questions, answers));
    }

    @Test
    void testRemoveAssessmentAnsKey_RemovesIsCorrect() {
        // Prepare QuestionSet with options containing isCorrect
        Questions q = new Questions();
        q.setQuestionType("mcq-sca");
        Map<String, Object> opt = new HashMap<>();
        opt.put("optionId", "opt1");
        opt.put("isCorrect", true);
        q.setOptions(new ArrayList<>(List.of(opt)));
        QuestionSet qs = new QuestionSet();
        qs.setQuestions(new ArrayList<>(List.of(q)));

        QuestionSet result = utilService.removeAssessmentAnsKey(qs);
        assertFalse(result.getQuestions().get(0).getOptions().get(0).containsKey("isCorrect"));
    }

    @Test
    void testRemoveAssessmentAnsKey_EmptyOptions() {
        Questions q = new Questions();
        q.setQuestionType("mcq-sca");
        q.setOptions(new ArrayList<>());
        QuestionSet qs = new QuestionSet();
        qs.setQuestions(new ArrayList<>(List.of(q)));

        QuestionSet result = utilService.removeAssessmentAnsKey(qs);
        assertTrue(ObjectUtils.isEmpty(result.getQuestions().get(0).getOptions()));
    }

    @Test
    void testGetAnswerKeyForAssessmentAuthoringPreview_ReturnsEmptyMap() {
        Map<String, Object> contentMeta = new HashMap<>();
        Map<String, Object> answerKey = utilService.getAnswerKeyForAssessmentAuthoringPreview(contentMeta);
        assertNotNull(answerKey);
        assertTrue(answerKey.isEmpty());
    }

    @Test
    void testGetAnswerKeyForAssessmentAuthoringPreview_WithNonEmptyInput() {
        Map<String, Object> contentMeta = new HashMap<>();
        contentMeta.put("dummyKey", "dummyValue");
        Map<String, Object> answerKey = utilService.getAnswerKeyForAssessmentAuthoringPreview(contentMeta);
        assertNotNull(answerKey);
        assertTrue(answerKey.isEmpty());
    }

    @Test
    void testValidateAssessment_WithAnswers_MissingQuestionId() {
        Map<String, Object> option = new HashMap<>();
        option.put("optionId", "opt1");
        option.put("userSelected", true);

        Map<String, Object> question = new HashMap<>();
        question.put("questionId", "q11");
        question.put("questionType", "mcq-sca");
        question.put("options", List.of(option));

        List<Map<String, Object>> questions = List.of(question);
        Map<String, Object> answers = new HashMap<>(); // Missing "q11" key

        assertThrows(ApplicationLogicError.class, () -> utilService.validateAssessment(questions, answers));
    }


}
