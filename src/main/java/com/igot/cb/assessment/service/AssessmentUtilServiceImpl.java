package com.igot.cb.assessment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.assessment.model.QuestionSet;
import com.igot.cb.assessment.model.Questions;
import com.igot.cb.core.exception.ApplicationLogicError;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.*;

@Service
public class AssessmentUtilServiceImpl implements AssessmentUtilService {

	public static final String QUESTION_TYPE = "questionType";
	public static final String OPTIONS = "options";
	public static final String IS_CORRECT = "isCorrect";
	public static final String OPTION_ID = "optionId";
	public static final String MCQ_SCA = "mcq-sca";
	public static final String MCQ_MCA = "mcq-mca";
	public static final String FITB = "fitb";
	public static final String MTF = "mtf";
	public static final String QUESTION_ID = "questionId";
	public static final String RESPONSE = "response";
	public static final String USER_SELECTED = "userSelected";

	/** How a single question scored. */
	private enum QuestionOutcome {
		CORRECT, INCORRECT, BLANK
	}

	private Map<String, Object> getAnswers(List<Map<String, Object>> questions) {
		Map<String, Object> ret = new HashMap<>();

		for (Map<String, Object> question : questions) {
			ret.put(question.get(QUESTION_ID).toString(), collectCorrectOptions(question));
		}

		return ret;
	}

	/**
	 * The answer key for one question, in the shape its type records correct options. A question
	 * that declares no type is treated the same as a multiple-choice one.
	 */
	private List<String> collectCorrectOptions(Map<String, Object> question) {
		List<String> correctOption = new ArrayList<>();
		if (!question.containsKey(QUESTION_TYPE)) {
			collectCorrectOptionIds(question, correctOption);
			return correctOption;
		}
		switch ((String) question.get(QUESTION_TYPE)) {
		case "mtf":
			collectCorrectMatchOptions(question, correctOption);
			break;
		case "fitb":
			collectCorrectTextOptions(question, correctOption);
			break;
		case MCQ_SCA, MCQ_MCA:
			collectCorrectOptionIds(question, correctOption);
			break;
		default:
			break;
		}
		return correctOption;
	}

	/** Correct options recorded as {@code optionId-text-match}. */
	private void collectCorrectMatchOptions(Map<String, Object> question, List<String> correctOption) {
		for (Map<String, Object> options : (List<Map<String, Object>>) question.get(OPTIONS)) {
			if ((boolean) options.get(IS_CORRECT)) {
				correctOption.add(options.get(OPTION_ID).toString() + "-"
						+ options.get("text").toString().toLowerCase() + "-"
						+ options.get("match").toString().toLowerCase());
			}
		}
	}

	/** Correct options recorded as {@code optionId-text}. */
	private void collectCorrectTextOptions(Map<String, Object> question, List<String> correctOption) {
		for (Map<String, Object> options : (List<Map<String, Object>>) question.get(OPTIONS)) {
			if ((boolean) options.get(IS_CORRECT)) {
				correctOption.add(options.get(OPTION_ID).toString() + "-"
						+ options.get("text").toString().toLowerCase());
			}
		}
	}

	/** Correct options recorded as bare option ids. */
	private void collectCorrectOptionIds(Map<String, Object> question, List<String> correctOption) {
		for (Map<String, Object> options : (List<Map<String, Object>>) question.get(OPTIONS)) {
			if ((boolean) options.get(IS_CORRECT)) {
				correctOption.add(options.get(OPTION_ID).toString());
			}
		}
	}

	/**
	 * Options the user responded to, recorded as {@code optionId-response}, or
	 * {@code optionId-text-response} when {@code includeText} is set.
	 */
	private void collectRespondedOptions(Map<String, Object> question, boolean includeText, List<String> marked) {
		for (Map<String, Object> options : (List<Map<String, Object>>) question.get(OPTIONS)) {
			if (options.containsKey(RESPONSE) && !options.get(RESPONSE).toString().isEmpty()) {
				String text = includeText ? options.get("text").toString().toLowerCase() + "-" : "";
				marked.add(options.get(OPTION_ID).toString() + "-" + text
						+ options.get(RESPONSE).toString().toLowerCase());
			}
		}
	}

	/** Ids of the options the user selected. */
	private void collectUserSelectedOptions(Map<String, Object> question, List<String> marked) {
		for (Map<String, Object> options : (List<Map<String, Object>>) question.get(OPTIONS)) {
			if ((boolean) options.get(USER_SELECTED)) {
				marked.add(options.get(OPTION_ID).toString());
			}
		}
	}

	/**
	 * Scores one question by comparing what the user marked against the answer key. Both lists are
	 * sorted first, so the comparison ignores the order options were given in.
	 */
	private QuestionOutcome scoreQuestion(List<String> marked, List<String> answer) {
		if (CollectionUtils.isEmpty(marked)) {
			return QuestionOutcome.BLANK;
		}
		if (answer.size() > 1) {
			Collections.sort(answer);
		}
		if (marked.size() > 1) {
			Collections.sort(marked);
		}
		return answer.equals(marked) ? QuestionOutcome.CORRECT : QuestionOutcome.INCORRECT;
	}

	/** The score summary in the shape callers expect. */
	private Map<String, Object> buildResultMap(int correct, int blank, int inCorrect) {
		Map<String, Object> resultMap = new HashMap<>();
		resultMap.put("result", (correct * 100d) / (correct + blank + inCorrect));
		resultMap.put("incorrect", inCorrect);
		resultMap.put("blank", blank);
		resultMap.put("correct", correct);
		return resultMap;
	}

	public Map<String, Object> validateAssessment(List<Map<String, Object>> questions) {
		try {
			int correct = 0;
			int blank = 0;
			int inCorrect = 0;
			Map<String, Object> answers = getAnswers(questions);
			for (Map<String, Object> question : questions) {
				List<String> marked = collectMarkedOptions(question);
				switch (scoreQuestion(marked, (List<String>) answers.get(question.get(QUESTION_ID)))) {
				case CORRECT -> correct++;
				case INCORRECT -> inCorrect++;
				case BLANK -> blank++;
				}
			}
			return buildResultMap(correct, blank, inCorrect);

		} catch (Exception ex) {
			throw new ApplicationLogicError("Error when verifying assessment. Error : " + ex.getMessage(), ex);
		}
	}

	/** What the user marked on one question, dispatched on an exact question-type match. */
	private List<String> collectMarkedOptions(Map<String, Object> question) {
		List<String> marked = new ArrayList<>();
		if (!question.containsKey(QUESTION_TYPE)) {
			collectUserSelectedOptions(question, marked);
			return marked;
		}
		switch ((String) question.get(QUESTION_TYPE)) {
		case "mtf":
			collectRespondedOptions(question, true, marked);
			break;
		case "fitb":
			collectRespondedOptions(question, false, marked);
			break;
		case MCQ_SCA, MCQ_MCA:
			collectUserSelectedOptions(question, marked);
			break;
		default:
			break;
		}
		return marked;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> validateAssessment(List<Map<String, Object>> questions, Map<String, Object> answers) {
		try {
			int correct = 0;
			int blank = 0;
			int inCorrect = 0;
			for (Map<String, Object> question : questions) {
				List<String> marked = collectMarkedOptionsIgnoringCase(question);
				switch (scoreQuestion(marked, (List<String>) answers.get(question.get(QUESTION_ID)))) {
				case CORRECT -> correct++;
				case INCORRECT -> inCorrect++;
				case BLANK -> blank++;
				}
			}
			return buildResultMap(correct, blank, inCorrect);
		} catch (Exception ex) {
			throw new ApplicationLogicError("Error when verifying assessment. Error : " + ex.getMessage(), ex);
		}

	}

	/**
	 * What the user marked on one question, dispatched on a case-insensitive question-type match.
	 * This overload has always matched the type ignoring case, unlike {@link #collectMarkedOptions}.
	 */
	private List<String> collectMarkedOptionsIgnoringCase(Map<String, Object> question) {
		List<String> marked = new ArrayList<>();
		if (!question.containsKey(QUESTION_TYPE)) {
			collectUserSelectedOptions(question, marked);
			return marked;
		}
		String questionType = question.get(QUESTION_TYPE).toString();
		if (questionType.equalsIgnoreCase("mtf")) {
			collectRespondedOptions(question, true, marked);
		} else if (questionType.equalsIgnoreCase("fitb")) {
			collectRespondedOptions(question, false, marked);
		} else if (questionType.equalsIgnoreCase(MCQ_SCA) || questionType.equalsIgnoreCase(MCQ_MCA)) {
			collectUserSelectedOptions(question, marked);
		}
		return marked;
	}

	/*
	 * This method fetches the answer key for assessment
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> getAnswerKeyForAssessmentAuthoringPreview(Map<String, Object> contentMeta) {
		return Collections.emptyMap();
	}

	/**
	 * To remove answers from the assessment question sets
	 * 
	 * @param assessmentContent
	 *            Object
	 * @return QuestionSet
	 */
	@Override
	public QuestionSet removeAssessmentAnsKey(Object assessmentContent) {
		QuestionSet questionSet = new ObjectMapper().convertValue(assessmentContent, QuestionSet.class);
		List<String> qnsTypes = Arrays.asList(MCQ_MCA, MCQ_SCA, FITB, MTF);
		for (Questions question : questionSet.getQuestions()) {
			if (qnsTypes.contains(question.getQuestionType()) && !ObjectUtils.isEmpty(question.getOptions())) {
				for (Map<String, Object> option : question.getOptions()) {
					option.remove(IS_CORRECT);
				}
			}
		}
		return questionSet;
	}
}
