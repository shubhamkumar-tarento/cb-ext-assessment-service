package com.igot.cb.common.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class CbExtAssessmentServerPropertiesTest {

    private static final Pattern ACCESSOR_PREFIX = Pattern.compile("^(get|is)");

    private CbExtAssessmentServerProperties props;

    @BeforeEach
    void setUp() {
        props = new CbExtAssessmentServerProperties();
    }

    @Test
    void testBasicGettersSetters() {
        props.setCourseReadPath("/course/read");
        assertEquals("/course/read", props.getCourseReadPath());

        props.setRedisDataHostName("localhost");
        assertEquals("localhost", props.getRedisDataHostName());

        props.setRedisDataPort("6379");
        assertEquals("6379", props.getRedisDataPort());

        props.setRedisWheeboxKey("wheebox");
        assertEquals("wheebox", props.getRedisWheeboxKey());

        props.setStateLearningInsightsRedisKeyMapping("state_key");
        assertEquals("state_key", props.getStateLearningInsightsRedisKeyMapping());

        props.setqListFromCacheEnabled(true);
        assertTrue(props.qListFromCacheEnabled());

        props.setRedisQuestionsReadTimeOut(100);
        assertEquals(100, props.getRedisQuestionsReadTimeOut());

        props.setBatchEnrolmentReturnSize(20);
        assertEquals(20, props.getBatchEnrolmentReturnSize());

        props.setSbOTPGeneratePath("/otp/generate");
        assertEquals("/otp/generate", props.getSbOTPGeneratePath());

        props.setInsightsLabelCertificatesAcross("cert_across");
        assertEquals("cert_across", props.getInsightsLabelCertificatesAcross());

        props.setUserMultiMapDeptEnabled(true);
        assertTrue(props.isUserMultiMapDeptEnabled());

        props.setVersion("1.0.0");
        assertEquals("1.0.0", props.getVersion());
        assertEquals("1.0.0", props.getSunbirdCbExtVersion());

        props.setSbUrl("https://sunbird.org");
        props.setSbHubGraphServiceUrl("https://graph.sunbird.org");
        props.setWfServiceHost("wf-host");
        props.setWfServicePath("/workflow");

        String expected = "SB-CB-Ext Server Properties: " +
            "[wfServiceHost=wf-host]," +
            "[wfServicePath=/workflow]," +
            "[isUserMultiMapDeptEnabled=true]," +
            "[sbUrl=https://sunbird.org]," +
            "[sbHubGraphServiceUrl=https://graph.sunbird.org]";

        assertEquals(expected, props.toString());
    }

    @Test
    void testListParsing() {
        props.setAssessmentLevelParams("param1,param2,param3");
        List<String> levelParams = props.getAssessmentLevelParams();
        assertEquals(3, levelParams.size());
        assertTrue(levelParams.contains("param1"));

        props.setAssessmentSectionParams("sec1,,sec2");
        List<String> sectionParams = props.getAssessmentSectionParams();
        assertEquals(3, sectionParams.size());

        props.setAssessmentQuestionParams("");
        assertEquals(1, props.getAssessmentQuestionParams().size());
        assertEquals("", props.getAssessmentQuestionParams().get(0));

        props.setAssessmentMinQuestionParams("min1");
        assertEquals(List.of("min1"), props.getAssessmentMinQuestionParams());

        props.setUserRegistrationDomain("domain1,domain2");
        assertEquals(List.of("domain1", "domain2"), props.getUserRegistrationDomain());

        props.setUserRegistrationDeptExcludeList("dept1,dept2");
        assertEquals(List.of("dept1", "dept2"), props.getUserRegistrationDeptExcludeList());
    }

    @Test
    void testScalarPropertyRoundTrip() {
        props.setInsightsLabelLearningHoursAcross("AcrossDept");
        props.setInsightsLabelCertificatesYourDepartment("CertsDept");
        props.setInsightsLabelLearningHoursYourDepartment("HoursDept");
        props.setRedisInsightIndex(7);
        props.setAssessmentSubmitTopic("submitTopic");
        props.setUserAssessmentSubmissionDuration("45m");
        props.setContentHost("https://content.example.com");
        props.setHierarchyEndPoint("/api/hierarchy");
        props.setWfServiceTransitionPath("/workflow/transition");

        // Assert getters return expected values
        assertEquals("AcrossDept", props.getInsightsLabelLearningHoursAcross());
        assertEquals("CertsDept", props.getInsightsLabelCertificatesYourDepartment());
        assertEquals("HoursDept", props.getInsightsLabelLearningHoursYourDepartment());
        assertEquals(7, props.getRedisInsightIndex());
        assertEquals("submitTopic", props.getAssessmentSubmitTopic());
        assertEquals("45m", props.getUserAssessmentSubmissionDuration());
        assertEquals("https://content.example.com", props.getContentHost());
        assertEquals("/api/hierarchy", props.getHierarchyEndPoint());
        assertEquals("/workflow/transition", props.getWfServiceTransitionPath());

        // Access default (uninitialized) getters to increase coverage
        assertNull(props.getPublicUserAssessmentTableName());
        assertNull(props.getSpringKafkaPublicAssessmentNotificationTopicName());
        assertNull(props.getPublicAssessmentEncryptionKey());
        assertNull(props.getPublicAssessmentCloudCertificateFolderName());
        assertNull(props.getCiosCloudIconFolderName());
        assertNull(props.getWfServiceHost());
        assertNull(props.getLmsUserUpdatePrivatePath());
    }

    @Test
    void testScalarPropertyOverwrite() {
        props.setInsightsLabelLearningHoursAcross("LabelAcross");
        props.setInsightsLabelCertificatesYourDepartment("CertDept");
        props.setInsightsLabelLearningHoursYourDepartment("HoursDept");
        props.setRedisInsightIndex(5);
        props.setAssessmentSubmitTopic("SubmitTopicTest");
        props.setUserAssessmentSubmissionDuration("60");
        props.setContentHost("https://test.content.host");
        props.setHierarchyEndPoint("/api/hierarchy/test");
        props.setWfServiceTransitionPath("/workflow/transition/test");

        // Get and assert values
        assertEquals("LabelAcross", props.getInsightsLabelLearningHoursAcross());
        assertEquals("CertDept", props.getInsightsLabelCertificatesYourDepartment());
        assertEquals("HoursDept", props.getInsightsLabelLearningHoursYourDepartment());
        assertEquals(5, props.getRedisInsightIndex());
        assertEquals("SubmitTopicTest", props.getAssessmentSubmitTopic());
        assertEquals("60", props.getUserAssessmentSubmissionDuration());
        assertEquals("https://test.content.host", props.getContentHost());
        assertEquals("/api/hierarchy/test", props.getHierarchyEndPoint());
        assertEquals("/workflow/transition/test", props.getWfServiceTransitionPath());

        // Also test getters with default/null values (unset fields)
        assertNull(props.getPublicUserAssessmentTableName());
        assertNull(props.getSpringKafkaPublicAssessmentNotificationTopicName());
        assertNull(props.getPublicAssessmentEncryptionKey());
        assertNull(props.getPublicAssessmentCloudCertificateFolderName());
        assertNull(props.getCiosCloudIconFolderName());
        assertNull(props.getWfServiceHost());
        assertNull(props.getLmsUserUpdatePrivatePath());
    }

    @Test
    void testUserRegistrationProperties() {
        props.setUserRegistrationIndex("idx001");
        props.setUserRegCodePrefix("URP");
        props.setUserRegistrationTopic("topic1");
        props.setUserRegistrationAutoCreateUserTopic("autoTopic");
        props.setUserRegistrationDomain("domain1.com,domain2.com");
        props.setUserRegistrationDeptExcludeList("deptA,deptB");
        props.setUserRegistrationWorkFlowServiceName("workflowService");
        props.setUserRegistrationTitle("Mr");
        props.setUserRegistrationStatus("Pending");
        props.setUserRegistrationThankyouMessage("Thanks for registering!");
        props.setUserRegistrationInitiatedMessage("Initiated");
        props.setUserRegistrationApprovedMessage("Approved");
        props.setUserRegistrationFailedMessage("Failed");
        props.setUserRegisterationButtonName("Register Now");
        props.setUserRegistrationSubject("Registration Subject");
        props.setUserRegistrationDomainName("user.domain.com");
        props.setUserRegistrationPreApprovedDomainList("trusted1.com,trusted2.com");

        // Assert all values
        assertEquals("idx001", props.getUserRegistrationIndex());
        assertEquals("URP", props.getUserRegCodePrefix());
        assertEquals("topic1", props.getUserRegistrationTopic());
        assertEquals("autoTopic", props.getUserRegistrationAutoCreateUserTopic());
        assertEquals(List.of("domain1.com", "domain2.com"), props.getUserRegistrationDomain());
        assertEquals(List.of("deptA", "deptB"), props.getUserRegistrationDeptExcludeList());
        assertEquals("workflowService", props.getUserRegistrationWorkFlowServiceName());
        assertEquals("Mr", props.getUserRegistrationTitle());
        assertEquals("Pending", props.getUserRegistrationStatus());
        assertEquals("Thanks for registering!", props.getUserRegistrationThankyouMessage());
        assertEquals("Initiated", props.getUserRegistrationInitiatedMessage());
        assertEquals("Approved", props.getUserRegistrationApprovedMessage());
        assertEquals("Failed", props.getUserRegistrationFailedMessage());
        assertEquals("Register Now", props.getUserRegisterationButtonName());
        assertEquals("Registration Subject", props.getUserRegistrationSubject());
        assertEquals("user.domain.com", props.getUserRegistrationDomainName());
        assertEquals(List.of("trusted1.com", "trusted2.com"), props.getUserRegistrationPreApprovedDomainList());
    }

    @Test
    void testAllSettersAndGettersRoundTrip() throws Exception {
        Map<String, Object> expectedByProperty = new HashMap<>();
        for (Method setter : CbExtAssessmentServerProperties.class.getDeclaredMethods()) {
            if (!Modifier.isPublic(setter.getModifiers()) || !setter.getName().startsWith("set")
                    || setter.getParameterCount() != 1) {
                continue;
            }
            Object value = sampleValue(setter.getParameterTypes()[0]);
            try {
                setter.invoke(props, value);
            } catch (InvocationTargetException e) {
                // Setters such as setMinEvictableIdleTime parse a numeric String into a long field
                assertInstanceOf(NumberFormatException.class, e.getCause(), setter.getName());
                setter.invoke(props, "7");
                value = 7L;
            }
            expectedByProperty.put(setter.getName().substring(3).toLowerCase(), value);
        }
        assertFalse(expectedByProperty.isEmpty());

        int verified = 0;
        for (Method getter : CbExtAssessmentServerProperties.class.getDeclaredMethods()) {
            if (!Modifier.isPublic(getter.getModifiers()) || getter.getParameterCount() != 0
                    || getter.getReturnType() == void.class) {
                continue;
            }
            Object actual = getter.invoke(props);
            assertNotNull(actual, getter.getName());
            String property = ACCESSOR_PREFIX.matcher(getter.getName()).replaceFirst("").toLowerCase();
            Object expected = expectedByProperty.get(property);
            if (expected == null) {
                continue;
            }
            if (actual instanceof List) {
                assertEquals(Arrays.asList(((String) expected).split(",", -1)), actual, getter.getName());
            } else if (actual instanceof String[] && expected instanceof String) {
                assertArrayEquals(((String) expected).split(",", -1), (String[]) actual, getter.getName());
            } else if (actual instanceof String[]) {
                assertArrayEquals((String[]) expected, (String[]) actual, getter.getName());
            } else {
                assertEquals(expected, actual, getter.getName());
            }
            verified++;
        }
        assertTrue(verified > 0);
    }

    @Test
    void testLatestCoursesAlertUserEmailListBlankReturnsEmpty() {
        props.setLatestCoursesAlertUserEmailList("  ");
        assertTrue(props.getLatestCoursesAlertUserEmailList().isEmpty());

        props.setLatestCoursesAlertUserEmailList("a@x.com,b@x.com");
        assertEquals(List.of("a@x.com", "b@x.com"), props.getLatestCoursesAlertUserEmailList());
    }

    private static Object sampleValue(Class<?> type) {
        if (type == String.class) return "a,b";
        if (type == boolean.class || type == Boolean.class) return true;
        if (type == int.class || type == Integer.class) return 7;
        if (type == long.class || type == Long.class) return 7L;
        if (type == char.class || type == Character.class) return 'x';
        if (type == String[].class) return new String[]{"a", "b"};
        if (Map.class.isAssignableFrom(type)) return new HashMap<>(Map.of("k", "v"));
        throw new IllegalArgumentException("No sample value for " + type);
    }

    @Test
    void testDownstreamServiceProperties() {
        props.setDiscussionHubHost("https://hub.example.com");
        props.setDiscussionHubCreateUserPath("/create/user");
        props.setSbResetPasswordPath("/reset");
        props.setSbSendNotificationEmailPath("/send/email");
        props.setSbAssignRolePath("/assign/role");
        props.setMasterOrgListFileName("org-list.csv");
        props.setCustodianOrgId("custodian123");
        props.setCustodianOrgName("Custodian Org Pvt Ltd");

        assertEquals("https://hub.example.com", props.getDiscussionHubHost());
        assertEquals("/create/user", props.getDiscussionHubCreateUserPath());
        assertEquals("/reset", props.getSbResetPasswordPath());
        assertEquals("/send/email", props.getSbSendNotificationEmailPath());
        assertEquals("/assign/role", props.getSbAssignRolePath());
        assertEquals("org-list.csv", props.getMasterOrgListFileName());
        assertEquals("custodian123", props.getCustodianOrgId());
        assertEquals("Custodian Org Pvt Ltd", props.getCustodianOrgName());
    }
}
