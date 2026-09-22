package com.igot.cb;


import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CbExtAssessmentServiceApplicationTest {

    private final CbExtAssessmentServiceApplication application = new CbExtAssessmentServiceApplication();

    @Test
    void testMain() {
        try (MockedStatic<SpringApplication> mockedStatic = Mockito.mockStatic(SpringApplication.class)) {
            CbExtAssessmentServiceApplication.main(new String[]{"arg1", "arg2"});
            mockedStatic.verify(() ->
                    SpringApplication.run(eq(CbExtAssessmentServiceApplication.class), eq(new String[]{"arg1", "arg2"}))
            );
        }
    }

    @Test
    void testRestTemplate() {
        RestTemplate restTemplate = application.restTemplate();
        assertNotNull(restTemplate);
        ClientHttpRequestFactory requestFactory = restTemplate.getRequestFactory();
        assertNotNull(requestFactory);
        assertTrue(requestFactory instanceof HttpComponentsClientHttpRequestFactory);
    }

    @Test
    void testGetClientHttpRequestFactory_UsingReflection() throws Exception {
        Method method = CbExtAssessmentServiceApplication.class.getDeclaredMethod("getClientHttpRequestFactory");
        method.setAccessible(true);
        ClientHttpRequestFactory factory = (ClientHttpRequestFactory) method.invoke(application);
        assertNotNull(factory);
        assertTrue(factory instanceof HttpComponentsClientHttpRequestFactory);
        HttpComponentsClientHttpRequestFactory httpFactory = (HttpComponentsClientHttpRequestFactory) factory;
        Field httpClientField = HttpComponentsClientHttpRequestFactory.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        assertNotNull(httpClientField.get(httpFactory));
    }

    @Test
    void testGetClientHttpRequestFactory_UsingSubclass() throws Exception {
        Method method = CbExtAssessmentServiceApplication.class.getDeclaredMethod("getClientHttpRequestFactory");
        method.setAccessible(true);
        ClientHttpRequestFactory factory = (ClientHttpRequestFactory) method.invoke(application);
        assertNotNull(factory);
        assertTrue(factory instanceof HttpComponentsClientHttpRequestFactory);
    }

    @Test
    void testGetClientHttpRequestFactory_ConfigValues() throws Exception {
        Method method = CbExtAssessmentServiceApplication.class.getDeclaredMethod("getClientHttpRequestFactory");
        method.setAccessible(true);
        try (MockedStatic<org.apache.hc.client5.http.impl.classic.HttpClients> httpClientsMock =
                     Mockito.mockStatic(org.apache.hc.client5.http.impl.classic.HttpClients.class)) {
            org.apache.hc.client5.http.impl.classic.HttpClientBuilder builderMock = mock(org.apache.hc.client5.http.impl.classic.HttpClientBuilder.class);
            org.apache.hc.client5.http.impl.classic.CloseableHttpClient httpClientMock = mock(org.apache.hc.client5.http.impl.classic.CloseableHttpClient.class);
            httpClientsMock.when(org.apache.hc.client5.http.impl.classic.HttpClients::custom).thenReturn(builderMock);
            when(builderMock.setDefaultRequestConfig(any())).thenReturn(builderMock);
            when(builderMock.setConnectionManager(any())).thenReturn(builderMock);
            when(builderMock.build()).thenReturn(httpClientMock);
            method.invoke(application);
            verify(builderMock).setDefaultRequestConfig(any());
            verify(builderMock).setConnectionManager(any());
            verify(builderMock).build();
        }
    }

}

