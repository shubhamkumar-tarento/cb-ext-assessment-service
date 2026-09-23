package com.igot.cb.common.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.igot.cb.common.util.Constants;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class OutboundRequestHandlerServiceImpl {
    private Logger log = LoggerFactory.getLogger(OutboundRequestHandlerServiceImpl.class);

    private static final String ERROR_RECEIVED_LOG = "Error received: {}";
    private static final String ERROR_RESPONSE_LOG = "Error Response: {}";

    private final RestTemplate restTemplate;

    public OutboundRequestHandlerServiceImpl(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }


    /**
     * @param uri
     * @return
     * @throws Exception
     */
    public Object fetchResult(String uri) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        Object response = null;
        try {
            logFetchUri(uri);
            response = restTemplate.getForObject(uri, Map.class);
        } catch (HttpClientErrorException e) {
            response = recoverErrorBody(e);
        } catch (Exception e) {
            logSerialisationFailure(e, mapper, response);
        }
        return response;
    }

    /**
     * @param uri
     * @return
     * @throws Exception
     */
    public Object fetchUsingGetWithHeaders(String uri, Map<String, String> headersValues) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            logFetchUri(uri);
            HttpHeaders headers = buildHeaders(headersValues, false);
            HttpEntity<Void> entity = new HttpEntity<>(headers);
            return restTemplate.exchange(uri, HttpMethod.GET, entity, Map.class).getBody();
        } catch (Exception e) {
            log.error(String.valueOf(e));
        }
        return null;
    }

    public Map<String, Object> fetchResultUsingPatch(String uri, Object request, Map<String, String> headersValues) {
        Map<String, Object> response = null;
        try {
            HttpHeaders headers = buildHeaders(headersValues, true);
            HttpEntity<Object> entity = new HttpEntity<>(request, headers);
            if (log.isDebugEnabled()) {
                logDetails(uri, request);
            }
            response = restTemplate.patchForObject(uri, entity, Map.class);
            if (log.isDebugEnabled()) {
                logDetails(uri, response);
            }
        } catch (HttpClientErrorException e) {
            try {
                response = (new ObjectMapper()).readValue(e.getResponseBodyAsString(),
                        new TypeReference<HashMap<String, Object>>() {
                        });
            } catch (Exception e1) {
                return Collections.emptyMap();
            }
            log.error(ERROR_RECEIVED_LOG, e.getResponseBodyAsString(), e);
        }
        if (response == null) {
            return MapUtils.EMPTY_SORTED_MAP;
        }
        return response;
    }


    private void logDetails(String uri, Object objectDetails) {
        if (!log.isDebugEnabled()) {
            return;
        }
        try {
            StringBuilder str = new StringBuilder(this.getClass().getCanonicalName()).append(".fetchResult")
                    .append(System.lineSeparator());
            str.append("URI: ").append(uri).append(System.lineSeparator());
            str.append("Request/Response: ").append((new ObjectMapper()).writeValueAsString(objectDetails))
                    .append(System.lineSeparator());
            log.debug(str.toString());
        } catch (JsonProcessingException je) {
            // Details could not be serialised; skip this debug line rather than fail the request.
        }
    }

    public Map<String, Object> fetchResultUsingPost(String uri, Object request, Map<String, String> headersValues) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        Map<String, Object> response = null;
        try {
            HttpHeaders headers = buildHeaders(headersValues, true);
            HttpEntity<Object> entity = new HttpEntity<>(request, headers);
            if (log.isDebugEnabled()) {
                StringBuilder str = new StringBuilder(this.getClass().getCanonicalName()).append(".fetchResult")
                        .append(System.lineSeparator());
                str.append("URI: ").append(uri).append(System.lineSeparator());
                str.append("Request: ").append(mapper.writeValueAsString(request)).append(System.lineSeparator());
                log.debug(str.toString());
            }
            response = restTemplate.postForObject(uri, entity, Map.class);
            logResponseDebug(mapper, response);
        } catch (HttpClientErrorException hce) {
            response = recoverErrorBody(hce);
        } catch(JsonProcessingException e) {
            logSerialisationFailure(e, mapper, response);
        }
        return response;
    }

    public Map<String, Object> fetchResultUsingGet(String uri, Map<String, String> headersValues) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        Map<String, Object> response = null;
        try {
            HttpHeaders headers = buildHeaders(headersValues, true);
            HttpEntity<Object> entity = new HttpEntity<>(headers);
            logFetchUri(uri);
            response = restTemplate.exchange(uri, HttpMethod.GET, entity, Map.class).getBody();
            logResponseDebug(mapper, response);
        } catch (HttpClientErrorException hce) {
            response = recoverErrorBody(hce);
        } catch(JsonProcessingException e) {
            logSerialisationFailure(e, mapper, response);
        }
        return response;
    }

    private HttpHeaders buildHeaders(Map<String, String> headersValues, boolean jsonContentType) {
        HttpHeaders headers = new HttpHeaders();
        if (!CollectionUtils.isEmpty(headersValues)) {
            headersValues.forEach(headers::set);
        }
        if (jsonContentType) {
            headers.setContentType(MediaType.APPLICATION_JSON);
        }
        return headers;
    }

    private void logFetchUri(String uri) {
        if (log.isDebugEnabled()) {
            StringBuilder str = new StringBuilder(this.getClass().getCanonicalName())
                    .append(Constants.FETCH_RESULT_CONSTANT).append(System.lineSeparator());
            str.append(Constants.URI_CONSTANT).append(uri).append(System.lineSeparator());
            log.debug(str.toString());
        }
    }

    private void logResponseDebug(ObjectMapper mapper, Object response) throws JsonProcessingException {
        if (log.isDebugEnabled()) {
            StringBuilder str = new StringBuilder("Response: ");
            str.append(mapper.writeValueAsString(response)).append(System.lineSeparator());
            log.debug(str.toString());
        }
    }

    private Map<String, Object> recoverErrorBody(HttpClientErrorException e) {
        Map<String, Object> response = null;
        try {
            response = (new ObjectMapper()).readValue(e.getResponseBodyAsString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });
        } catch (Exception e1) {
            // Body is not JSON; leave response null and fall through to the error log below.
        }
        log.error(ERROR_RECEIVED_LOG, e.getResponseBodyAsString(), e);
        return response;
    }

    private void logSerialisationFailure(Exception e, ObjectMapper mapper, Object response) {
        log.error(String.valueOf(e));
        try {
            log.warn(ERROR_RESPONSE_LOG, mapper.writeValueAsString(response));
        } catch (Exception e1) {
            // Response could not be serialised for logging; nothing further to report.
        }
    }
}
