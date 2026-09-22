package com.igot.cb.common.util;

import com.igot.cb.common.model.KeyData;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.common.util.Time;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import java.security.PublicKey;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AccessTokenValidatorTest {

    @InjectMocks
    private AccessTokenValidator validator;

    @Mock
    private KeyManager keyManager;

    @Mock
    private PublicKey publicKey;

    @Mock
    private Logger logger;

    @Mock
    private KeyData keyData;

    @BeforeEach
    void setup() {
        // Mock PropertiesCache singleton
        PropertiesCache cache = mock(PropertiesCache.class);
        lenient().when(cache.getProperty(Constants.SSO_URL)).thenReturn("http://sso/");
        lenient().when(cache.getProperty(Constants.SSO_REALM)).thenReturn("realm");
        lenient().when(keyData.getPublicKey()).thenReturn(publicKey);
        // Use reflection to set static cache field
        try {
            var field = AccessTokenValidator.class.getDeclaredField("cache");
            field.setAccessible(true);
            field.set(null, cache);
        } catch (Exception e) {
            fail("Could not inject the mocked PropertiesCache into AccessTokenValidator.cache", e);
        }
    }

    @Test
    void testVerifyUserToken_ValidToken() {
        String headerJson = "{\"alg\":\"RS256\",\"kid\":\"kid1\"}";
        String bodyJson = "{\"exp\":" + (Time.currentTime() + 1000) + ",\"iss\":\"http://sso/realms/realm\",\"sub\":\"user:123\"}";
        String header = Base64.getUrlEncoder().withoutPadding().encodeToString(headerJson.getBytes());
        String body = Base64.getUrlEncoder().withoutPadding().encodeToString(bodyJson.getBytes());
        String signature = Base64.getUrlEncoder().withoutPadding().encodeToString("sig".getBytes());
        String token = header + "." + body + "." + signature;

        when(keyManager.getPublicKey("kid1")).thenReturn(keyData);
        try (MockedStatic<CryptoUtil> cryptoUtil = mockStatic(CryptoUtil.class)) {
            cryptoUtil.when(() -> CryptoUtil.verifyRSASign(anyString(), any(), any(), anyString())).thenReturn(true);

            String userId = validator.verifyUserToken(token);
            assertEquals("123", userId);
        }
    }

    @Test
    void testVerifyUserToken_ExpiredToken() {
        String headerJson = "{\"alg\":\"RS256\",\"kid\":\"kid1\"}";
        String bodyJson = "{\"exp\":1,\"iss\":\"http://sso/realms/realm\",\"sub\":\"user:123\"}";
        String header = Base64.getUrlEncoder().withoutPadding().encodeToString(headerJson.getBytes());
        String body = Base64.getUrlEncoder().withoutPadding().encodeToString(bodyJson.getBytes());
        String signature = Base64.getUrlEncoder().withoutPadding().encodeToString("sig".getBytes());
        String token = header + "." + body + "." + signature;

        when(keyManager.getPublicKey("kid1")).thenReturn(keyData);
        try (MockedStatic<CryptoUtil> cryptoUtil = mockStatic(CryptoUtil.class)) {
            cryptoUtil.when(() -> CryptoUtil.verifyRSASign(anyString(), any(), any(), anyString())).thenReturn(true);

            String userId = validator.verifyUserToken(token);
            assertEquals(Constants.UNAUTHORIZED_USER_ID, userId);
        }
    }

    @Test
    void testVerifyUserToken_InvalidSignature() {
        String headerJson = "{\"alg\":\"RS256\",\"kid\":\"kid1\"}";
        String bodyJson = "{\"exp\":" + (Time.currentTime() + 1000) + ",\"iss\":\"http://sso/realms/realm\",\"sub\":\"user:123\"}";
        String header = Base64.getUrlEncoder().withoutPadding().encodeToString(headerJson.getBytes());
        String body = Base64.getUrlEncoder().withoutPadding().encodeToString(bodyJson.getBytes());
        String signature = Base64.getUrlEncoder().withoutPadding().encodeToString("sig".getBytes());
        String token = header + "." + body + "." + signature;

        when(keyManager.getPublicKey("kid1")).thenReturn(keyData);
        try (MockedStatic<CryptoUtil> cryptoUtil = mockStatic(CryptoUtil.class)) {
            cryptoUtil.when(() -> CryptoUtil.verifyRSASign(anyString(), any(), any(), anyString())).thenReturn(false);

            String userId = validator.verifyUserToken(token);
            assertEquals(Constants.UNAUTHORIZED_USER_ID, userId);
        }
    }

    @Test
    void testFetchUserIdFromAccessToken_NullToken() {
        String userId = validator.fetchUserIdFromAccessToken(null);
        assertNull(userId);
    }

    @Test
    void testFetchUserIdFromAccessToken_Unauthorized() {
        AccessTokenValidator spyValidator = spy(validator);
        doReturn(Constants.UNAUTHORIZED_USER_ID).when(spyValidator).verifyUserToken(anyString());
        String userId = spyValidator.fetchUserIdFromAccessToken("token");
        assertNull(userId);
    }
}
