package com.igot.cb.common.util;

import com.igot.cb.common.model.KeyData;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KeyManagerTest {

    @Test
    void testLoadPublicKey_InvalidFormat() {
        String invalidPem = "not-a-key";
        assertThrows(Exception.class, () -> KeyManager.loadPublicKey(invalidPem));
    }

    @Test
    void testLoadPublicKey_ValidKey() throws Exception {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair pair = keyGen.generateKeyPair();
        PublicKey pubKey = pair.getPublic();

        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getEncoder().encodeToString(pubKey.getEncoded())
                + "\n-----END PUBLIC KEY-----";
        PublicKey loaded = KeyManager.loadPublicKey(pem);
        assertNotNull(loaded);
        assertEquals(pubKey, loaded);
    }

    @Test
    void testGetPublicKey_ReturnsNullIfNotPresent() {
        KeyManager keyManager = new KeyManager();
        assertNull(keyManager.getPublicKey("nonexistent"));
    }

    @Test
    void testGetPublicKey_ReturnsKeyDataIfPresent() {
        KeyManager keyManager = new KeyManager();
        // Use reflection to set keyMap
        KeyData keyData = new KeyData("testKey", null);
        setKeyMap(Map.of("testKey", keyData));
        assertSame(keyData, keyManager.getPublicKey("testKey"));
    }

    @Test
    void testInit_HandlesExceptionGracefully() {
        KeyManager keyManager = new KeyManager();
        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
            PropertiesCache cache = mock(PropertiesCache.class);
            staticMock.when(PropertiesCache::getInstance).thenReturn(cache);
            when(cache.getProperty(anyString())).thenReturn("/invalid/path");
            // an unreadable key directory must be logged and swallowed, not propagated
            assertDoesNotThrow(keyManager::init);
        }
    }

    // Helper to set static keyMap via reflection
    private void setKeyMap(Map<String, KeyData> map) {
        try {
            java.lang.reflect.Field field = KeyManager.class.getDeclaredField("keyMap");
            field.setAccessible(true);
            field.set(null, new HashMap<>(map));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testInit_LoadsValidPublicKeyFile(@TempDir Path tempDir) throws Exception {
        // 1. Generate dummy public key PEM
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keyPair = keyGen.generateKeyPair();
        PublicKey pubKey = keyPair.getPublic();

        String pem = "-----BEGIN PUBLIC KEY-----\n" +
                Base64.getEncoder().encodeToString(pubKey.getEncoded()) +
                "\n-----END PUBLIC KEY-----";

        // 2. Write it to a file in the temp directory
        Path keyFile = tempDir.resolve("mykey.pub");
        Files.write(keyFile, pem.getBytes(StandardCharsets.UTF_8));

        // 3. Create KeyManager instance
        KeyManager keyManager = new KeyManager();

        // 4. Inject private field 'propertiesCache' via reflection
        PropertiesCache mockCache = mock(PropertiesCache.class);
        when(mockCache.getProperty(anyString())).thenReturn(tempDir.toString());
        Field field = KeyManager.class.getDeclaredField("propertiesCache");
        field.setAccessible(true);
        field.set(keyManager, mockCache);

        // 5. Inject 'keyMap' and 'logger' fields as well
        Field keyMapField = KeyManager.class.getDeclaredField("keyMap");
        keyMapField.setAccessible(true);
        keyMapField.set(keyManager, new HashMap<>());

        Field loggerField = KeyManager.class.getDeclaredField("logger");
        loggerField.setAccessible(true);
        loggerField.set(keyManager, mock(org.slf4j.Logger.class));

        // 6. Execute init()
        keyManager.init();

        // 7. Assert that the key was loaded
        assertTrue(((Map<?, ?>) keyMapField.get(keyManager)).containsKey("mykey.pub"));
    }

}
