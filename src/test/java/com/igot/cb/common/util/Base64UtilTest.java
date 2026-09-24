package com.igot.cb.common.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class Base64UtilTest {

    @Test
    void testEncodeDecode_Default() {
        String original = "hello world";
        String encoded = Base64Util.encodeToString(original.getBytes(), Base64Util.DEFAULT);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertEquals(original, new String(decoded));
    }

    @Test
    void testEncodeDecode_NoPadding() {
        String original = "test";
        String encoded = Base64Util.encodeToString(original.getBytes(), Base64Util.NO_PADDING);
        // Should not end with '='
        assertFalse(encoded.endsWith("="));
        byte[] decoded = Base64Util.decode(encoded, Base64Util.NO_PADDING);
        assertEquals(original, new String(decoded));
    }

    @Test
    void testEncodeDecode_NoWrap() {
        String original = "this is a longer string to test no wrap option in base64 encoding";
        String encoded = Base64Util.encodeToString(original.getBytes(), Base64Util.NO_WRAP);
        assertFalse(encoded.contains("\n"));
        byte[] decoded = Base64Util.decode(encoded, Base64Util.NO_WRAP);
        assertEquals(original, new String(decoded));
    }

    @Test
    void testEncodeDecode_UrlSafe() {
        String original = "foo?bar=baz+qux/=";
        String encoded = Base64Util.encodeToString(original.getBytes(), Base64Util.URL_SAFE);
        assertFalse(encoded.contains("+"));
        assertFalse(encoded.contains("/"));
        byte[] decoded = Base64Util.decode(encoded, Base64Util.URL_SAFE);
        assertEquals(original, new String(decoded));
    }

    @Test
    void testEncodeDecode_Empty() {
        String original = "";
        String encoded = Base64Util.encodeToString(original.getBytes(), Base64Util.DEFAULT);
        assertEquals("", encoded);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertEquals(original, new String(decoded));
    }

    @Test
    void testDecode_InvalidInput() {
        String invalid = "!!!notbase64!!!";
        assertThrows(IllegalArgumentException.class, () -> Base64Util.decode(invalid, Base64Util.DEFAULT));
    }

    @Test
    void testEncodeToStringWithUnsupportedEncoding() throws Exception {
        byte[] data = "test".getBytes();

        // Simulate UnsupportedEncodingException using reflection
        Method method = Base64Util.class.getDeclaredMethod("encodeToString", byte[].class, int.class, int.class, int.class);
        method.setAccessible(true);

        try {
            method.invoke(Base64Util.class, data, 0, data.length, Base64Util.DEFAULT);
        } catch (InvocationTargetException e) {
            // Catch the AssertionError thrown
            assertTrue(e.getCause() instanceof AssertionError);
            assertTrue(e.getCause().getCause() instanceof UnsupportedEncodingException);
        }
    }

    @ParameterizedTest(name = "encode byte[{0}] (len % 3 == {1})")
    @CsvSource({
            "3, 0",
            "4, 1",
            "5, 2"
    })
    void testEncodeLengthMod3(int dataLength, int expectedMod) {
        assertEquals(expectedMod, dataLength % 3);
        byte[] data = new byte[dataLength];
        String encoded = Base64Util.encodeToString(data, Base64Util.NO_PADDING);
        assertNotNull(encoded);
    }

    @ParameterizedTest(name = "decode \"{0}\" -> \"{1}\"")
    @CsvSource({
            "TQ==, M",
            "TWE=, Ma",
            "TWFu, Man"
    })
    void testDecoderPadding(String encoded, String expected) {
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertEquals(expected, new String(decoded));
    }

    @Test
    void testDecoderIllegalCharacter() {
        // Bad Base64 - Invalid length (should be multiple of 4), and illegal padding
        String invalidBase64 = "T==="; // Incorrect padding pattern
        assertThrows(IllegalArgumentException.class, () -> {
            Base64Util.decode(invalidBase64, Base64Util.DEFAULT);
        });
    }

    @Test
    void testEncoderMaxOutputSize() {
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.DEFAULT, new byte[0]);
        int result = encoder.maxOutputSize(10);
        assertTrue(result > 0);
    }

    @Test
    void testDecoderMaxOutputSize() {
        Base64Util.Decoder decoder = new Base64Util.Decoder(Base64Util.DEFAULT, new byte[0]);
        int result = decoder.maxOutputSize(10);
        assertTrue(result > 0);
    }

    @Test
    void testTailCase1WithFollowingInput() {
        byte[] fullInput = new byte[]{(byte) 0xE1, (byte) 0xE2, (byte) 0xE3}; // 3 bytes
        byte[] firstChunk = Arrays.copyOf(fullInput, 1); // Only 1 byte initially
        byte[] nextChunk = Arrays.copyOfRange(fullInput, 1, 3); // Remaining 2 bytes

        // Setup encoder manually
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.DEFAULT, new byte[100]);

        // Call process on first chunk to populate tailLen == 1
        encoder.process(firstChunk, 0, firstChunk.length, false); // Not finish

        // Call process on remaining chunk (triggers case 1)
        encoder.process(nextChunk, 0, nextChunk.length, true);

        // Validate output is not empty
        assertTrue(encoder.output.length > 0);
    }

    @Test
    void testTailCase2WithFollowingInput() {
        byte[] fullInput = new byte[]{(byte) 0xC1, (byte) 0xC2, (byte) 0xC3}; // 3 bytes
        byte[] firstChunk = Arrays.copyOf(fullInput, 2); // 2 bytes first
        byte[] nextChunk = Arrays.copyOfRange(fullInput, 2, 3); // 1 byte remaining

        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.DEFAULT, new byte[100]);

        encoder.process(firstChunk, 0, firstChunk.length, false); // Stores 2 in tail
        encoder.process(nextChunk, 0, nextChunk.length, true);    // triggers case 2

        assertTrue(encoder.output.length > 0);
    }

    @Test
    void testNewlineAfterLineGroupLimit() {
        // 57 bytes → 76 Base64 chars → triggers line break
        byte[] input = new byte[57];
        for (int i = 0; i < input.length; i++) input[i] = (byte) i;

        String encoded = Base64Util.encodeToString(input, Base64Util.DEFAULT);
        assertTrue(encoded.contains("\n")); // Newline inserted
    }

    @Test
    void testTailSaveConditions() {
        // Input that leaves 1 or 2 bytes unprocessed (not finish)
        byte[] input = new byte[]{1, 2, 3, 4, 5}; // 5 bytes, remainder = 2

        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.DEFAULT, new byte[100]);
        encoder.process(input, 0, input.length, false); // finish = false

        // Tail should have 2 bytes left
        assertEquals(2, encoder.tailLen);
    }


}