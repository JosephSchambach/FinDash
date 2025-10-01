package com.findash.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class formatJSONTest {
    // Test cases for formatJSON class
    @Test
    public void testFormat() {
        String input = "{\"name\":\"John\", \"age\":30}";
        String expectedOutput = "{\n  \"name\": \"John\",\n  \"age\": 30\n}";
        String actualOutput = formatJSON.format(input);
        assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testFormatArray() {
        String[] input = {"{\"name\":\"John\", \"age\":30}", "{\"name\":\"Jane\", \"age\":25}"};
        String[] expectedOutput = {
            "{\n  \"name\": \"John\",\n  \"age\": 30\n}",
            "{\n  \"name\": \"Jane\",\n  \"age\": 25\n}"
        };
        String[] actualOutput = formatJSON.formatArray(input);
        assertArrayEquals(expectedOutput, actualOutput);
    }
}
