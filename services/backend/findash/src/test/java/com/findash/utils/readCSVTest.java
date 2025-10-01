package com.findash.utils;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class readCSVTest {
    // Test cases for readCSV class
    @Test
    public void testRead() {
        String[] result = readCSV.read("test.csv");
        assertNotNull(result);
        assertTrue(result.length > 0);
    }
}
