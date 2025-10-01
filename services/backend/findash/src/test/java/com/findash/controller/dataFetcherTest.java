package com.findash.controller;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class dataFetcherTest {
    // Test cases for dataFetcher class
    @Test
    public void testData() {
        dataFetcher controller = new dataFetcher();
        Map<String, String> payload = new HashMap<>();
        payload.put("symbol", "AAPL");
        String[] result = controller.data(payload);
        assertNotNull(result);
        assertTrue(result.length > 0);
    }
}
