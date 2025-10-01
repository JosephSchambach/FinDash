package com.findash.services;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class getAssetDataTest {
    // Test cases for getAssetData class
    @Test
    public void testFetchData() {
        getAssetData service = new getAssetData();
        String[] result = service.fetchData("AAPL");
        assertNotNull(result);
        assertTrue(result.length > 0);
    }
}
