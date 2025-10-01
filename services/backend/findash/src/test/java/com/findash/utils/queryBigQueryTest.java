package com.findash.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class queryBigQueryTest {
    // Test cases for queryBigQuery class
    @Test
    public void testRunQuery() {
        queryBigQuery bq = new queryBigQuery();
        String result = bq.runQuery("SELECT * FROM dataset.table");
        assertEquals("Query result for: SELECT * FROM dataset.table", result);
    }
}
