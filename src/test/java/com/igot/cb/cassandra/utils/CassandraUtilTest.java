package com.igot.cb.cassandra.utils;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class CassandraUtilTest {

    private ResultSet mockResultSet;
    private Row mockRow;
    private ColumnDefinitions mockColumnDefinitions;

    @BeforeEach
    void setUp() {
        mockResultSet = mock(ResultSet.class);
        mockRow = mock(Row.class);
        mockColumnDefinitions = mock(ColumnDefinitions.class);
    }

    @Test
    void testGetPreparedStatement() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("id", 1);
        data.put("name", "Mahesh");

        String actual = CassandraUtil.getPreparedStatement("test_keyspace", "test_table", data);
        String expected = "INSERT INTO test_keyspace.test_table(id,name) VALUES (?,?);";
        assertEquals(expected, actual);
    }

    @Test
    void testCreateResponseList() {
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockResultSet.iterator()).thenReturn(List.of(mockRow).iterator());

        List<Map<String, Object>> result = CassandraUtil.createResponse(mockResultSet);
        assertEquals(1, result.size());
    }

    @Test
    void testCreateResponseMap() {
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockResultSet.iterator()).thenReturn(List.of(mockRow).iterator());

        Map<String, Object> result = CassandraUtil.createResponse(mockResultSet, "id");
        assertEquals(1, result.size());
    }

    @Test
    void testPrivateConstructor() throws Exception {
        var constructor = CassandraUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        CassandraUtil instance = constructor.newInstance();
        assertNotNull(instance);
    }
}
