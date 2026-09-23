package com.igot.cb.cassandra.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.igot.cb.common.helper.cassandra.CassandraConnectionManager;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CassandraOperationImplTest {

    @InjectMocks
    private CassandraOperationImpl cassandraOperation;

    @Mock
    private CassandraConnectionManager connectionManager;

    @Mock
    private CqlSession session;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetRecordsByProperties() {
        String keyspace = "ks";
        String table = "tbl";
        Map<String, Object> propertyMap = Map.of("id", "1");
        List<String> fields = List.of("id", "name");
        ResultSet resultSet = mock(ResultSet.class);

        when(connectionManager.getSession(keyspace)).thenReturn(session);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);
        try (MockedStatic<CassandraUtil> util = mockStatic(CassandraUtil.class)) {
            util.when(() -> CassandraUtil.createResponse(resultSet)).thenReturn(List.of(Map.of("id", "1", "name", "test")));
            List<Map<String, Object>> result = cassandraOperation.getRecordsByProperties(keyspace, table, propertyMap, fields);
            assertEquals(1, result.size());
            assertEquals("1", result.get(0).get("id"));
        }
    }

    @Test
    void testGetCountByProperties() {
        String keyspace = "ks";
        String table = "tbl";
        Map<String, Object> propertyMap = Map.of("id", "1");
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        BoundStatement boundStatement = mock(BoundStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);

        when(connectionManager.getSession(keyspace)).thenReturn(session);
        when(session.prepare((SimpleStatement) any(Statement.class))).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(Object[].class))).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenReturn(resultSet);
        when(resultSet.one()).thenReturn(row);
        when(row.getLong(0)).thenReturn(5L);

        int count = cassandraOperation.getCountByProperties(keyspace, table, propertyMap);
        assertEquals(5, count);
    }

    @Test
    void testInsertRecordSuccess() {
        String keyspace = "ks";
        String table = "tbl";
        Map<String, Object> request = Map.of("id", "1", "name", "test");
        PreparedStatement statement = mock(PreparedStatement.class);
        BoundStatement boundStatement = mock(BoundStatement.class);

        when(connectionManager.getSession(keyspace)).thenReturn(session);
        when(session.prepare(anyString())).thenReturn(statement);
        when(statement.bind(any(Object[].class))).thenReturn(boundStatement);

        try (MockedStatic<CassandraUtil> util = mockStatic(CassandraUtil.class)) {
            util.when(() -> CassandraUtil.getPreparedStatement(keyspace, table, request)).thenReturn("insert into ...");
            SBApiResponse response = cassandraOperation.insertRecord(keyspace, table, request);
            assertEquals("SUCCESS", response.get("STATUS"));
        }
    }

    @Test
    void testInsertRecordFailure() {
        String keyspace = "ks";
        String table = "tbl";
        Map<String, Object> request = Map.of("id", "1");
        when(connectionManager.getSession(keyspace)).thenThrow(new RuntimeException("fail"));

        SBApiResponse response = cassandraOperation.insertRecord(keyspace, table, request);
        assertEquals("FAILED", response.get("STATUS"));
    }

    @Test
    void testGetRecordsByPropertiesWithoutFiltering() {
        String keyspace = "ks";
        String table = "tbl";
        Map<String, Object> propertyMap = Map.of("id", "1");
        List<String> fields = List.of("id");
        ResultSet resultSet = mock(ResultSet.class);

        when(connectionManager.getSession(keyspace)).thenReturn(session);
        when(session.execute(any(SimpleStatement.class))).thenReturn(resultSet);
        try (MockedStatic<CassandraUtil> util = mockStatic(CassandraUtil.class)) {
            util.when(() -> CassandraUtil.createResponse(resultSet)).thenReturn(List.of(Map.of("id", "1")));
            List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering(keyspace, table, propertyMap, fields);
            assertEquals(1, result.size());
        }
    }

    @Test
    void testUpdateRecordSuccess() {
        String keyspace = "ks";
        String table = "tbl";
        Map<String, Object> updateAttributes = Map.of("name", "newName");
        Map<String, Object> compositeKey = Map.of("id", "1");

        when(connectionManager.getSession(keyspace)).thenReturn(session);

        Map<String, Object> result = cassandraOperation.updateRecord(keyspace, table, updateAttributes, compositeKey);
        assertEquals(Constants.SUCCESS, result.get(Constants.RESPONSE));
    }

    @Test
    void testUpdateRecordFailure() {
        String keyspace = "ks";
        String table = "tbl";
        Map<String, Object> updateAttributes = Map.of("name", "newName");
        Map<String, Object> compositeKey = Map.of("id", "1");

        when(connectionManager.getSession(keyspace)).thenThrow(new RuntimeException("fail"));
        assertThrows(RuntimeException.class, () -> cassandraOperation.updateRecord(keyspace, table, updateAttributes, compositeKey));
    }

    @Test
    void testUpdateRecord_BuildsUpdateStatement() {
        when(connectionManager.getSession("ks")).thenReturn(session);

        Map<String, Object> result = cassandraOperation.updateRecord("ks", "tbl",
                Map.of("name", "newName"), Map.of("id", "1"));

        assertEquals(Constants.SUCCESS, result.get(Constants.RESPONSE));
        ArgumentCaptor<SimpleStatement> captor = ArgumentCaptor.forClass(SimpleStatement.class);
        verify(session).execute(captor.capture());
        assertEquals("UPDATE ks.tbl SET name='newName' WHERE id='1'", captor.getValue().getQuery());
    }

    @Test
    void testGetRecordsByProperties_AllColumnsWithListAndScalarFilters() {
        ResultSet resultSet = mock(ResultSet.class);
        when(connectionManager.getSession("ks")).thenReturn(session);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);
        Map<String, Object> propertyMap = new LinkedHashMap<>();
        propertyMap.put("id", List.of("1", "2"));
        propertyMap.put("ignored", Collections.emptyList());
        propertyMap.put("name", "test");

        try (MockedStatic<CassandraUtil> util = mockStatic(CassandraUtil.class)) {
            util.when(() -> CassandraUtil.createResponse(resultSet)).thenReturn(List.of(Map.of("id", "1")));

            List<Map<String, Object>> result = cassandraOperation.getRecordsByProperties("ks", "tbl", propertyMap, null);

            assertEquals(1, result.size());
        }
        ArgumentCaptor<SimpleStatement> captor = ArgumentCaptor.forClass(SimpleStatement.class);
        verify(session).execute(captor.capture());
        String query = captor.getValue().getQuery();
        assertEquals("SELECT * FROM ks.tbl WHERE id IN ('1','2') AND name='test' ALLOW FILTERING", query);
    }

    @Test
    void testGetRecordsByProperties_NoFilters() {
        ResultSet resultSet = mock(ResultSet.class);
        when(connectionManager.getSession("ks")).thenReturn(session);
        when(session.execute(any(Statement.class))).thenReturn(resultSet);

        try (MockedStatic<CassandraUtil> util = mockStatic(CassandraUtil.class)) {
            util.when(() -> CassandraUtil.createResponse(resultSet)).thenReturn(Collections.emptyList());

            List<Map<String, Object>> result = cassandraOperation.getRecordsByProperties("ks", "tbl",
                    Collections.emptyMap(), Collections.emptyList());

            assertTrue(result.isEmpty());
        }
        ArgumentCaptor<SimpleStatement> captor = ArgumentCaptor.forClass(SimpleStatement.class);
        verify(session).execute(captor.capture());
        assertEquals("SELECT * FROM ks.tbl", captor.getValue().getQuery());
    }

    @Test
    void testGetRecordsByProperties_Exception() {
        when(connectionManager.getSession("ks")).thenThrow(new RuntimeException("no session"));

        List<Map<String, Object>> result = cassandraOperation.getRecordsByProperties("ks", "tbl",
                Map.of("id", "1"), List.of("id"));

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetCountByProperties_NoRow() {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        BoundStatement boundStatement = mock(BoundStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connectionManager.getSession("ks")).thenReturn(session);
        when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(Object[].class))).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenReturn(resultSet);
        when(resultSet.one()).thenReturn(null);

        assertEquals(0, cassandraOperation.getCountByProperties("ks", "tbl", Map.of("id", "1")));
    }

    @Test
    void testGetCountByProperties_Exception() {
        when(connectionManager.getSession("ks")).thenThrow(new RuntimeException("no session"));

        assertEquals(0, cassandraOperation.getCountByProperties("ks", "tbl", Map.of("id", "1")));
    }

    @Test
    void testGetRecordsByPropertiesWithoutFiltering_ListFiltersAndAllColumns() {
        ResultSet resultSet = mock(ResultSet.class);
        when(connectionManager.getSession("ks")).thenReturn(session);
        when(session.execute(any(SimpleStatement.class))).thenReturn(resultSet);
        Map<String, Object> propertyMap = new LinkedHashMap<>();
        propertyMap.put("id", List.of("1", "2"));
        propertyMap.put("tags", Collections.emptyList());
        propertyMap.put("name", "test");

        try (MockedStatic<CassandraUtil> util = mockStatic(CassandraUtil.class)) {
            util.when(() -> CassandraUtil.createResponse(resultSet)).thenReturn(List.of(Map.of("id", "1")));

            List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering("ks", "tbl",
                    propertyMap, null);

            assertEquals(1, result.size());
        }
        ArgumentCaptor<SimpleStatement> captor = ArgumentCaptor.forClass(SimpleStatement.class);
        verify(session).execute(captor.capture());
        String query = captor.getValue().getQuery();
        assertTrue(query.startsWith("SELECT * FROM ks.tbl WHERE id IN ('1','2')"), query);
        assertFalse(query.contains("ALLOW FILTERING"));
    }

    @Test
    void testGetRecordsByPropertiesWithoutFiltering_NoFilters() {
        ResultSet resultSet = mock(ResultSet.class);
        when(connectionManager.getSession("ks")).thenReturn(session);
        when(session.execute(any(SimpleStatement.class))).thenReturn(resultSet);

        try (MockedStatic<CassandraUtil> util = mockStatic(CassandraUtil.class)) {
            util.when(() -> CassandraUtil.createResponse(resultSet)).thenReturn(Collections.emptyList());

            List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering("ks", "tbl",
                    null, List.of("id"));

            assertTrue(result.isEmpty());
        }
        ArgumentCaptor<SimpleStatement> captor = ArgumentCaptor.forClass(SimpleStatement.class);
        verify(session).execute(captor.capture());
        assertEquals("SELECT id FROM ks.tbl", captor.getValue().getQuery());
    }

    @Test
    void testGetRecordsByPropertiesWithoutFiltering_Exception() {
        when(connectionManager.getSession("ks")).thenThrow(new RuntimeException("no session"));

        List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering("ks", "tbl",
                Map.of("id", "1"), List.of("id"));

        assertTrue(result.isEmpty());
    }

    @Test
    void testGetRecordsByPropertiesWithPagination() {
        assertTrue(cassandraOperation.getRecordsByPropertiesWithPagination().isEmpty());
    }
}