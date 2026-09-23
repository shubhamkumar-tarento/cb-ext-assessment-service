package com.igot.cb.common.helper.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.igot.cb.common.util.Constants;
import com.igot.cb.common.util.PropertiesCache;
import com.igot.cb.core.exception.CustomException;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CassandraConnectionManagerImplTest {

    @Mock
    PropertiesCache propertiesCache;

    private AutoCloseable mocks;

    @BeforeEach
    void setup() {
        mocks = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void tearDown() throws Exception {
        sessionMap().clear();
        setDefaultSession(null);
        mocks.close();
    }

    @Test
    void testGetConsistencyLevel_valid() {
        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
            staticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL))
                    .thenReturn("LOCAL_QUORUM");

            ConsistencyLevel level = invokeGetConsistencyLevel();
            assertEquals(DefaultConsistencyLevel.LOCAL_QUORUM, level);
        }
    }

    @Test
    void testGetConsistencyLevel_invalid() {
        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
            staticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL))
                    .thenReturn("INVALID");

            ConsistencyLevel level = invokeGetConsistencyLevel();
            assertNull(level);
        }
    }

    @Test
    void testGetConsistencyLevel_blank() {
        try (MockedStatic<PropertiesCache> staticMock = mockStatic(PropertiesCache.class)) {
            staticMock.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            when(propertiesCache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL)).thenReturn(" ");

            assertNull(invokeGetConsistencyLevel());
        }
    }

    private ConsistencyLevel invokeGetConsistencyLevel() {
        try {
            Method method = CassandraConnectionManagerImpl.class.getDeclaredMethod("getConsistencyLevel");
            method.setAccessible(true);
            return (ConsistencyLevel) method.invoke(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testConstructorThrowsException_whenHostIsBlank() {
        try (
                MockedStatic<PropertiesCache> propertiesCacheStatic = Mockito.mockStatic(PropertiesCache.class)
        ) {
            // Arrange
            PropertiesCache mockPropertiesCache = mock(PropertiesCache.class);
            propertiesCacheStatic.when(PropertiesCache::getInstance).thenReturn(mockPropertiesCache);
            when(mockPropertiesCache.getProperty(Constants.CASSANDRA_CONFIG_HOST)).thenReturn("");

            // Act & Assert
            CustomException exception = assertThrows(CustomException.class, CassandraConnectionManagerImpl::new);
            assertEquals("Cassandra host is not configured", exception.getMessage()); // Adjust message if needed
        }
    }

    @Test
    void testConstructor_createsDefaultSessionWithoutKeyspace() throws Exception {
        CqlSession defaultSession = sessionWithMetadata();
        try (MockedStatic<PropertiesCache> cacheStatic = mockStatic(PropertiesCache.class);
             MockedConstruction<CqlSessionBuilder> builders = mockBuilders(defaultSession)) {
            cacheStatic.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            stubConnectionProperties(null);

            CassandraConnectionManagerImpl manager = new CassandraConnectionManagerImpl();

            assertNotNull(manager);
            assertEquals(1, builders.constructed().size());
            CqlSessionBuilder builder = builders.constructed().get(0);
            verify(builder).withLocalDatacenter("datacenter1");
            verify(builder, never()).withKeyspace(anyString());
            verify(defaultSession).getMetadata();
            assertSame(defaultSession, getDefaultSession());
        }
    }

    @Test
    void testConstructor_builderFailure_wrapsInCustomException() {
        try (MockedStatic<PropertiesCache> cacheStatic = mockStatic(PropertiesCache.class);
             MockedConstruction<CqlSessionBuilder> builders = mockConstruction(CqlSessionBuilder.class,
                     withSettings().defaultAnswer(RETURNS_SELF),
                     (mock, ctx) -> doThrow(new IllegalStateException("cluster unreachable")).when(mock).build())) {
            cacheStatic.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            stubConnectionProperties("QUORUM");

            CustomException ex = assertThrows(CustomException.class, CassandraConnectionManagerImpl::new);
            assertEquals("cluster unreachable", ex.getMessage());
            assertEquals(1, builders.constructed().size());
        }
    }

    @Test
    void testGetSession_createsCachesAndRecreatesClosedSession() throws Exception {
        CqlSession defaultSession = sessionWithMetadata();
        CqlSession keyspaceSession = sessionWithMetadata();
        CqlSession recreatedSession = sessionWithMetadata();
        try (MockedStatic<PropertiesCache> cacheStatic = mockStatic(PropertiesCache.class);
             MockedConstruction<CqlSessionBuilder> builders =
                     mockBuilders(defaultSession, keyspaceSession, recreatedSession)) {
            cacheStatic.when(PropertiesCache::getInstance).thenReturn(propertiesCache);
            stubConnectionProperties("LOCAL_QUORUM");

            CassandraConnectionManagerImpl manager = new CassandraConnectionManagerImpl();

            // first call creates a keyspace scoped session
            assertSame(keyspaceSession, manager.getSession("ks_one"));
            verify(builders.constructed().get(1)).withKeyspace("ks_one");

            // second call re-uses the open cached session
            when(keyspaceSession.isClosed()).thenReturn(false);
            assertSame(keyspaceSession, manager.getSession("ks_one"));
            assertEquals(2, builders.constructed().size());

            // a closed session is replaced by a new one
            when(keyspaceSession.isClosed()).thenReturn(true);
            assertSame(recreatedSession, manager.getSession("ks_one"));
            assertEquals(3, builders.constructed().size());
            assertSame(recreatedSession, sessionMap().get("ks_one"));
        }
    }

    @Test
    void testResourceCleanUp_closesAllSessions() throws Exception {
        CqlSession mapSession = mock(CqlSession.class);
        CqlSession defaultSession = mock(CqlSession.class);
        sessionMap().put("ks_cleanup", mapSession);
        setDefaultSession(defaultSession);

        new CassandraConnectionManagerImpl.ResourceCleanUp().run();

        verify(mapSession).close();
        verify(defaultSession).close();
    }

    @Test
    void testResourceCleanUp_noSessions() {
        CassandraConnectionManagerImpl.ResourceCleanUp cleanUp = new CassandraConnectionManagerImpl.ResourceCleanUp();
        assertDoesNotThrow(cleanUp::run);
    }

    @Test
    void testResourceCleanUp_swallowsCloseFailure() throws Exception {
        CqlSession failing = mock(CqlSession.class);
        doThrow(new RuntimeException("close failed")).when(failing).close();
        CqlSession defaultSession = mock(CqlSession.class);
        sessionMap().put("ks_fail", failing);
        setDefaultSession(defaultSession);

        assertDoesNotThrow(() -> new CassandraConnectionManagerImpl.ResourceCleanUp().run());
        verify(failing).close();
        // the failure aborts the rest of the cleanup
        verify(defaultSession, never()).close();
    }

    @Test
    void testRegisterShutDownHook() {
        assertDoesNotThrow(CassandraConnectionManagerImpl::registerShutDownHook);
    }

    private void stubConnectionProperties(String consistency) {
        when(propertiesCache.getProperty(Constants.CASSANDRA_CONFIG_HOST)).thenReturn("127.0.0.1, 127.0.0.2");
        when(propertiesCache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)).thenReturn("1");
        when(propertiesCache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)).thenReturn("1");
        when(propertiesCache.getProperty(Constants.HEARTBEAT_INTERVAL)).thenReturn("30");
        when(propertiesCache.readProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL)).thenReturn(consistency);
    }

    private static MockedConstruction<CqlSessionBuilder> mockBuilders(CqlSession... sessions) {
        AtomicInteger index = new AtomicInteger();
        return mockConstruction(CqlSessionBuilder.class, withSettings().defaultAnswer(RETURNS_SELF),
                (mock, ctx) -> {
                    CqlSession next = sessions[Math.min(index.getAndIncrement(), sessions.length - 1)];
                    doReturn(next).when(mock).build();
                });
    }

    private static CqlSession sessionWithMetadata() {
        CqlSession cqlSession = mock(CqlSession.class);
        Metadata metadata = mock(Metadata.class);
        Node node = mock(Node.class);
        when(node.getDatacenter()).thenReturn("datacenter1");
        when(node.getEndPoint()).thenReturn(mock(EndPoint.class));
        when(node.getRack()).thenReturn("rack1");
        when(metadata.getClusterName()).thenReturn(Optional.of("test-cluster"));
        when(metadata.getNodes()).thenReturn(Map.of(UUID.randomUUID(), node));
        when(cqlSession.getMetadata()).thenReturn(metadata);
        return cqlSession;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, CqlSession> sessionMap() throws Exception {
        Field mapField = CassandraConnectionManagerImpl.class.getDeclaredField("cassandraSessionMap");
        mapField.setAccessible(true);
        return (Map<String, CqlSession>) mapField.get(null);
    }

    private static Field defaultSessionField() throws Exception {
        Field sessionField = CassandraConnectionManagerImpl.class.getDeclaredField("session");
        sessionField.setAccessible(true);
        return sessionField;
    }

    private static CqlSession getDefaultSession() throws Exception {
        return (CqlSession) defaultSessionField().get(null);
    }

    private static void setDefaultSession(CqlSession value) throws Exception {
        defaultSessionField().set(null, value);
    }
}
