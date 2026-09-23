package com.igot.cb.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class SunbirdConfigTest {

    /**
     * Supplies the local data center (not configured on the base class) and a standalone
     * converter so the configuration can be exercised without a Spring context.
     */
    static class TestSunbirdConfig extends SunbirdConfig {
        private final MappingCassandraConverter converter = new MappingCassandraConverter();

        @Override
        protected String getLocalDataCenter() {
            return "datacenter1";
        }

        @Override
        public CassandraConverter cassandraConverter() {
            return converter;
        }
    }

    private TestSunbirdConfig config;

    @BeforeEach
    void setUp() {
        config = new TestSunbirdConfig();
        config.setContactPoints("127.0.0.1, 127.0.0.2");
        config.setPort(9042);
        config.setKeyspaceName("sunbird");
    }

    private void setCredentials(String user, String password) {
        ReflectionTestUtils.setField(config, "sunbirdUser", user);
        ReflectionTestUtils.setField(config, "sunbirdPassword", password);
    }

    private MockedConstruction<CqlSessionBuilder> mockBuilder(CqlSession session) {
        return mockConstruction(CqlSessionBuilder.class, withSettings().defaultAnswer(RETURNS_SELF),
                (mock, ctx) -> doReturn(session).when(mock).build());
    }

    @Test
    void testLogProperties() {
        setCredentials("cassandra", "secret");
        assertDoesNotThrow(config::logProperties);
    }

    @Test
    void testCqlSession_WithCredentials() {
        setCredentials("cassandra", "secret");
        CqlSession session = mock(CqlSession.class);
        try (MockedConstruction<CqlSessionBuilder> builders = mockBuilder(session)) {
            CqlSession result = config.cqlSession();

            assertSame(session, result);
            CqlSessionBuilder builder = builders.constructed().get(0);
            verify(builder).addContactPoint(new InetSocketAddress("127.0.0.1", 9042));
            verify(builder).addContactPoint(new InetSocketAddress("127.0.0.2", 9042));
            verify(builder).withLocalDatacenter("datacenter1");
            verify(builder).withKeyspace("sunbird");
            verify(builder).withAuthCredentials("cassandra", "secret");
        }
    }

    @Test
    void testCqlSession_WithoutUser_SkipsCredentials() {
        setCredentials("", "secret");
        CqlSession session = mock(CqlSession.class);
        try (MockedConstruction<CqlSessionBuilder> builders = mockBuilder(session)) {
            assertSame(session, config.cqlSession());
            verify(builders.constructed().get(0), never()).withAuthCredentials(anyString(), anyString());
        }
    }

    @Test
    void testCqlSession_WithoutPassword_SkipsCredentials() {
        setCredentials("cassandra", "");
        CqlSession session = mock(CqlSession.class);
        try (MockedConstruction<CqlSessionBuilder> builders = mockBuilder(session)) {
            assertSame(session, config.cqlSession());
            verify(builders.constructed().get(0), never()).withAuthCredentials(anyString(), anyString());
        }
    }

    @Test
    void testCassandraTemplate() {
        CqlSession session = mock(CqlSession.class);

        CassandraAdminTemplate template = config.cassandraTemplate(session);

        assertNotNull(template);
        assertSame(config.cassandraConverter(), template.getConverter());
    }

    @Test
    void testCqlSession_DefaultsLocalDataCenterWhenUnset() {
        // AbstractSessionConfiguration#getLocalDataCenter() defaults to "datacenter1" when
        // no value is bound, so Objects.requireNonNull(getLocalDataCenter()) never throws
        // here; the builder still receives that default.
        SunbirdConfig plainConfig = new SunbirdConfig();
        plainConfig.setContactPoints("127.0.0.1");
        plainConfig.setPort(9042);
        ReflectionTestUtils.setField(plainConfig, "sunbirdUser", "");
        ReflectionTestUtils.setField(plainConfig, "sunbirdPassword", "");
        CqlSession session = mock(CqlSession.class);
        try (MockedConstruction<CqlSessionBuilder> builders = mockBuilder(session)) {
            assertSame(session, plainConfig.cqlSession());
            verify(builders.constructed().get(0)).addContactPoint(any(InetSocketAddress.class));
            verify(builders.constructed().get(0)).withLocalDatacenter("datacenter1");
        }
    }
}
