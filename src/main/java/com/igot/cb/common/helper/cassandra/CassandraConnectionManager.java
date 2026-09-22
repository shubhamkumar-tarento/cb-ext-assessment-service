package com.igot.cb.common.helper.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Interface for cassandra connection manager , implementation would be Standalone and Embedde
 * cassandra connection manager .
 */
public interface CassandraConnectionManager {
  CqlSession getSession(String keyspaceName);
}