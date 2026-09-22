package com.igot.cb.cassandra.utils;


import com.igot.cb.common.model.SBApiResponse;

import java.util.List;
import java.util.Map;

/**
 * @author fathima
 * @desc this interface will hold functions for cassandra db interaction
 */
public interface CassandraOperation {

	SBApiResponse insertRecord(String keyspaceName, String tableName, Map<String, Object> request);

	List<Map<String, Object>> getRecordsByPropertiesWithoutFiltering(String keyspaceName, String tableName,
																	 Map<String, Object> propertyMap, List<String> fields);

	Map<String, Object> updateRecord(String keyspaceName, String tableName, Map<String, Object> updateAttributes,
									 Map<String, Object> compositeKey);

	/**
	 * Retrieves records from Cassandra based on specified properties and key.
	 *
	 * @param keyspaceName The name of the keyspace containing the table.
	 * @param tableName    The name of the table from which to retrieve records.
	 * @param propertyMap  A map representing properties to filter records.
	 * @param fields       A list of fields to include in the retrieved records.
	 * @return A list of maps representing the retrieved records.
	 */

	List<Map<String, Object>> getRecordsByProperties(String keyspaceName, String tableName,
													 Map<String, Object> propertyMap, List<String> fields);

}

