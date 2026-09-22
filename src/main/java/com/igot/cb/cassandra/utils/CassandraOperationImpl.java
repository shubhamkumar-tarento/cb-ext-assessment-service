package com.igot.cb.cassandra.utils;


import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.igot.cb.common.helper.cassandra.CassandraConnectionManager;
import com.igot.cb.common.model.SBApiResponse;
import com.igot.cb.common.util.Constants;
import com.igot.cb.core.exception.ApplicationLogicError;

@Component
public class CassandraOperationImpl implements CassandraOperation {
	private Logger logger = LoggerFactory.getLogger(CassandraOperationImpl.class);

	final CassandraConnectionManager connectionManager;

	public CassandraOperationImpl(CassandraConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

	@Override
	public List<Map<String, Object>> getRecordsByProperties(String keyspaceName, String tableName,
															Map<String, Object> propertyMap, List<String> fields) {
		List<Map<String, Object>> response = new ArrayList<>();
		CqlSession session = null;
		try {
			session = connectionManager.getSession(keyspaceName);
			Select selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);
			ResultSet results = session.execute(selectQuery.build());
			response = CassandraUtil.createResponse(results);
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return response;
	}

	public int getCountByProperties(String keyspaceName, String tableName, Map<String, Object> propertyMap) {
		int count = 0;
		CqlSession session = null;
		try {
			session = connectionManager.getSession(keyspaceName);
			Select selectQuery = selectFrom(keyspaceName, tableName).countAll().where();
			propertyMap.forEach((key, value) -> selectQuery.whereColumn(key).isEqualTo(bindMarker()));
			PreparedStatement preparedStatement = session.prepare(selectQuery.build());
			BoundStatement boundStatement = preparedStatement.bind(propertyMap.values().toArray());
			ResultSet resultSet = session.execute(boundStatement);
			Row row = resultSet.one();
			if (row != null) {
				count = (int) row.getLong(0);
			}
		} catch (Exception e) {
			logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
		}
		return count;
	}

	private Select processQuery(String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields) {
		Select selectFrom;
		if (CollectionUtils.isNotEmpty(fields)) {
			selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields.toArray(new String[0]));
		} else {
			selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName).all();
		}

		Select selectQuery = selectFrom.all();
		if (MapUtils.isNotEmpty(propertyMap)) {
			for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
				if (entry.getValue() instanceof List) {
					List<?> list = (List<?>) entry.getValue();
					if (CollectionUtils.isNotEmpty(list)) {
						List<Term> terms = list.stream()
								.map(QueryBuilder::literal)
								.collect(Collectors.toList());
						selectQuery = selectQuery.whereColumn(entry.getKey()).in(terms);
					}
				} else {
					selectQuery = selectQuery.whereColumn(entry.getKey()).isEqualTo(QueryBuilder.literal(entry.getValue()));
				}
			}
			selectQuery = selectQuery.allowFiltering();
		}

		return selectQuery;
	}

	public SBApiResponse insertRecord(String keyspaceName, String tableName, Map<String, Object> request) {
		SBApiResponse response = new SBApiResponse();
		CqlSession session = null;
		try {
			session = connectionManager.getSession(keyspaceName);
			String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, request);
			PreparedStatement statement = session.prepare(query);
			BoundStatement boundStatement = statement.bind(request.values().toArray());
			session.execute(boundStatement);
			response.put("STATUS", "SUCCESS");
		} catch (Exception e) {
			String errorMessage = String.format("Exception occurred while inserting record to %s %s", tableName, e.getMessage());
			logger.error(errorMessage, e);
			response.put("STATUS", "FAILED");
		}
		return response;
	}

	@Override
	public List<Map<String, Object>> getRecordsByPropertiesWithoutFiltering(String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields) {
		List<Map<String, Object>> response = new ArrayList<>();
		CqlSession session = null;
		try {
			session = connectionManager.getSession(keyspaceName);
			Select selectQuery = null;
			selectQuery = processQueryWithoutFiltering(keyspaceName, tableName, propertyMap, fields);

			String queryString = selectQuery.toString();
			SimpleStatement statement = SimpleStatement.newInstance(queryString);
			ResultSet results = session.execute(statement);
			response = CassandraUtil.createResponse(results);
		} catch (Exception e) {
			logger.error("Error fetching records from {}: {}", tableName, e.getMessage());
		}
		return response;
	}

	public Map<String, Object> getRecordsByPropertiesWithPagination() {
		return Map.of();
	}

	public Map<String, Object> updateRecord(String keyspaceName, String tableName, Map<String, Object> updateAttributes,
											Map<String, Object> compositeKey) {
		Map<String, Object> response = new HashMap<>();
		CqlSession session = null;
		try {
			session = connectionManager.getSession(keyspaceName);
			UpdateStart updateStart = QueryBuilder.update(keyspaceName, tableName);
			UpdateWithAssignments updateWithAssignments = updateStart.set(updateAttributes.entrySet().stream()
					.map(entry -> Assignment.setColumn(entry.getKey(), QueryBuilder.literal(entry.getValue())))
					.toArray(Assignment[]::new));
			Update update = updateWithAssignments.where(compositeKey.entrySet().stream()
					.map(entry -> Relation.column(entry.getKey()).isEqualTo(QueryBuilder.literal(entry.getValue())))
					.toArray(Relation[]::new));
			SimpleStatement statement = update.build();
			session.execute(statement);
			response.put(Constants.RESPONSE, Constants.SUCCESS);
		} catch (Exception e) {
			String errMsg = String.format("Exception occurred while updating record to %s: %s", tableName, e.getMessage());
			response.put(Constants.RESPONSE, Constants.FAILED);
			response.put(Constants.ERROR_MESSAGE, errMsg);
			throw new ApplicationLogicError(errMsg, e);
		}
		return response;
	}

	private Select processQueryWithoutFiltering(String keyspaceName, String tableName, Map<String, Object> propertyMap,
			List<String> fields) {
		SelectFrom from = QueryBuilder.selectFrom(keyspaceName, tableName);

		Select selectQuery = CollectionUtils.isNotEmpty(fields)
				? from.columns(fields.toArray(new String[0]))
				: from.all();

		if (MapUtils.isNotEmpty(propertyMap)) {
			for (Map.Entry<String, Object> e : propertyMap.entrySet()) {
				Object v = e.getValue();
				if (v instanceof List && CollectionUtils.isNotEmpty((List<?>) v)) {
					List<Term> terms = ((List<?>) v).stream()
							.map(QueryBuilder::literal)
							.collect(Collectors.toList());
					selectQuery = selectQuery.whereColumn(e.getKey()).in(terms);
				} else {
					selectQuery = selectQuery.whereColumn(e.getKey())
							.isEqualTo(QueryBuilder.literal(v));
				}
			}
		}
		return selectQuery;
	}
}

