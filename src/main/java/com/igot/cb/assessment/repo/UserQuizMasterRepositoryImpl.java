package com.igot.cb.assessment.repo;

import org.springframework.data.cassandra.core.CassandraBatchOperations;
import org.springframework.data.cassandra.core.CassandraOperations;

public class UserQuizMasterRepositoryImpl implements UserQuizMasterRepositoryCustom {

	CassandraOperations cassandraOperations;

	public UserQuizMasterRepositoryImpl(CassandraOperations cassandraOperations) {
	    this.cassandraOperations = cassandraOperations;
	}
	
	@Override
	public UserQuizMasterModel updateQuiz(UserQuizMasterModel quiz, UserQuizSummaryModel quizSummary) {
		CassandraBatchOperations batchOps = cassandraOperations.batchOps();
		batchOps.insert(quiz);
		batchOps.insert(quizSummary);
		batchOps.execute();
		return quiz;
	}
}
