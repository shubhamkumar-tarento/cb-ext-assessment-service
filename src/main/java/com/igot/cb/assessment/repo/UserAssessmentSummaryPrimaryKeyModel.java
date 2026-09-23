package com.igot.cb.assessment.repo;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

import java.io.Serializable;

@Getter
@Setter
@PrimaryKeyClass
public class UserAssessmentSummaryPrimaryKeyModel implements Serializable {
	private static final long serialVersionUID = 1L;

	@PrimaryKeyColumn(name = "root_org", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private String rootOrg;

	@PrimaryKeyColumn(name = "user_id", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
	private String userId;

	@PrimaryKeyColumn(name = "content_id", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
	private String contentId;

	public UserAssessmentSummaryPrimaryKeyModel() {
		super();
	}

	public UserAssessmentSummaryPrimaryKeyModel(String rootOrg, String userId, String contentId) {
		this.rootOrg = rootOrg;
		this.userId = userId;
		this.contentId = contentId;
	}

	@Override
	public String toString() {
		return "UserAssessmentSummaryPrimaryKeyModel [rootOrg=" + rootOrg + ", userId=" + userId + ", contentId="
				+ contentId + "]";
	}
}
