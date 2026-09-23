package com.igot.cb.assessment.repo;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

@Getter
@Setter
@PrimaryKeyClass
public class UserAssessmentMasterPrimaryKeyModel implements Serializable {

	private static final long serialVersionUID = 1L;

	@PrimaryKeyColumn(name = "root_org", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private String rootOrg;

	@PrimaryKeyColumn(name = "ts_created", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
	private Date tsCreated;

	@PrimaryKeyColumn(name = "parent_source_id", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
	private String parentSourceId;

	@PrimaryKeyColumn(name = "result_percent", ordinal = 3, type = PrimaryKeyType.CLUSTERED)
	private BigDecimal resultPercent;

	@PrimaryKeyColumn(name = "id", ordinal = 3, type = PrimaryKeyType.CLUSTERED)
	private UUID id;

	public UserAssessmentMasterPrimaryKeyModel() {
		super();
	}

	public UserAssessmentMasterPrimaryKeyModel(String rootOrg, Date tsCreated, String parentSourceId,
                                               BigDecimal resultPercent, UUID id) {
		this.rootOrg = rootOrg;
		this.tsCreated = tsCreated;
		this.parentSourceId = parentSourceId;
		this.resultPercent = resultPercent;
		this.id = id;
	}

	@Override
	public String toString() {
		return "UserAssessmentMasterPrimaryKeyModel [rootOrg=" + rootOrg + ", tsCreated=" + tsCreated
				+ ", parentSourceId=" + parentSourceId + ", resultPercent=" + resultPercent + ", id=" + id + "]";
	}

}
