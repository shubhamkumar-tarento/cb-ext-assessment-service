package com.igot.cb.assessment.repo;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

@Getter
@Setter
@PrimaryKeyClass
public class UserQuizMasterPrimaryKeyModel {
	private static final long serialVersionUID = 1L;

	@PrimaryKeyColumn(name = "root_org", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private String rootOrg;

	@PrimaryKeyColumn(name = "ts_created", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
	private Date tsCreated;

	@PrimaryKeyColumn(name = "result_percent", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
	private BigDecimal resultPercent;

	@PrimaryKeyColumn(name = "id", ordinal = 3, type = PrimaryKeyType.CLUSTERED)
	private UUID id;

	public UserQuizMasterPrimaryKeyModel() {
		super();
	}

	public UserQuizMasterPrimaryKeyModel(String rootOrg, Date tsCreated, BigDecimal resultPercent, UUID id) {
		this.rootOrg = rootOrg;
		this.tsCreated = tsCreated;
		this.resultPercent = resultPercent;
		this.id = id;
	}

	@Override
	public String toString() {
		return "UserQuizMasterPrimaryKeyModel [rootOrg=" + rootOrg + ", tsCreated=" + tsCreated + ", resultPercent="
				+ resultPercent + ", id=" + id + "]";
	}
}
