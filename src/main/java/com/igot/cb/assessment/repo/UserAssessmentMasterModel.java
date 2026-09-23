package com.igot.cb.assessment.repo;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.math.BigDecimal;
import java.util.Date;

@Getter
@Setter
@Builder(builderClassName = "Builder")
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Table("user_assessment_master")
public class UserAssessmentMasterModel {

	@PrimaryKey
	private UserAssessmentMasterPrimaryKeyModel primaryKey;

	@Column("correct_count")
	private Integer correctCount;
	@Column("date_created")
	private Date dateCreated;
	@Column("incorrect_count")
	private Integer incorrectCount;
	@Column("not_answered_count")
	private Integer notAnsweredCount;
	@Column("parent_content_type")
	private String parentContentType;
	@Column("pass_percent")
	private BigDecimal passPercent;
	@Column("source_id")
	private String sourceId;
	@Column("source_title")
	private String sourceTitle;
	@Column("user_id")
	private String userId;

	@Override
	public String toString() {
		return "UserAssessmentMasterModel [primaryKey=" + primaryKey + ", correctCount=" + correctCount
				+ ", dateCreated=" + dateCreated + ", incorrectCount=" + incorrectCount + ", notAnsweredCount="
				+ notAnsweredCount + ", parentContentType=" + parentContentType + ", passPercent=" + passPercent
				+ ", sourceId=" + sourceId + ", sourceTitle=" + sourceTitle + ", userId=" + userId + "]";
	}

}
