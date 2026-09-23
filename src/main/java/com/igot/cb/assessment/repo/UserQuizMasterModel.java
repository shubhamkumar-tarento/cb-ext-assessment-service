package com.igot.cb.assessment.repo;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.math.BigDecimal;
import java.util.Date;

@Getter
@Setter
@Table("user_quiz_master")
public class UserQuizMasterModel {

	@PrimaryKey
	private UserQuizMasterPrimaryKeyModel primaryKey;

	@Column("correct_count")
	private Integer correctCount;
	@Column("date_created")
	private Date dateCreated;
	@Column("incorrect_count")
	private Integer incorrectCount;
	@Column("not_answered_count")
	private Integer notAnsweredCount;
	@Column("pass_percent")
	private BigDecimal passPercent;
	@Column("source_id")
	private String sourceId;
	@Column("source_title")
	private String sourceTitle;
	@Column("user_id")
	private String userId;

	private UserQuizMasterModel(Builder builder) {
		this.primaryKey = builder.primaryKey;
		this.correctCount = builder.correctCount;
		this.dateCreated = builder.dateCreated;
		this.incorrectCount = builder.incorrectCount;
		this.notAnsweredCount = builder.notAnsweredCount;
		this.passPercent = builder.passPercent;
		this.sourceId = builder.sourceId;
		this.sourceTitle = builder.sourceTitle;
		this.userId = builder.userId;
	}

	public UserQuizMasterModel() {
		super();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private UserQuizMasterPrimaryKeyModel primaryKey;
		private Integer correctCount;
		private Date dateCreated;
		private Integer incorrectCount;
		private Integer notAnsweredCount;
		private BigDecimal passPercent;
		private String sourceId;
		private String sourceTitle;
		private String userId;

		public Builder primaryKey(UserQuizMasterPrimaryKeyModel primaryKey) {
			this.primaryKey = primaryKey;
			return this;
		}

		public Builder correctCount(Integer correctCount) {
			this.correctCount = correctCount;
			return this;
		}

		public Builder dateCreated(Date dateCreated) {
			this.dateCreated = dateCreated;
			return this;
		}

		public Builder incorrectCount(Integer incorrectCount) {
			this.incorrectCount = incorrectCount;
			return this;
		}

		public Builder notAnsweredCount(Integer notAnsweredCount) {
			this.notAnsweredCount = notAnsweredCount;
			return this;
		}

		public Builder passPercent(BigDecimal passPercent) {
			this.passPercent = passPercent;
			return this;
		}

		public Builder sourceId(String sourceId) {
			this.sourceId = sourceId;
			return this;
		}

		public Builder sourceTitle(String sourceTitle) {
			this.sourceTitle = sourceTitle;
			return this;
		}

		public Builder userId(String userId) {
			this.userId = userId;
			return this;
		}

		public UserQuizMasterModel build() {
			return new UserQuizMasterModel(this);
		}
	}

	@Override
	public String toString() {
		return "UserQuizMasterModel [primaryKey=" + primaryKey + ", correctCount=" + correctCount + ", dateCreated="
				+ dateCreated + ", incorrectCount=" + incorrectCount + ", notAnsweredCount=" + notAnsweredCount
				+ ", passPercent=" + passPercent + ", sourceId=" + sourceId + ", sourceTitle=" + sourceTitle
				+ ", userId=" + userId + "]";
	}
}
