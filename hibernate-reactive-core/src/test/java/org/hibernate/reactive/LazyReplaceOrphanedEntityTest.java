/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.DiscriminatorColumn;
import jakarta.persistence.DiscriminatorType;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;

import static org.assertj.core.api.Assertions.assertThat;

public class LazyReplaceOrphanedEntityTest extends BaseReactiveTest {

	private Campaign theCampaign;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Campaign.class, ExecutionDate.class, Schedule.class );
	}

	private Uni<Void> populateDb() {
		theCampaign = new Campaign();
		theCampaign.setSchedule( new ExecutionDate(OffsetDateTime.now(), "ALPHA") );
		return getMutinySessionFactory().withTransaction( (s, t) -> s.persist( theCampaign ) );
	}

	@Test
	public void testUpdateScheduleChange(VertxTestContext context) {
		test( context, populateDb()
				.call( () -> getMutinySessionFactory()
				.withSession( session -> session
						.find( Campaign.class, theCampaign.getId() )
						.invoke( foundCampaign -> foundCampaign
								.setSchedule( new ExecutionDate( OffsetDateTime.now(), "BETA" ) ) )
						.call( session::flush ) ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Campaign.class, theCampaign.getId() ) )
				.invoke( updatedCampaign -> assertThat( updatedCampaign.getSchedule().getCodeName() )
						.isNotEqualTo( theCampaign.getSchedule().getCodeName() ) )
		);
	}

	@Test
	public void testUpdateWithMultipleScheduleChanges(VertxTestContext context) {
		test( context, populateDb()
				.call( () -> getMutinySessionFactory()
				.withSession( session -> session
						.find( Campaign.class, theCampaign.getId() )
						.invoke( foundCampaign -> foundCampaign
								.setSchedule( new ExecutionDate( OffsetDateTime.now(), "BETA" ) ) )
						.call( session::flush ) ) )
				.call( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Campaign.class, theCampaign.getId() )
								.invoke( foundCampaign -> foundCampaign
										.setSchedule( new ExecutionDate( OffsetDateTime.now(), "GAMMA" ) ) )
								.call( session::flush )
						) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Campaign.class, theCampaign.getId() ) )
				.invoke( updatedCampaign -> assertThat(
						updatedCampaign.getSchedule().getCodeName() )
						.isNotEqualTo( theCampaign.getSchedule().getCodeName() )
				)
		);
	}

	@Entity (name="Campaign")
	public static class Campaign implements Serializable {

		@Id @GeneratedValue
		private Integer id;

		@OneToOne(mappedBy = "campaign",  cascade = CascadeType.ALL, fetch = FetchType.LAZY, orphanRemoval = true)
		public Schedule schedule;

		public Campaign() {
		}

		// Getters and setters
		public void setSchedule(Schedule schedule) {
			this.schedule = schedule;
			if ( schedule != null ) {
				this.schedule.setCampaign( this );
			}
		}

		public Schedule getSchedule() {
			return this.schedule;
		}

		public Integer getId() {
			return id;
		}
	}

	@Entity (name="Schedule")
	@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
	@DiscriminatorColumn(name = "schedule_type", discriminatorType = DiscriminatorType.STRING)
	public static abstract class Schedule implements Serializable {
		@Id
		@Column(name = "id")
		private String id = UUID.randomUUID().toString();

		@Column(name = "code_name")
		private String code_name;

		@OneToOne
		@JoinColumn(name = "campaign_id")
		private Campaign campaign;

		// Getters and setters
		public String getId() {
			return id;
		}

		public void setCampaign(Campaign campaign) {
			this.campaign = campaign;
		}

		public Campaign getCampaign() {
			return campaign;
		}

		public void setCodeName(String code_name) {
			this.code_name = code_name;
		}

		public String getCodeName() {
			return code_name;
		}
	}

	@Entity (name="ExecutionDate")
	@DiscriminatorValue("EXECUTION_DATE")
	public static class ExecutionDate extends Schedule {

		@Column(name = "start_date")
		private OffsetDateTime start;

		public ExecutionDate() {
		}

		public ExecutionDate( OffsetDateTime start, String code_name ) {
			this.start = start;
			setCodeName( code_name );
		}

		// Getters and setters

		public void setStart(OffsetDateTime start) {
			this.start = start;
		}

		public OffsetDateTime getStart() {
			return start;
		}
	}

}
