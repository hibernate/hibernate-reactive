/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.UUID;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;

public class LazyReplaceOrphanedEntityTest extends BaseReactiveTest {

	private Campaign theCampaign;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Campaign.class );
		configuration.addAnnotatedClass( ExecutionDate.class );
		configuration.addAnnotatedClass( Schedule.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		theCampaign = new Campaign();
		theCampaign.setSchedule( new ExecutionDate(OffsetDateTime.now(), "ALPHA") );

		test( context, getMutinySessionFactory().withTransaction( (s, t) -> s.persist( theCampaign ) ) );
	}

	@After
	public void cleanDB(TestContext context) {
		test( context, deleteEntities( "Campaign", "Schedule" ) );
	}

	@Test
	public void testUpdateScheduleChange(TestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( session -> session
						.find( Campaign.class, theCampaign.getId() )
						.invoke( foundCampaign -> foundCampaign
								.setSchedule( new ExecutionDate( OffsetDateTime.now(), "BETA" ) ) )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( Campaign.class, theCampaign.getId() ) )
				.invoke( updatedCampaign -> assertThat( updatedCampaign.getSchedule().getCodeName() )
						.isNotEqualTo( theCampaign.getSchedule().getCodeName() ) )
		);
	}

	@Test
	public void testUpdateWithMultipleScheduleChanges(TestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( session -> session
						.find( Campaign.class, theCampaign.getId() )
						.invoke( foundCampaign -> foundCampaign
								.setSchedule( new ExecutionDate( OffsetDateTime.now(), "BETA" ) ) )
						.call( session::flush ) )
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
			if( schedule != null ) {
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

		public Schedule setStart(OffsetDateTime start) {
			this.start = start;
			return null;
		}

		public OffsetDateTime getStart() {
			return start;
		}
	}

}
