/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

public class ManyToOneMergeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( AcademicYearDetailsDBO.class, CampusDBO.class );
	}

	@Before
	public void populateDb(TestContext context) {
		CampusDBO campusDBO2 = new CampusDBO();
		campusDBO2.setId( 42 );
		campusDBO2.setCampusName( "Kuchl" );

		CampusDBO campusDBO = new CampusDBO();
		campusDBO.setId( 66 );
		campusDBO.setCampusName( "Qualunquelandia" );

		AcademicYearDetailsDBO academicYearDetailsDBO = new AcademicYearDetailsDBO();
		academicYearDetailsDBO.setId( 69 );
		academicYearDetailsDBO.setCampusDBO( campusDBO );
		academicYearDetailsDBO.setCreatedUsersId( 12 );
		academicYearDetailsDBO.setRecordStatus( 'F' );
		academicYearDetailsDBO.setModifiedUsersId( 66 );
		test( context, getMutinySessionFactory().withTransaction( session -> session.persistAll(
				campusDBO, campusDBO2, academicYearDetailsDBO
		) ) );
	}

	@Test
	public void test(TestContext context) {
		test( context, getMutinySessionFactory().withSession( session -> session
						.createQuery( "select dbo from AcademicYearDetailsDBO dbo", AcademicYearDetailsDBO.class )
						.getSingleResult() )
				.invoke( dbo -> dbo.setRecordStatus( 'A' ) )
				.chain( dbo -> getMutinySessionFactory().withTransaction( session -> {
					dbo.setCampusDBO( session.getReference( CampusDBO.class, 42 ) );
					return session.merge( dbo );
				} ) )
				.chain( merged -> getMutinySessionFactory()
						.withTransaction( session -> session.fetch( merged.getCampusDBO() ) ) )
				.invoke( fetched -> context.assertEquals( "Kuchl", fetched.getCampusName() ) )
		);
	}

	@Test
	public void testTransient(TestContext context) {
		CampusDBO campusDBO = new CampusDBO();
		campusDBO.setId( 77 );
		campusDBO.setCampusName( "Qualunquelandia" );

		AcademicYearDetailsDBO dbo = new AcademicYearDetailsDBO();
		dbo.setId( 88 );
		dbo.setCampusDBO( campusDBO );
		dbo.setCreatedUsersId( 12 );
		dbo.setRecordStatus( 'A' );
		dbo.setModifiedUsersId( 66 );

		test( context, getMutinySessionFactory().withTransaction( session -> {
						  dbo.setCampusDBO( session.getReference( CampusDBO.class, 42 ) );
						  return session.merge( dbo );
					  } )
					  .chain( merged -> getMutinySessionFactory()
							  .withTransaction( session -> session.fetch( merged.getCampusDBO() ) ) )
					  .invoke( fetched -> context.assertEquals( "Kuchl", fetched.getCampusName() ) )
		);
	}

	@Entity(name = "AcademicYearDetailsDBO")
	@Table(name = "erp_academic_year_detail")
	static class AcademicYearDetailsDBO implements Serializable {
		@Id
		@Column(name = "erp_academic_year_detail_id")
		private Integer id;

		@ManyToOne(fetch = FetchType.EAGER)
		@JoinColumn(name = "erp_campus_id")
		private CampusDBO campusDBO;

		@Column(name = "record_status")
		private char recordStatus;

		@Column(name = "created_users_id")
		private Integer createdUsersId;

		@Column(name = "modified_users_id")
		private Integer modifiedUsersId;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public CampusDBO getCampusDBO() {
			return campusDBO;
		}

		public void setCampusDBO(CampusDBO campusDBO) {
			this.campusDBO = campusDBO;
		}

		public char getRecordStatus() {
			return recordStatus;
		}

		public void setRecordStatus(char recordStatus) {
			this.recordStatus = recordStatus;
		}

		public Integer getCreatedUsersId() {
			return createdUsersId;
		}

		public void setCreatedUsersId(Integer createdUsersId) {
			this.createdUsersId = createdUsersId;
		}

		public Integer getModifiedUsersId() {
			return modifiedUsersId;
		}

		public void setModifiedUsersId(Integer modifiedUsersId) {
			this.modifiedUsersId = modifiedUsersId;
		}
	}

	@Entity(name = "CampusDBO")
	@Table(name = "erp_campus")
	static class CampusDBO implements Serializable {
		@Id
		@Column(name = "erp_campus_id")
		private Integer id;

		@Column(name = "campus_name")
		private String campusName;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getCampusName() {
			return campusName;
		}

		public void setCampusName(String campusName) {
			this.campusName = campusName;
		}
	}
}
