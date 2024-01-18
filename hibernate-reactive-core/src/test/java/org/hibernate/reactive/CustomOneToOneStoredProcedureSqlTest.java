/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.reactive.testing.ReactiveAssertions;
import org.hibernate.reactive.annotations.EnabledFor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(POSTGRESQL)
public class CustomOneToOneStoredProcedureSqlTest extends BaseReactiveTest {

	private IndividualPerson individualPerson;
	private DriverLicence driverLicence;
	private static final String INITIAL_LICENCE_NO = "12545KLI12";
	private static final String INSERT_DRIVER_LICENCE_SQL = "CREATE OR REPLACE FUNCTION PROC_INSERT_INDIVIDUAL_PERSON_DRIVER_LICENCE995 ( " +
			"   ID_PARAM IN bigint, " +
			"   TEXT_PARAM IN varchar(255), " +
			"   ID_PERSON_PARAM IN bigint " +
			"   ) " +
			" RETURNS void AS " +
			"$BODY$ " +
			"    BEGIN " +
			"        insert into DRIVER_LICENCE (individual_person_Id, id, licenceNo, updated) values (ID_PERSON_PARAM, ID_PARAM, TEXT_PARAM, localtimestamp); " +
			"    END; " +
			"$BODY$ " +
			"LANGUAGE plpgsql;";

	private static final String DELETE_DRIVER_LICENCE_SQL = "CREATE OR REPLACE FUNCTION PROC_DELETE_INDIVIDUAL_PERSON_DRIVER_LICENCE928 ( " +
			"   ID_PARAM IN bigint" +
			"   ) RETURNS void AS " +
			"$BODY$ " +
			"    BEGIN " +
			"        update DRIVER_LICENCE set deleted=localtimestamp where id=ID_PARAM; " +
			"    END; " +
			"$BODY$ " +
			"LANGUAGE plpgsql;";


	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( IndividualPerson.class, DriverLicence.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		individualPerson = new IndividualPerson();
		individualPerson.name = "Doruk";

		driverLicence = new DriverLicence();
		driverLicence.licenceNo = INITIAL_LICENCE_NO;
		driverLicence.individualPerson = individualPerson;

		test( context, openSession()
				.thenCompose( s -> s
						.createNativeQuery( INSERT_DRIVER_LICENCE_SQL ).executeUpdate()
						.thenCompose( v -> s.createNativeQuery( DELETE_DRIVER_LICENCE_SQL ).executeUpdate() )
						.thenCompose( v -> s.persist( individualPerson, driverLicence ) )
						.thenCompose( v -> s.flush() )
				)
		);
	}

	@Test
	public void testFailureWithGetSingleResultOrNull(VertxTestContext context) {
		test( context, ReactiveAssertions.assertThrown( HibernateException.class, getMutinySessionFactory()
						.withTransaction( s ->  s.createNativeQuery( INSERT_DRIVER_LICENCE_SQL ).getSingleResultOrNull() ) )
				.invoke( e -> assertThat( e ).hasMessageContainingAll( "HR000080:", INSERT_DRIVER_LICENCE_SQL ) )
		);
	}

	@Test
	public void testFailureWithGetSingleResult(VertxTestContext context) {
		test( context, ReactiveAssertions.assertThrown( HibernateException.class, getSessionFactory()
						.withTransaction( s ->  s.createNativeQuery( INSERT_DRIVER_LICENCE_SQL ).getSingleResult() ) )
				.thenAccept( e -> assertThat( e ).hasMessageContainingAll( "HR000080:", INSERT_DRIVER_LICENCE_SQL ) )
		);
	}

	@Test
	public void testInsertStoredProcedureDriverLicence(VertxTestContext context) {
		test( context, openSession().thenCompose( session -> session
				.find( DriverLicence.class, driverLicence.id )
				.thenAccept( Assertions::assertNotNull ) )
		);
	}


	@Test
	public void testDeleteStoredProcedure(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( DriverLicence.class, driverLicence.id )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( DriverLicence.class, driverLicence.id ) )
				.thenAccept( foundRecord -> {
					assertEquals( INITIAL_LICENCE_NO, foundRecord.licenceNo );
					assertNotNull( foundRecord.deleted );
					assertNotNull( foundRecord.updated );

				} )
		);
	}

	@Entity
	@Table(name = "DRIVER_LICENCE")
	@SQLInsert(sql = "SELECT PROC_INSERT_INDIVIDUAL_PERSON_DRIVER_LICENCE995( $1, $2, $3 );", callable = true)
	@SQLDelete(sql = "SELECT PROC_DELETE_INDIVIDUAL_PERSON_DRIVER_LICENCE928( $1 );", callable = true)
	public static class DriverLicence {
		@GeneratedValue
		@Id
		long id;

		@Basic(optional = false)
		String licenceNo;

		@OneToOne
		@JoinColumn(name = "individual_person_Id")
		IndividualPerson individualPerson;

		@Column(insertable = false, updatable = false, nullable = false)
		LocalDateTime updated;

		@Column(insertable = false, updatable = false)
		LocalDateTime deleted;

	}

	@Entity(name = "INDIVIDUAL_PERSON")
	@Table(name = "INDIVIDUAL_PERSON")
	public static class IndividualPerson {
		@GeneratedValue
		@Id
		long id;

		@Basic(optional = false)
		String name;

	}

}
