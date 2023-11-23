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
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.reactive.testing.DBSelectionExtension;
import org.hibernate.reactive.testing.ReactiveAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.DBSelectionExtension.runOnlyFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Timeout(value = 10, timeUnit = MINUTES)

public class CustomStoredProcedureSqlTest extends BaseReactiveTest {

	@RegisterExtension
	public DBSelectionExtension dbSelection = runOnlyFor( POSTGRESQL );

	private SimpleRecord theRecord;

	private static final String INITIAL_TEXT = "blue suede shoes";
	private static final String UPDATED_TEXT = "ruby slippers";

	private static final String INSERT_SP_SQL = "CREATE OR REPLACE FUNCTION PROC_INSERT_RECORD ( " +
			"   TEXT_PARAM IN varchar(255), " +
			"   ID_PARAM IN bigint" +
			"   ) " +
			" RETURNS void AS " +
			"$BODY$ " +
			"    BEGIN " +
			"        insert into simplerecord (text, updated, id) values (TEXT_PARAM, localtimestamp, ID_PARAM); " +
			"    END; " +
			"$BODY$ " +
			"LANGUAGE plpgsql;";

	private static final String UPDATE_SP_SQL = "CREATE OR REPLACE FUNCTION PROC_UPDATE_RECORD ( " +
			"   TEXT_PARAM IN varchar(255), " +
			"   ID_PARAM IN bigint" +
			"   ) RETURNS void AS " +
			"$BODY$ " +
			"    BEGIN " +
			"        update simplerecord set text=TEXT_PARAM, updated=localtimestamp where id=ID_PARAM; " +
			"    END; " +
			"$BODY$ " +
			"LANGUAGE plpgsql;";

	private static final String DELETE_SP_SQL = "CREATE OR REPLACE FUNCTION PROC_DELETE_RECORD ( " +
			"   ID_PARAM IN bigint" +
			"   ) RETURNS void AS " +
			"$BODY$ " +
			"    BEGIN " +
			"        update simplerecord set deleted=localtimestamp where id=ID_PARAM; " +
			"    END; " +
			"$BODY$ " +
			"LANGUAGE plpgsql;";


	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( SimpleRecord.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		theRecord = new SimpleRecord();
		theRecord.text = INITIAL_TEXT;

		test( context, openSession()
				.thenCompose( s -> s
						.createNativeQuery( INSERT_SP_SQL ).executeUpdate()
						.thenCompose( v -> s.createNativeQuery( UPDATE_SP_SQL ).executeUpdate() )
						.thenCompose( v -> s.createNativeQuery( DELETE_SP_SQL ).executeUpdate() )
						.thenCompose( v -> s.persist( theRecord ) )
						.thenCompose( v -> s.flush() )
				)
		);
	}

	@Test
	public void testFailureWithGetSingleResultOrNull(VertxTestContext context) {
		test( context, ReactiveAssertions.assertThrown( HibernateException.class, getMutinySessionFactory()
				.withTransaction( s ->  s.createNativeQuery( INSERT_SP_SQL ).getSingleResultOrNull() ) )
				.invoke( e -> assertThat( e ).hasMessageContainingAll( "HR000080:", INSERT_SP_SQL ) )
		);
	}

	@Test
	public void testFailureWithGetSingleResult(VertxTestContext context) {
		test( context, ReactiveAssertions.assertThrown( HibernateException.class, getSessionFactory()
						.withTransaction( s ->  s.createNativeQuery( INSERT_SP_SQL ).getSingleResult() ) )
				.thenAccept( e -> assertThat( e ).hasMessageContainingAll( "HR000080:", INSERT_SP_SQL ) )
		);
	}

	@Test
	public void testInsertStoredProcedure(VertxTestContext context) {
		test( context, openSession().thenCompose( session -> session
				.find( SimpleRecord.class, theRecord.id )
				.thenAccept( Assertions::assertNotNull ) )
		);
	}

	@Test
	public void testUpdateStoredProcedure(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( SimpleRecord.class, theRecord.id )
						.thenAccept( foundRecord -> foundRecord.text = UPDATED_TEXT )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( SimpleRecord.class, theRecord.id ) )
				.thenAccept( foundRecord -> {
					assertEquals( UPDATED_TEXT, foundRecord.text );
					assertNotNull( foundRecord.updated );
					assertNull( foundRecord.deleted );
				} )
		);
	}

	@Test
	public void testDeleteStoredProcedure(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( SimpleRecord.class, theRecord.id )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( SimpleRecord.class, theRecord.id ) )
				.thenAccept( foundRecord -> {
					assertEquals( INITIAL_TEXT, foundRecord.text );
					assertNotNull( foundRecord.updated );
					assertNotNull( foundRecord.deleted );
				} )
		);
	}

	@Entity(name = "SimpleRecord")
	@Table(name = "SimpleRecord")
	@SQLInsert(sql = "SELECT PROC_INSERT_RECORD( $1, $2 );", callable = true)
	@SQLUpdate(sql = "SELECT PROC_UPDATE_RECORD( $1, $2 );", callable = true)
	@SQLDelete(sql = "SELECT PROC_DELETE_RECORD( $1 );", callable = true)
	static class SimpleRecord {
		@GeneratedValue @Id long id;
		@Basic(optional = false) String text;
		@Column(insertable = false, updatable = false, nullable = false) LocalDateTime updated;
		@Column(insertable = false, updatable = false) LocalDateTime deleted;
	}
}
