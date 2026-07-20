/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.ReactiveAssertions;
import org.hibernate.reactive.testing.SqlStatementTracker;
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
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(POSTGRESQL)
public class CustomStoredProcedureSqlTest extends BaseReactiveTest {

	private SimpleRecord theRecord;

	// Tracks the SQL statements emitted by Hibernate so that the tests can assert
	// the custom @SQLInsert / @SQLUpdate / @SQLDelete queries are actually used.
	// Static because BaseReactiveTest reuses one SessionFactory across test methods,
	// so constructConfiguration() (which creates the tracker) runs only once per class.
	private static SqlStatementTracker sqlTracker;

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

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		// Collect only the custom @SQL* calls (the "SELECT PROC_*" statements),
		// excluding the "CREATE OR REPLACE FUNCTION" statements used to set up
		// the stored procedures and any other framework-issued query.
		sqlTracker = new SqlStatementTracker(
				CustomStoredProcedureSqlTest::isCustomProcedureCall,
				configuration.getProperties()
		);
		return configuration;
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean isCustomProcedureCall(String sql) {
		return sql.toLowerCase().startsWith( "select proc_" );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		theRecord = new SimpleRecord();
		theRecord.text = INITIAL_TEXT;
		sqlTracker.clear();

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
		test( context, openSession()
				.thenCompose( session -> session.find( SimpleRecord.class, theRecord.id ) )
				.thenAccept( Assertions::assertNotNull )
				.thenAccept( v -> assertThat( sqlTracker.getLoggedQueries() )
						.as( "Custom @SQLInsert statement was not emitted" )
						.containsExactly( "SELECT PROC_INSERT_RECORD( $1, $2 );" ) )
		);
	}

	@Test
	public void testUpdateStoredProcedure(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( SimpleRecord.class, theRecord.id )
						.thenAccept( foundRecord -> {
							sqlTracker.clear();
							foundRecord.text = UPDATED_TEXT;
						} )
						.thenCompose( v -> session.flush() ) )
				.thenAccept( v -> assertThat( sqlTracker.getLoggedQueries() )
						.as( "Custom @SQLUpdate statement was not emitted" )
						.containsExactly( "SELECT PROC_UPDATE_RECORD( $1, $2 );" ) )
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
						.thenCompose( foundRecord -> {
							sqlTracker.clear();
							return session.remove( foundRecord );
						} )
						.thenCompose( v -> session.flush() ) )
				.thenAccept( v -> assertThat( sqlTracker.getLoggedQueries() )
						.as( "Custom @SQLDelete statement was not emitted" )
						.containsExactly( "SELECT PROC_DELETE_RECORD( $1 );" ) )
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
