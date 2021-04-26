/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.LocalDateTime;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

public class CustomStoredProcedureSqlTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

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
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( SimpleRecord.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
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
	public void testInsertStoredProcedure(TestContext context) {
		test( context, openSession().thenCompose( session -> session
				.find( SimpleRecord.class, theRecord.id )
				.thenAccept( foundRecord -> context.assertNotNull( foundRecord ) ) )
		);
	}

	@Test
	public void testUpdateStoredProcedure(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( SimpleRecord.class, theRecord.id )
						.thenAccept( foundRecord -> foundRecord.text = UPDATED_TEXT )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( SimpleRecord.class, theRecord.id ) )
				.thenAccept( foundRecord -> {
					context.assertEquals( UPDATED_TEXT, foundRecord.text );
					context.assertNotNull( foundRecord.updated );
					context.assertNull( foundRecord.deleted );
				} )
		);
	}

	@Test
	public void testDeleteStoredProcedure(TestContext context) {
		test( context, openSession()
				.thenCompose( session -> session
						.find( SimpleRecord.class, theRecord.id )
						.thenCompose( foundRecord -> session.remove( foundRecord ) )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( SimpleRecord.class, theRecord.id ) )
				.thenAccept( foundRecord -> {
					context.assertEquals( INITIAL_TEXT, foundRecord.text );
					context.assertNotNull( foundRecord.updated );
					context.assertNotNull( foundRecord.deleted );
				} )
		);
	}

	@Entity
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
