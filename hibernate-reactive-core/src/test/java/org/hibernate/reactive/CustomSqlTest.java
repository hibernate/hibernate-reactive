/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

public class CustomSqlTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL, COCKROACHDB );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class );
	}

	@Test
	public void test(TestContext context) {
		Record record = new Record();
		record.text = "initial text";
		test(
				context,
				getMutinySessionFactory()
						.withSession( session -> session.persist( record )
								.chain( session::flush )
								.invoke( () -> record.text = "edited text" )
								.chain( session::flush )
						)
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( Record.class, record.id )
										.invoke( (result) -> {
											context.assertNotNull( result );
											context.assertEquals( "edited text", result.text );
											context.assertNotNull( result.updated );
											context.assertNull( result.deleted );
										} )
										.chain( session::remove )
										.chain( session::flush )
								) )
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( Record.class, record.id )
										.invoke( (result) -> {
											context.assertNotNull( result );
											context.assertEquals( "edited text", result.text );
											context.assertNotNull( result.updated );
											context.assertNotNull( result.deleted );
										} )
								) )
		);
	}

	@Entity
	@Table(name = "SqlRecord")
	@SQLInsert(sql = "insert into sqlrecord (text, updated, id) values ($1, localtimestamp, $2)")
	@SQLUpdate(sql = "update sqlrecord set text=$1, updated=localtimestamp where id=$2")
	@SQLDelete(sql = "update sqlrecord set deleted=localtimestamp where id=$1")
	static class Record {
		@GeneratedValue
		@Id
		long id;
		@Basic(optional = false)
		String text;
		@Column(insertable = false, updatable = false, nullable = false)
		LocalDateTime updated;
		@Column(insertable = false, updatable = false)
		LocalDateTime deleted;
	}
}
