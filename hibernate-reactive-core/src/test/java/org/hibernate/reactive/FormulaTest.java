/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;

import org.hibernate.annotations.Formula;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.Instant;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;

public class FormulaTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.skipTestsFor( MARIA, MYSQL );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Record.class );
		return configuration;
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
						)
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( Record.class, record.id ) ) )
						.invoke( (r) -> context.assertNotNull( r.current ) )
		);
	}

	@Entity(name = "Record")
	@Table(name = "FRecord")
	static class Record {
		@GeneratedValue
		@Id
		long id;
		@Basic(optional = false)
		String text;
		@Formula("current_timestamp")
		Instant current;
	}
}
