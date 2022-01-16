/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.junit.Rule;
import org.junit.Test;

import javax.persistence.*;
import java.util.Objects;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;


public class SequenceGeneratorTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( SequenceId.class );
		return configuration;
	}

	@Test
	public void testSequenceGenerator(TestContext context) {

		SequenceId b = new SequenceId();
		b.string = "Hello World";

		test( context,
				openSession()
				.thenCompose(s -> s.persist(b).thenCompose(v -> s.flush()))
				.thenCompose( v -> openSession() )
				.thenCompose( s2 ->
					s2.find( SequenceId.class, b.getId() )
						.thenAccept( bb -> {
							context.assertNotNull( bb );
							context.assertEquals( bb.id, 5 );
							context.assertEquals( bb.string, b.string );
							context.assertEquals( bb.version, 0 );

							bb.string = "Goodbye";
						})
						.thenCompose(vv -> s2.flush())
						.thenCompose(vv -> s2.find( SequenceId.class, b.getId() ))
						.thenAccept( bt -> {
							context.assertEquals( bt.version, 1 );
						}))
				.thenCompose( v -> openSession() )
				.thenCompose( s3 -> s3.find( SequenceId.class, b.getId() ) )
				.thenAccept( bb -> {
					context.assertEquals(bb.version, 1);
					context.assertEquals(bb.string, "Goodbye");
				})
		);
	}


	public static class SequenceGeneratorDefaultSchemaTest extends SequenceGeneratorTest {

		@Rule //only because we don't have permission to create schema in the CI!
		public DatabaseSelectionRule dbRule = DatabaseSelectionRule.skipTestsFor( COCKROACHDB );

		@Override
		protected Configuration constructConfiguration() {
			Configuration configuration = super.constructConfiguration();
			configuration.setProperty( Settings.DEFAULT_SCHEMA, "hr" );
			configuration.setProperty( Settings.HBM2DDL_CREATE_SCHEMAS, "true" );
			return configuration;
		}
	}

	@Entity
	@SequenceGenerator(name = "seq",
			sequenceName = "test_id_seq",
			initialValue = 5,
			allocationSize = 1)
	public static class SequenceId {
		@Id @GeneratedValue(generator = "seq")
		Integer id;
		@Version Integer version;
		String string;

		public SequenceId() {
		}

		public SequenceId(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			SequenceId sequenceId = (SequenceId) o;
			return Objects.equals(string, sequenceId.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
