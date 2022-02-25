/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Version;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.*;


public class UUIDGeneratorTest extends BaseReactiveTest {

	@Rule // Storing UUID doesn't work with DB2, Searching a UUID doesn't work for Oracle
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.skipTestsFor( DB2, ORACLE );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( TableId.class );
	}

	@Test
	public void testUUIDGenerator(TestContext context) {
		TableId b = new TableId();
		b.string = "Hello World";

		test( context,
				openSession()
				.thenCompose(s -> s.persist(b).thenCompose(v -> s.flush()))
				.thenCompose( v -> openSession() )
				.thenCompose( s2 ->
					s2.find( TableId.class, b.getId() )
						.thenAccept( bb -> {
							context.assertNotNull( bb );
							context.assertNotNull( bb.id );
							context.assertEquals( bb.string, b.string );
							context.assertEquals( bb.version, 0 );

							bb.string = "Goodbye";
						})
						.thenCompose(vv -> s2.flush())
						.thenCompose(vv -> s2.find( TableId.class, b.getId() ))
						.thenAccept( bt -> {
							context.assertEquals( bt.version, 1 );
						}))
				.thenCompose( v -> openSession() )
				.thenCompose( s3 -> s3.find( TableId.class, b.getId() ) )
				.thenAccept( bb -> {
					context.assertEquals(bb.version, 1);
					context.assertEquals( bb.string, "Goodbye");
				})
		);
	}

	@Entity
	public static class TableId {
		@Id @GeneratedValue @Column(length=16) UUID id;
		@Version Integer version;
		String string;

		public TableId() {
		}

		public TableId(UUID id, String string) {
			this.id = id;
			this.string = string;
		}

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
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
			TableId tableId = (TableId) o;
			return Objects.equals(string, tableId.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
