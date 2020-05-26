/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Version;
import java.util.Objects;
import java.util.UUID;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.Assume.assumeTrue;

public class UUIDGeneratorTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( TableId.class );
		return configuration;
	}

	@Test
	public void testUUIDGenerator(TestContext context) {

		//TODO: why precisely are UUIDs not working on MySQL?
		assumeTrue( dbType() != DBType.MYSQL );

		TableId b = new TableId();
		b.string = "Hello World";

		test( context,
				openSession()
				.thenCompose(s -> s.persist(b))
				.thenCompose(s -> s.flush())
				.thenCompose( v -> openSession())
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
				.thenCompose( v -> openSession())
				.thenCompose( s3 -> s3.find( TableId.class, b.getId() ) )
				.thenAccept( bb -> {
					context.assertEquals(bb.version, 1);
					context.assertEquals( bb.string, "Goodbye");
				})
		);
	}

	@Entity
	public static class TableId {
		@Id @GeneratedValue UUID id;
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
