/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, timeUnit = MINUTES)

public class BlockSequenceGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( TableId.class );
	}

	@Test
	public void testTableGenerator(VertxTestContext context) {
		TableId b = new TableId();
		b.string = "Hello World";
		TableId c = new TableId();
		c.string = "Goodbye";
		test( context, openSession()
				.thenCompose( s -> s.persist( new TableId( "One" ) )
						.thenCompose( v -> s.persist( new TableId( "Two" ) ) )
						.thenCompose( v -> s.persist( new TableId( "Three" ) ) )
						.thenCompose( v -> s.persist( new TableId( "Four" ) ) )
						.thenCompose( v -> s.persist( new TableId( "Five" ) ) )
						.thenCompose( v -> s.persist( b ) )
						.thenCompose( v -> s.persist( c ) )
						.thenCompose( v -> s.flush() )
				)
				.thenCompose( v -> openSession()
						.thenCompose( s2 -> s2.find( TableId.class, b.getId() )
								.thenAccept( bb -> {
									assertNotNull( bb );
									assertEquals( bb.id, 10 );
									assertEquals( bb.string, b.string );
									assertEquals( bb.version, 0 );
									bb.string = "Goodbye";
								} )
								.thenCompose( vv -> s2.flush() )
								.thenCompose( vv -> s2.find( TableId.class, b.getId() ) )
								.thenAccept( bt -> assertEquals( bt.version, 1 ) )
						) )
				.thenCompose( v -> openSession()
						.thenCompose( s3 -> s3.find( TableId.class, b.getId() ) ) )
				.thenAccept( bb -> {
					assertEquals( bb.version, 1 );
					assertEquals( bb.string, "Goodbye" );
				} )
				.thenCompose( v -> openSession()
						.thenCompose( s4 -> s4.find( TableId.class, c.getId() )
								.thenAccept( cc -> {
									assertNotNull( cc );
									assertEquals( cc.id, 11 );
									assertEquals( cc.string, c.string );
									assertEquals( cc.version, 0 );

									cc.string = "Goodbye";
								} )
								.thenCompose( vv -> s4.flush() )
								.thenCompose( vv -> s4.find( TableId.class, c.getId() ) )
								.thenAccept( ct -> assertEquals( ct.version, 0 ) )
						) )
		);
	}

	@Entity
	@Table(name = "tab_id")
	@SequenceGenerator(name = "seq",
			sequenceName = "test_id_seq",
			initialValue = 5,
			allocationSize = 5)
	public static class TableId {
		@Id
		@GeneratedValue(generator = "seq")
		Integer id;
		@Version
		Integer version;
		String string;

		public TableId() {
		}

		public TableId(String string) {
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
			TableId tableId = (TableId) o;
			return Objects.equals( string, tableId.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}
}
