/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;


import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class UUIDGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( TableId.class );
	}

	@Test
	public void testUUIDGenerator(VertxTestContext context) {
		TableId b = new TableId();
		b.string = "Hello World";

		test( context,
				openSession()
				.thenCompose(s -> s.persist(b).thenCompose(v -> s.flush()))
				.thenCompose( v -> openSession() )
				.thenCompose( s2 ->
					s2.find( TableId.class, b.getId() )
						.thenAccept( bb -> {
							assertThat( bb ).isNotNull();
							assertThat( bb.id ).isNotNull();
							assertThat( bb.string ).isEqualTo( b.string );
							assertThat( bb.version ).isEqualTo( 0 );

							bb.string = "Goodbye";
						})
						.thenCompose(vv -> s2.flush())
						.thenCompose(vv -> s2.find( TableId.class, b.getId() ))
						.thenAccept( bt -> {
							assertThat( bt.version ).isEqualTo( 1 );
						}))
				.thenCompose( v -> openSession() )
				.thenCompose( s3 -> s3.find( TableId.class, b.getId() ) )
				.thenAccept( bb -> {
					assertThat( bb.version ).isEqualTo( 1 );
					assertThat( bb.string ).isEqualTo( "Goodbye" );
				})
		);
	}

	@Entity(name = "TableId")
	@Table(name = "TableId")
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
