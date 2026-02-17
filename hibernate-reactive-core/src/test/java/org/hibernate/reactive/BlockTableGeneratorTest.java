/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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
import jakarta.persistence.Table;
import jakarta.persistence.TableGenerator;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)

public class BlockTableGeneratorTest extends BaseReactiveTest {

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

		test( context,
				openSession()
						.thenCompose( s -> s.persist(new TableId("One"))
								.thenCompose(v -> s.persist(new TableId("Two")))
								.thenCompose(v -> s.persist(new TableId("Three")))
								.thenCompose(v -> s.persist(new TableId("Four")))
								.thenCompose(v -> s.persist(new TableId("Five")))
								.thenCompose(v -> s.persist(b))
								.thenCompose(v -> s.persist(c))
								.thenCompose(v -> s.flush())
						)
						.thenCompose( v -> openSession()
								.thenCompose( s2 -> s2.find( TableId.class, b.getId() )
										.thenAccept( bb -> {
											assertThat( bb ).isNotNull();
											assertThat( bb.id ).isEqualTo( 10 );
											assertThat( bb.string ).isEqualTo( b.string );
											assertThat( bb.version ).isEqualTo( 0 );

											bb.string = "Goodbye";
										} )
										.thenCompose(vv -> s2.flush())
										.thenCompose(vv -> s2.find( TableId.class, b.getId() ))
										.thenAccept( bt -> assertThat( bt.version ).isEqualTo( 1 ) ))
						)
						.thenCompose( v -> openSession()
								.thenCompose( s3 -> s3.find( TableId.class, b.getId() ) ) )
						.thenAccept( bb -> {
							assertThat( bb.version ).isEqualTo( 1 );
							assertThat( bb.string ).isEqualTo( "Goodbye" );
						} )
						.thenCompose( v -> openSession()
								.thenCompose( s4 -> s4.find( TableId.class, c.getId() )
										.thenAccept( cc -> {
											assertThat( cc ).isNotNull();
											assertThat( cc.id ).isEqualTo( 11 );
											assertThat( cc.string ).isEqualTo( c.string );
											assertThat( cc.version ).isEqualTo( 0 );

											cc.string = "Goodbye";
										} )
										.thenCompose( vv -> s4.flush() )
										.thenCompose( vv -> s4.find( TableId.class, c.getId() ) )
										.thenAccept( ct -> assertThat( ct.version ).isEqualTo( 0 ) )
								) )
		);
	}

	@Entity(name="TableId")
	@Table(name="tab_id")
	@TableGenerator(name = "tab",
			valueColumnName = "nextid",
			table = "test_id_tab",
			allocationSize = 5)
	public static class TableId {
		@Id @GeneratedValue(generator = "tab")
		Integer id;
		@Version Integer version;
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
			return Objects.equals(string, tableId.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
