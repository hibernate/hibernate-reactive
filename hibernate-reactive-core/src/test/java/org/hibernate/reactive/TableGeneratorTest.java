/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.TableGenerator;
import jakarta.persistence.Version;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;


public class TableGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( TableId.class );
	}

	@Test
	public void testTableGenerator(TestContext context) {

		TableId b = new TableId();
		b.string = "Hello World";

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( b ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s2 ->
											  s2.find( TableId.class, b.getId() )
													  .thenAccept( bb -> {
														  context.assertNotNull( bb );
														  context.assertEquals( bb.id, 6 );
														  context.assertEquals( bb.string, b.string );
														  context.assertEquals( bb.version, 0 );

														  bb.string = "Goodbye";
													  } )
													  .thenCompose( vv -> s2.flush() )
													  .thenCompose( vv -> s2.find( TableId.class, b.getId() ) )
													  .thenAccept( bt -> {
														  context.assertEquals( bt.version, 1 );
													  } ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s3 -> s3.find( TableId.class, b.getId() ) )
						.thenAccept( bb -> {
							context.assertEquals( bb.version, 1 );
							context.assertEquals( bb.string, "Goodbye" );
						} )
		);
	}

	@Entity
	@TableGenerator(name = "tab",
			valueColumnName = "nextid",
			table = "test_id_tab",
			initialValue = 5,
			allocationSize = 1)
	public static class TableId {
		@Id
		@GeneratedValue(generator = "tab")
		Integer id;
		@Version
		Integer version;
		String string;

		public TableId() {
		}

		public TableId(Integer id, String string) {
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
			TableId tableId = (TableId) o;
			return Objects.equals( string, tableId.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}
}
