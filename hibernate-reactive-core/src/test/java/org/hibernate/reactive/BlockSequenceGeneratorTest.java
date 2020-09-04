/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.*;
import java.util.Objects;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;


public class BlockSequenceGeneratorTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( TableId.class );
		return configuration;
	}

	@Test
	public void testTableGenerator(TestContext context) {

		TableId b = new TableId();
		b.string = "Hello World";

		TableId c = new TableId();
		c.string = "Goodbye";

		test( context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(new TableId("One"))
								.thenCompose(v -> s.persist(new TableId("Two")))
								.thenCompose(v -> s.persist(new TableId("Three")))
								.thenCompose(v -> s.persist(new TableId("Four")))
								.thenCompose(v -> s.persist(new TableId("Five")))
								.thenCompose(v -> s.persist(b))
								.thenCompose(v -> s.persist(c))
								.thenCompose(v -> s.flush())
						)
						.thenCompose( v -> completedFuture( openSession() )
								.thenCompose( s2 -> s2.find( TableId.class, b.getId() )
										.thenAccept( bb -> {
											context.assertNotNull( bb );
											context.assertEquals( bb.id, 10 );
											context.assertEquals( bb.string, b.string );
											context.assertEquals( bb.version, 0 );

											bb.string = "Goodbye";
										} )
										.thenCompose(vv -> s2.flush())
										.thenCompose(vv -> s2.find( TableId.class, b.getId() ))
										.thenAccept( bt -> context.assertEquals( bt.version, 1 ))
								) )
						.thenCompose( v -> completedFuture( openSession() )
								.thenCompose( s3 -> s3.find( TableId.class, b.getId() ) ) )
						.thenAccept( bb -> {
							context.assertEquals(bb.version, 1);
							context.assertEquals( bb.string, "Goodbye");
						} )
						.thenCompose( v -> completedFuture( openSession() )
								.thenCompose( s4 -> s4.find( TableId.class, c.getId() )
										.thenAccept( cc -> {
											context.assertNotNull( cc );
											context.assertEquals( cc.id, 11 );
											context.assertEquals( cc.string, c.string );
											context.assertEquals( cc.version, 0 );

											cc.string = "Goodbye";
										} )
										.thenCompose( vv -> s4.flush() )
										.thenCompose( vv -> s4.find( TableId.class, c.getId() ) )
										.thenAccept( ct -> context.assertEquals( ct.version, 0 ))
								) )
		);
	}

	@Entity
	@Table(name="tab_id")
	@SequenceGenerator(name = "seq",
			sequenceName = "test_id_seq",
			initialValue = 5,
			allocationSize = 5)
	public static class TableId {
		@Id @GeneratedValue(generator = "seq")
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
