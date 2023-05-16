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

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Version;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MutinySequenceGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( SequenceId.class );
	}

	@Test
	public void testSequenceGenerator(VertxTestContext context) {

		SequenceId b = new SequenceId();
		b.string = "Hello World";

		test( context,
				getMutinySessionFactory()
						.withSession( s -> s.persist(b).call( s::flush ) )
						.call( () -> getMutinySessionFactory().withSession(
								s2 -> s2.find( SequenceId.class, b.getId() )
										.map( bb -> {
											assertNotNull( bb );
											assertEquals( bb.id, 5 );
											assertEquals( bb.string, b.string );
											assertEquals( bb.version, 0 );

											bb.string = "Goodbye";
											return null;
										} )
										.call( s2::flush )
										.chain( () -> s2.find( SequenceId.class, b.getId() ) )
										.map( bt -> {
											assertEquals( bt.version, 1 );
											return null;
										} )
						) )
						.call( () -> getMutinySessionFactory().withSession(
								s3 -> s3.find( SequenceId.class, b.getId() )
										.map( bb -> {
											assertEquals( bb.version, 1 );
											assertEquals( bb.string, "Goodbye" );
											return null;
										} )
						) )
		);
	}

	@Entity
	@SequenceGenerator(name = "seq",
			sequenceName = "test_id_seq",
			initialValue = 5)
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
