/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;
import org.hibernate.id.Configurable;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Version;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CustomGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( CustomId.class );
	}

	@Test
	public void testSequenceGenerator(VertxTestContext context) {

		CustomId b = new CustomId();
		b.string = "Hello World";

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( b ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s2 -> s2
								.find( CustomId.class, b.getId() )
								.thenAccept( bb -> {
									assertNotNull( bb );
									assertEquals( bb.id, 1100 );
									assertEquals( bb.string, b.string );
									assertEquals( bb.version, 0 );

									bb.string = "Goodbye";
								} )
								.thenCompose( vv -> s2.flush() )
								.thenCompose( vv -> s2.find( CustomId.class, b.getId() ) )
								.thenAccept( bt -> {
									assertEquals( bt.version, 1 );
								} ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s3 -> s3.find( CustomId.class, b.getId() ) )
						.thenAccept( bb -> {
							assertEquals( bb.version, 1 );
							assertEquals( bb.string, "Goodbye" );
						} )
		);
	}

	public static class Thousands implements ReactiveIdentifierGenerator<Integer>, Configurable {
		int current = 0;

		@Override
		public CompletionStage<Integer> generate(ReactiveConnectionSupplier session, Object entity) {
			current += 1000;
			return completedFuture( current );
		}

		@Override
		public void configure(Type type, Properties params, ServiceRegistry serviceRegistry) {
			current = Integer.parseInt( params.getProperty( "offset", "0" ) );
		}
	}

	@Entity
	@GenericGenerator(
			name = "thousands",
			strategy = "org.hibernate.reactive.CustomGeneratorTest$Thousands",
			parameters = @Parameter(name = "offset", value = "100")
	)
	public static class CustomId {
		@Id
		@GeneratedValue(generator = "thousands")
		Integer id;
		@Version
		Integer version;
		String string;

		public CustomId() {
		}

		public CustomId(Integer id, String string) {
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
			CustomId customId = (CustomId) o;
			return Objects.equals( string, customId.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}
}
