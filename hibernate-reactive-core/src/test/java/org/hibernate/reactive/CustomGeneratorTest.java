/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import org.hibernate.MappingException;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;
import org.hibernate.generator.EventType;
import org.hibernate.generator.GeneratorCreationContext;
import org.hibernate.id.Configurable;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.internal.CompletionStages.completedFuture;

@Timeout(value = 10, timeUnit = MINUTES)
public class CustomGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( CustomId.class );
	}

	@Test
	public void testSequenceGenerator(VertxTestContext context) {
		CustomId entity1 = new CustomId();
		entity1.string = "Stateful Hello World";

		CustomId entity2 = new CustomId();
		entity2.string = "Stateless Hello World";

		test(
				context,
				// First, we create and update an entity with the regular session
				openSession()
						// Persist an entity
						.thenCompose( s1 -> s1.persist( entity1 ).thenCompose( v -> s1.flush() ) )
						.thenCompose( v -> openSession() )
						// Find it and update a field
						.thenCompose( s2 -> s2.find( CustomId.class, entity1.getId() )
								.thenCompose( found -> {
									assertEntity( found, 1100, entity1.string, 0 );
									found.string = "Stateful Goodbye";
									return s2.flush()
											// Check version after flush
											.thenAccept( v -> assertThat( found.version ).isEqualTo( 1 ) );
								} )
						)
						// Find the entity and check the version again (shouldn't have changed)
						.thenCompose( v -> openSession() )
						.thenCompose( s3 -> s3.find( CustomId.class, entity1.getId() ) )
						.thenAccept( found -> assertEntity( found, 1100, "Stateful Goodbye", 1 ) )

						// then, we insert and update another entity with the stateless session
						.thenCompose( v -> openStatelessSession() )
						// Insert a new entity
						.thenCompose( ss1 -> ss1.insert( entity2 ) )
						// Find it and update a field
						.thenCompose( v -> openStatelessSession() )
						.thenCompose( ss2 -> ss2
								.find( CustomId.class, entity2.getId() )
								.thenCompose( found -> {
									assertEntity( found, 2100, entity2.string, 0 );
									found.string = "Stateless Goodbye";
									return ss2.update( found )
											// Check version after update
											.thenAccept( v -> assertThat( found.version ).isEqualTo( 1 ) );
								} )

						)
						// Find the entity and check the version again (shouldn't have changed)
						.thenCompose( v -> openStatelessSession() )
						.thenCompose( s3 -> s3.find( CustomId.class, entity2.getId() ) )
						.thenAccept( found -> assertEntity( found, 2100, "Stateless Goodbye", 1 ) )
		);
	}

	private static void assertEntity(CustomId found, int expectedId, String expectedString, int expectedVersion) {
		assertThat( found ).isNotNull();
		assertThat( found.id ).isEqualTo( expectedId );
		assertThat( found.string ).isEqualTo( expectedString );
		assertThat( found.version ).isEqualTo( expectedVersion );
	}

	public static class Thousands implements ReactiveIdentifierGenerator<Integer>, Configurable {
		int current = 0;

		@Override
		public CompletionStage<Integer> generate(ReactiveConnectionSupplier session, Object entity) {
			current += 1000;
			return completedFuture( current );
		}

		@Override
		public void configure(GeneratorCreationContext creationContext, Properties parameters) throws MappingException {
			current = Integer.parseInt( parameters.getProperty( "offset", "0" ) );
		}

		@Override
		public boolean generatedOnExecution() {
			return false;
		}

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EnumSet.of( EventType.INSERT );
		}
	}

	@Entity
	public static class CustomId {
		@Id
		@GeneratedValue(generator = "thousands")
		@GenericGenerator(
				type = Thousands.class,
				parameters = @Parameter(name = "offset", value = "100")
		)
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
