/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

import org.hibernate.annotations.IdGeneratorType;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.ValueGenerationType;
import org.hibernate.dialect.Dialect;
import org.hibernate.generator.EventType;
import org.hibernate.generator.EventTypeSets;
import org.hibernate.reactive.id.ReactiveOnExecutionGenerator;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class OnExecutionGeneratorTypeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Tournament.class );
	}

	@Test
	public void testPersist(VertxTestContext context) {
		Tournament tournament = new Tournament( "Tekken World Tour" );
		test(
				context, getSessionFactory()
						.withTransaction( session -> session.persist( tournament ) )
						.thenAccept( v -> {
							assertThat( tournament.getId() ).isNotNull();
							assertThat( tournament.getCreated() ).isNotNull();
						} )
		);
	}

	@Entity(name = "Tournament")
	public static class Tournament {
		@Id
		@FunctionCreatedValueId
		Date id;

		@NaturalId
		String name;

		@FunctionCreatedValue
		Date created;

		public Tournament() {
		}

		public Tournament(String name) {
			this.name = name;
		}

		public Date getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Date getCreated() {
			return created;
		}
	}

	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@IdGeneratorType(FunctionCreationValueGeneration.class)
	public @interface FunctionCreatedValueId {
	}

	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@ValueGenerationType(generatedBy = FunctionCreationValueGeneration.class)
	public @interface FunctionCreatedValue {
	}

	public static class FunctionCreationValueGeneration
			implements ReactiveOnExecutionGenerator {

		@Override
		public boolean referenceColumnsInSql(Dialect dialect) {
			return true;
		}

		@Override
		public boolean writePropertyValue() {
			return false;
		}

		@Override
		public String[] getReferencedColumnValues(Dialect dialect) {
			return new String[] { dialect.currentTimestamp() };
		}

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EventTypeSets.INSERT_ONLY;
		}
	}

}
