/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import io.smallrye.mutiny.Uni;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.hibernate.annotations.ValueGenerationType;
import org.hibernate.generator.EventType;
import org.hibernate.generator.EventTypeSets;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.tuple.MutinyGenerator;
import org.hibernate.reactive.tuple.StageGenerator;

/**
 * An example from the Hibernate ORM documentation that we use for testing of
 * entities using @{@link org.hibernate.annotations.GeneratorType}
 *
 * @see GeneratedPropertyJoinedTableTest
 * @see GeneratedPropertySingleTableTest
 * @see GeneratedPropertyUnionSubclassesTest
 */
public class CurrentUser {

	public static final CurrentUser INSTANCE = new CurrentUser();

	private static final ThreadLocal<String> storage = new ThreadLocal<>();

	public void logIn(String user) {
		storage.set( user );
	}

	public void logOut() {
		storage.remove();
	}

	public String get() {
		return storage.get();
	}

	@ValueGenerationType(generatedBy = InsertLoggedUserGeneratorWithStage.class)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface LoggedUserStageInsert {}

	@ValueGenerationType(generatedBy = AlwaysLoggedUserGeneratorWithStage.class)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface LoggedUserStageAlways {}

	public static abstract class AbstractLoggedUserGeneratorWithStage extends StageGenerator {
		@Override
		public CompletionStage<Object> generate(Stage.Session session, Object owner, Object currentValue,
												EventType eventType) {
			Objects.requireNonNull( session );
			String value = CurrentUser.INSTANCE.get();
			return completedFuture( value );
		}
	}

	public static class InsertLoggedUserGeneratorWithStage extends AbstractLoggedUserGeneratorWithStage {

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EventTypeSets.INSERT_ONLY;
		}
	}

	public static class AlwaysLoggedUserGeneratorWithStage extends AbstractLoggedUserGeneratorWithStage {

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EventTypeSets.ALL;
		}
	}

	@ValueGenerationType(generatedBy = InsertLoggedUserGeneratorWithMutiny.class)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface LoggedUserMutinyInsert {}

	@ValueGenerationType(generatedBy = AlwaysLoggedUserGeneratorWithMutiny.class)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface LoggedUserMutinyAlways {}

	public static abstract class AbstractLoggedUserGeneratorWithMutiny extends MutinyGenerator {

		@Override
		public Uni<Object> generate(Mutiny.Session session, Object owner, Object currentValue,
										 EventType eventType) {
			Objects.requireNonNull( session );
			String value = CurrentUser.INSTANCE.get();
			return Uni.createFrom().item( value );
		}
	}

	public static class InsertLoggedUserGeneratorWithMutiny extends AbstractLoggedUserGeneratorWithMutiny {

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EventTypeSets.INSERT_ONLY;
		}
	}

	public static class AlwaysLoggedUserGeneratorWithMutiny extends AbstractLoggedUserGeneratorWithMutiny {

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EventTypeSets.ALL;
		}
	}

}
