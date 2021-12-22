/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.tuple.MutinyValueGenerator;
import org.hibernate.reactive.tuple.StageValueGenerator;

import io.smallrye.mutiny.Uni;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

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

	public static class LoggedUserGeneratorWithStage extends StageValueGenerator<String> {

		@Override
		public CompletionStage<String> generateValue(Stage.Session session, Object owner) {
			Objects.nonNull( session );
			String value = CurrentUser.INSTANCE.get();
			return completedFuture( value );
		}
	}

	public static class LoggedUserGeneratorWithMutiny extends MutinyValueGenerator<String> {

		@Override
		public Uni<String> generateValue(Mutiny.Session session, Object owner) {
			Objects.nonNull( session );
			String value = CurrentUser.INSTANCE.get();
			return Uni.createFrom().item( value );
		}
	}

}
