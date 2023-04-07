/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import io.smallrye.mutiny.Uni;
import org.assertj.core.api.AbstractInstantAssert;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Utility to handle verifying the information about an expected {@link Throwable}
 */
public class ReactiveAssertions {

	/**
	 * Assert that the expected exception type is thrown.
	 * If the thrown exception is a {@link CompletionException}, then the cause will be tested instead.
	 *
	 * @return a {@link CompletionStage} with the expected exception thrown by the stage we were testing.
	 */
	public static <T extends Throwable> CompletionStage<T> assertThrown(Class<T> expectedException, CompletionStage<?> stage) {
		return stage
				.handle( ReactiveAssertions::catchException )
				.thenApply( e -> {
					assertThat( e ).isInstanceOf( expectedException );
					return (T) e;
				} );
	}

	public static AbstractInstantAssert<?> assertWithTruncationThat(Instant instant) {
		return javaVersion() < 15
				? assertThat( instant )
				// Since java 15, the precision of the equals has changed.
				// It won't work unless we truncate the value to the millis
				: assertThat( instant ).usingComparator( comparing( o -> o.truncatedTo( MILLIS ) ) );
	}

	private static Integer javaVersion() {
		String version = System.getProperty( "java.version" );
		return Integer.valueOf( version.substring( 0, 2 ) );
	}

	/**
	 * Depending on the use case, the real exception could be wrapped in a {@link CompletionException}.
	 * In that case the method returns the cause.
	 */
	private static Throwable catchException(Object ignore, Throwable e) {
		return e instanceof CompletionException ? e.getCause() : e;
	}

	/**
	 * Assert that the expected exception type is thrown.
	 *
	 * @return a {@link Uni} with the expected exception thrown by the uni we were testing.
	 */
	public static <U extends Throwable> Uni<U> assertThrown(Class<U> expectedException, Uni<?> uni) {
		return uni.onItemOrFailure().transform( (s, e) -> {
			assertThat( e ).isInstanceOf( expectedException );
			return (U) e;
		} );
	}
}
