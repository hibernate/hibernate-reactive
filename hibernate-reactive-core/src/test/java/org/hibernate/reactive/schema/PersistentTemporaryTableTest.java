/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.hibernate.cfg.Configuration;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableStrategy;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.containers.DatabaseConfiguration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(value = DatabaseConfiguration.DBType.COCKROACHDB, reason = "It uses PersistentTemporaryTableStrategy by default")
public class PersistentTemporaryTableTest extends AbstractTemporaryTableTest {

	public static Stream<Arguments> settings() {
		return Stream.of(
				arguments(
						(Consumer<Configuration>) (Configuration configuration) -> {
							configuration.setProperty( PersistentTableStrategy.CREATE_ID_TABLES, "true" );
							configuration.setProperty( PersistentTableStrategy.DROP_ID_TABLES, "true" );
						},
						(CheckTemporaryTableCommandExecution) () -> assertNumberOfTemporaryTableCreated(
								1,
								"Temporary table has not been created even if CREATE_ID_TABLES is true"
						),
						(CheckTemporaryTableCommandExecution) () -> assertNumberOfTemporaryTableDropped(
								1,
								"Temporary table has not been dropped even if DROP_ID_TABLES is true"
						)
				),
				arguments(
						(Consumer<Configuration>) (Configuration configuration) -> {
							configuration.setProperty( PersistentTableStrategy.CREATE_ID_TABLES, "false" );
							configuration.setProperty( PersistentTableStrategy.DROP_ID_TABLES, "false" );
						},
						(CheckTemporaryTableCommandExecution) () -> assertNumberOfTemporaryTableCreated(
								0,
								"Temporary table has been created even if CREATE_ID_TABLES is false"
						),
						(CheckTemporaryTableCommandExecution) () -> assertNumberOfTemporaryTableDropped(
								0,
								"Temporary table has been dropped even if DROP_ID_TABLES is false"
						)
				)
		);
	}

	@ParameterizedTest
	@MethodSource("settings")
	public void testPersistentTemporaryTableCreation(
			Consumer<Configuration> configure,
			CheckTemporaryTableCommandExecution temporaryTableCreationCheck,
			CheckTemporaryTableCommandExecution temporaryTableDopCheck,
			VertxTestContext context) {
		testTemporaryTableCreation( configure, temporaryTableCreationCheck, temporaryTableDopCheck, context );
	}
}
