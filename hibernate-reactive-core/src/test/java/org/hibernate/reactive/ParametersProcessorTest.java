/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.stream.Stream;

import org.hibernate.reactive.pool.impl.OracleParameters;
import org.hibernate.reactive.pool.impl.PostgresParameters;
import org.hibernate.reactive.pool.impl.SQLServerParameters;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test the {@link org.hibernate.reactive.pool.impl.Parameters} processor for each database
 */
public class ParametersProcessorTest {

	/**
	 * Each test will replace this placeholder with the correct parameter prefix for the selected database
	 */
	private static final String PARAM_PREFIX = "__paramPrefix__";


	/**
	 * Return the strings to process and the expected result for each one
	 */
	static Stream<Arguments> expectations() {
		return Stream.of(
				arguments( "/* One comment */ \\?", "/* One comment */ ?" ),
				arguments( "/* One comment */ ?", "/* One comment */ " + PARAM_PREFIX + "1" ),
				arguments( "'Sql text ?'", "'Sql text ?'" ),
				arguments( "\\?", "?" ),
				arguments( "???", PARAM_PREFIX + "1" + PARAM_PREFIX + "2" + PARAM_PREFIX + "3" ),
				arguments( "\\?|?", "?|" + PARAM_PREFIX + "1" ),
				arguments( " ? ", " " + PARAM_PREFIX + "1 " )
		);
	}

	@ParameterizedTest
	@MethodSource("expectations")
	public void testPostgreSQLProcessing(String unprocessed, String expected) {
		assertThat( PostgresParameters.INSTANCE.process( unprocessed ) )
				.isEqualTo( expected.replaceAll( PARAM_PREFIX, "\\$" ) );
	}

	@ParameterizedTest
	@MethodSource("expectations")
	public void testSqlServerProcessing(String unprocessed, String expected) {
		assertThat( SQLServerParameters.INSTANCE.process( unprocessed ) )
				.isEqualTo( expected.replaceAll( PARAM_PREFIX, "@P" ) );
	}

	@ParameterizedTest
	@MethodSource("expectations")
	public void testOracleProcessing(String unprocessed, String expected) {
		assertThat( OracleParameters.INSTANCE.process( unprocessed ) )
				.isEqualTo( expected.replaceAll( PARAM_PREFIX, ":" ) );
	}
}
