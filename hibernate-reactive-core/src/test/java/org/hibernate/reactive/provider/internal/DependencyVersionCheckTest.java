/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.hibernate.HibernateException;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.provider.internal.DependencyVersionCheck.ComparableVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test the check of the minimum versions of Hibernate ORM and the Vert.x SQL client
 */
public class DependencyVersionCheckTest {

	@ParameterizedTest
	@CsvSource({
			"7.1.0.Final, 7.1.1.Final",
			"7.1.1.Final, 7.2.0.Final",
			"7.2.0.Final, 8.0.0.Alpha1",
			"8.0.0.Alpha1, 8.0.0.Alpha2",
			"8.0.0.Alpha2, 8.0.0.Beta1",
			"8.0.0.Beta2, 8.0.0.Beta3",
			"8.0.0.Beta3, 8.0.0.CR1",
			"8.0.0.CR1, 8.0.0.CR2",
			"8.0.0.CR2, 8.0.0.Final",
			"5.0.9, 5.1.0",
			"5.1.0, 5.1.8",
			"5.1.8, 5.1.10",
			"5.1.0-RC1, 5.1.0",
			"5, 5.0.1",
			"4.5.10, 5.0.0"
	})
	public void olderVersion(String older, String newer) {
		assertThat( ComparableVersion.parse( older ) ).isLessThan( ComparableVersion.parse( newer ) );
		assertThat( ComparableVersion.parse( newer ) ).isGreaterThan( ComparableVersion.parse( older ) );
	}

	@ParameterizedTest
	@CsvSource({
			"5.1.8, 5.1.8.Final",
			"5.1.8, 5.1.8.GA",
			"5.1, 5.1.0",
			"8.0.0.CR1, 8.0.0-RC1",
			"8.0.0.Beta3, 8.0.0.beta3"
	})
	public void sameVersion(String version1, String version2) {
		assertThat( ComparableVersion.parse( version1 ) ).isEqualByComparingTo( ComparableVersion.parse( version2 ) );
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"[WORKING]",
			"${hibernateOrmMinimumVersion}",
			"8.0.0-SNAPSHOT",
			"5.1.0-SNAPSHOT",
			"8.0.0.Unknown1",
			"",
			"not a version"
	})
	public void unsupportedVersion(String version) {
		assertThat( ComparableVersion.parse( version ) ).isNull();
	}

	@Test
	public void nullVersion() {
		assertThat( ComparableVersion.parse( null ) ).isNull();
	}

	@ParameterizedTest
	@CsvSource({
			"8.0.0.Beta3, 8.0.0.Beta3",
			"8.0.0.Beta3, 8.0.0.Final",
			"8.0.0.Beta3, 8.0.1.Final",
			"8.0.0.Beta3, 9.0.0.Final"
	})
	public void compatibleVersion(String minimum, String found) {
		assertThatCode( () -> DependencyVersionCheck.check( "Hibernate ORM", found, minimum ) )
				.doesNotThrowAnyException();
	}

	@Test
	public void olderOrmVersionFails() {
		assertThatThrownBy( () -> DependencyVersionCheck.check( "Hibernate ORM", "7.1.2.Final", "8.0.0.Beta3" ) )
				.isInstanceOf( HibernateException.class )
				.hasMessageContaining( "HR000004" )
				.hasMessageContaining( "requires Hibernate ORM 8.0.0.Beta3 or later, but version 7.1.2.Final was found" )
				.hasMessageContaining( Settings.SKIP_VERSION_CHECK );
	}

	@Test
	public void olderVertxVersionFails() {
		assertThatThrownBy( () -> DependencyVersionCheck.check( "Vert.x SQL client", "5.0.4", "5.1.8" ) )
				.isInstanceOf( HibernateException.class )
				.hasMessageContaining( "requires Vert.x SQL client 5.1.8 or later, but version 5.0.4 was found" );
	}

	@ParameterizedTest
	@CsvSource({
			// Version unknown (no manifest)
			"[WORKING], 8.0.0.Beta3",
			// Snapshot
			"7.0.0-SNAPSHOT, 8.0.0.Beta3",
			"8.0.0.Final, 9.0.0-SNAPSHOT",
			// Resource not filtered (for example, running from the IDE)
			"7.0.0.Final, ${hibernateOrmMinimumVersion}"
	})
	public void checkSkipped(String found, String minimum) {
		assertThatCode( () -> DependencyVersionCheck.check( "Hibernate ORM", found, minimum ) )
				.doesNotThrowAnyException();
	}

	@Test
	public void checkSkippedWhenVersionIsMissing() {
		assertThatCode( () -> DependencyVersionCheck.check( "Hibernate ORM", null, "8.0.0.Beta3" ) )
				.doesNotThrowAnyException();
		assertThatCode( () -> DependencyVersionCheck.check( "Hibernate ORM", "7.0.0.Final", null ) )
				.doesNotThrowAnyException();
	}

	@ParameterizedTest
	@CsvSource(value = {
			// configuration, system property, expected
			"null, null, false",
			"true, null, true",
			"false, null, false",
			"null, true, true",
			"null, false, false",
			// The system property has precedence over the configuration
			"false, true, true",
			"true, false, false",
			"true, true, true",
			"false, false, false"
	}, nullValues = "null")
	public void skipVersionCheck(String configuration, String systemProperty, boolean expected) {
		final Map<String, Object> settings = configuration == null
				? Map.of()
				: Map.of( Settings.SKIP_VERSION_CHECK, configuration );
		final String previous = System.getProperty( Settings.SKIP_VERSION_CHECK );
		try {
			if ( systemProperty == null ) {
				System.clearProperty( Settings.SKIP_VERSION_CHECK );
			}
			else {
				System.setProperty( Settings.SKIP_VERSION_CHECK, systemProperty );
			}
			assertThat( DependencyVersionCheck.skipCheck( settings ) ).isEqualTo( expected );
		}
		finally {
			if ( previous == null ) {
				System.clearProperty( Settings.SKIP_VERSION_CHECK );
			}
			else {
				System.setProperty( Settings.SKIP_VERSION_CHECK, previous );
			}
		}
	}

	/**
	 * Check that the minimum versions have been set at build time,
	 * and that they are compatible with the versions we test with.
	 */
	@Test
	public void minimumVersionsAreCompatibleWithTheClasspath() throws IOException {
		final Properties minimumVersions = new Properties();
		try ( InputStream stream = DependencyVersionCheck.class.getResourceAsStream( "minimum-versions.properties" ) ) {
			assertThat( stream ).isNotNull();
			minimumVersions.load( stream );
		}
		assertThat( minimumVersions.getProperty( "hibernate-orm" ) ).doesNotContain( "${" );
		assertThat( minimumVersions.getProperty( "vertx-sql-client" ) ).doesNotContain( "${" );

		assertThatCode( () -> DependencyVersionCheck.verifyOnce( Map.of() ) ).doesNotThrowAnyException();
	}
}
