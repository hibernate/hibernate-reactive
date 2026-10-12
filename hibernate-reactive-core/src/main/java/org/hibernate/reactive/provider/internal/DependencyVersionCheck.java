/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.internal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.module.ModuleDescriptor;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;
import org.hibernate.reactive.logging.internal.Version;
import org.hibernate.reactive.provider.Settings;

import io.vertx.sqlclient.Pool;

import static org.hibernate.internal.util.config.ConfigurationHelper.getBoolean;

/**
 * Verifies, during bootstrap, that the versions of Hibernate ORM and the Vert.x SQL client
 * on the classpath are not older than the minimum versions required by
 * Hibernate Reactive.
 * <p>
 * Only the lower bound is checked: newer versions are always accepted.
 * When a version cannot be determined (for example, when the jar manifest
 * is missing), or when a snapshot is involved, the check is skipped.
 *
 * @see Settings#SKIP_VERSION_CHECK
 */
public final class DependencyVersionCheck {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	/**
	 * Generated at build time from the version catalog
	 */
	private static final String MINIMUM_VERSIONS_RESOURCE = "minimum-versions.properties";

	static final String HIBERNATE_ORM = "Hibernate ORM";
	static final String VERTX_SQL_CLIENT = "Vert.x SQL client";

	private static volatile boolean verified;

	private DependencyVersionCheck() {
	}

	/**
	 * Check the versions of the dependencies, unless they have already been
	 * successfully checked, or the check has been disabled via
	 * {@link Settings#SKIP_VERSION_CHECK}.
	 *
	 * @throws org.hibernate.HibernateException if a dependency is older than the minimum required version
	 */
	public static void verifyOnce(Map<String, Object> settings) {
		if ( verified || skipCheck( settings ) ) {
			return;
		}
		final Properties minimumVersions = loadMinimumVersions();
		check( HIBERNATE_ORM, ormVersion(), minimumVersions.getProperty( "hibernate-orm" ) );
		check( VERTX_SQL_CLIENT, vertxSqlClientVersion(), minimumVersions.getProperty( "vertx-sql-client" ) );
		verified = true;
	}

	/**
	 * The system property, when set, has precedence over the configuration.
	 */
	static boolean skipCheck(Map<String, Object> settings) {
		final String systemProperty = System.getProperty( Settings.SKIP_VERSION_CHECK );
		return systemProperty != null
				? Boolean.parseBoolean( systemProperty.trim() )
				: getBoolean( Settings.SKIP_VERSION_CHECK, settings, false );
	}

	static void check(String library, String found, String minimum) {
		final ComparableVersion foundVersion = ComparableVersion.parse( found );
		final ComparableVersion minimumVersion = ComparableVersion.parse( minimum );
		if ( foundVersion == null || minimumVersion == null ) {
			LOG.skippingVersionCheck( library, found, minimum );
		}
		else if ( foundVersion.compareTo( minimumVersion ) < 0 ) {
			throw LOG.incompatibleDependencyVersion(
					Version.getVersionString(),
					library,
					minimum,
					found,
					Settings.SKIP_VERSION_CHECK
			);
		}
	}

	private static String ormVersion() {
		return org.hibernate.Version.getVersionString();
	}

	private static String vertxSqlClientVersion() {
		return versionOf( Pool.class );
	}

	/**
	 * The version from the jar manifest or, when running on the module path,
	 * from the module descriptor.
	 */
	private static String versionOf(Class<?> type) {
		final String version = type.getPackage().getImplementationVersion();
		if ( version != null ) {
			return version;
		}
		final ModuleDescriptor descriptor = type.getModule().getDescriptor();
		return descriptor == null ? null : descriptor.rawVersion().orElse( null );
	}

	private static Properties loadMinimumVersions() {
		final Properties properties = new Properties();
		try ( InputStream stream = DependencyVersionCheck.class.getResourceAsStream( MINIMUM_VERSIONS_RESOURCE ) ) {
			if ( stream != null ) {
				properties.load( stream );
			}
		}
		catch (IOException e) {
			// The check will be skipped
			LOG.tracef( e, "Unable to load the minimum versions from %s", MINIMUM_VERSIONS_RESOURCE );
		}
		return properties;
	}

	/**
	 * A version in the format used by Hibernate and Vert.x: {@code major.minor.micro}
	 * followed by an optional qualifier. For example: {@code 7.1.0.Final},
	 * {@code 8.0.0.Beta3}, {@code 5.0.0.CR2}, {@code 5.1.8}.
	 * <p>
	 * Qualifiers are ordered as: {@code Alpha < Beta < CR < Final}.
	 * No qualifier is the same as {@code Final}.
	 */
	record ComparableVersion(int major, int minor, int micro, int qualifierRank, int qualifierNumber)
			implements Comparable<ComparableVersion> {

		private static final Pattern PATTERN = Pattern.compile(
				"(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:[.-]([a-zA-Z]+)(\\d*))?" );

		/**
		 * @return the parsed version, or {@code null} if the version is unknown,
		 * a snapshot, or it has an unrecognized format
		 */
		static ComparableVersion parse(String version) {
			if ( version == null ) {
				return null;
			}
			final Matcher matcher = PATTERN.matcher( version.trim() );
			if ( !matcher.matches() ) {
				return null;
			}
			final int qualifierRank = qualifierRank( matcher.group( 4 ) );
			if ( qualifierRank < 0 ) {
				return null;
			}
			return new ComparableVersion(
					Integer.parseInt( matcher.group( 1 ) ),
					toInt( matcher.group( 2 ) ),
					toInt( matcher.group( 3 ) ),
					qualifierRank,
					toInt( matcher.group( 5 ) )
			);
		}

		private static int toInt(String value) {
			return value == null || value.isEmpty() ? 0 : Integer.parseInt( value );
		}

		/**
		 * @return the rank of the qualifier, or -1 if we don't recognize it (SNAPSHOT included)
		 */
		private static int qualifierRank(String qualifier) {
			if ( qualifier == null ) {
				return 3;
			}
			return switch ( qualifier.toLowerCase( Locale.ROOT ) ) {
				case "alpha" -> 0;
				case "beta" -> 1;
				case "cr", "rc" -> 2;
				case "final", "ga" -> 3;
				default -> -1;
			};
		}

		@Override
		public int compareTo(ComparableVersion other) {
			int result = Integer.compare( major, other.major );
			if ( result == 0 ) {
				result = Integer.compare( minor, other.minor );
			}
			if ( result == 0 ) {
				result = Integer.compare( micro, other.micro );
			}
			if ( result == 0 ) {
				result = Integer.compare( qualifierRank, other.qualifierRank );
			}
			if ( result == 0 ) {
				result = Integer.compare( qualifierNumber, other.qualifierNumber );
			}
			return result;
		}
	}
}
