/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.connections.spi.DatabaseConnectionInfo;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.env.JdbcMetadataOnBoot;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentImpl;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentInitiator;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import io.vertx.sqlclient.spi.DatabaseMetadata;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;
import static org.hibernate.cfg.JdbcSettings.ALLOW_METADATA_ON_BOOT;
import static org.hibernate.internal.util.config.ConfigurationHelper.getBooleanWrapper;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A Hibernate {@linkplain StandardServiceInitiator service initiator}
 * that provides an implementation of {@link JdbcEnvironment} that infers
 * the Hibernate {@link org.hibernate.dialect.Dialect} from the JDBC URL.
 */
public class NoJdbcEnvironmentInitiator extends JdbcEnvironmentInitiator
		implements StandardServiceInitiator<JdbcEnvironment> {

	public static final NoJdbcEnvironmentInitiator INSTANCE = new NoJdbcEnvironmentInitiator();

	@Override
	public Class<JdbcEnvironment> getServiceInitiated() {
		return JdbcEnvironment.class;
	}

	@Override
	protected void logConnectionInfo(DatabaseConnectionInfo databaseConnectionInfo) {
		// Nothing to do we log the connection info somewhere else
	}

	@Override
	protected JdbcEnvironmentImpl getJdbcEnvironmentWithExplicitConfiguration(
			Map<String, Object> configurationValues,
			ServiceRegistryImplementor registry,
			DialectFactory dialectFactory,
			DialectResolutionInfo dialectResolutionInfo) {
		return super.getJdbcEnvironmentWithExplicitConfiguration(
				configurationValues,
				registry,
				dialectFactory,
				dialectResolutionInfo
		);
	}

	@Override
	protected JdbcEnvironmentImpl getJdbcEnvironmentWithDefaults(
			Map<String, Object> configurationValues,
			ServiceRegistryImplementor registry,
			DialectFactory dialectFactory) {
		return new JdbcEnvironmentImpl( registry, new DialectBuilder( configurationValues, registry )
				.build( dialectFactory )
		);
	}

	@Override
	protected JdbcEnvironmentImpl getJdbcEnvironmentUsingJdbcMetadata(
			JdbcMetadataOnBoot jdbcMetadataAccess,
			Map<String, Object> configurationValues,
			ServiceRegistryImplementor registry,
			DialectFactory dialectFactory,
			String explicitDatabaseName,
			Integer explicitDatabaseMajorVersion,
			Integer explicitDatabaseMinorVersion,
			String explicitDatabaseVersion) {
		if ( getBooleanWrapper( ALLOW_METADATA_ON_BOOT, configurationValues, true ) ) {
			// We query the database for the metadata to build the Dialect
			final Dialect dialect = new DialectBuilder( configurationValues, registry )
					.build(
							dialectFactory,
							new ExplicitMetadata(
									explicitDatabaseName,
									explicitDatabaseMajorVersion,
									explicitDatabaseMinorVersion,
									explicitDatabaseVersion
							)
					);
			return new JdbcEnvironmentImpl( registry, dialect );
		}
		else {
			// We don't query the database but use default config values
			return getJdbcEnvironmentWithDefaults( configurationValues, registry, dialectFactory );
		}
	}

	private static class DialectBuilder {

		private final Map<String, Object> configurationValues;
		private final ServiceRegistry registry;

		public DialectBuilder(Map<String, Object> configurationValues, ServiceRegistry registry) {
			this.configurationValues = configurationValues;
			this.registry = registry;
		}

		public Dialect build(DialectFactory dialectFactory) {
			return dialectFactory.buildDialect( configurationValues, this::dialectResolutionInfo );
		}

		public Dialect build(DialectFactory dialectFactory, ExplicitMetadata explicitMetadata) {
			return dialectFactory.buildDialect( configurationValues, () -> dialectResolutionInfo( explicitMetadata ) );
		}

		private DialectResolutionInfo dialectResolutionInfo() {
			return dialectResolutionInfo( DialectBuilder::buildResolutionInfo );
		}

		private DialectResolutionInfo dialectResolutionInfo(ExplicitMetadata explicitMetadata) {
			return dialectResolutionInfo( reactiveConnection -> DialectBuilder
					.buildResolutionInfo( reactiveConnection, explicitMetadata )
			);
		}

		private DialectResolutionInfo dialectResolutionInfo(Function<ReactiveConnection, CompletionStage<ReactiveDialectResolutionInfo>> dialectResolutionFunction) {
			return registry
					.getService( ReactiveConnectionPool.class )
					// The default SqlExceptionHelper in ORM requires the dialect, but we haven't created a dialect yet,
					// so we need to override it at this stage, or we will have an exception.
					.getConnection( new SqlExceptionHelper( true ) )
					.thenCompose( dialectResolutionFunction )
					.toCompletableFuture().join();
		}

		private static CompletionStage<ReactiveDialectResolutionInfo> buildResolutionInfo(ReactiveConnection connection) {
			return buildResolutionInfo( connection, null );
		}

		private static CompletionStage<ReactiveDialectResolutionInfo> buildResolutionInfo(ReactiveConnection connection, ExplicitMetadata explicitMetadata) {
			return resolutionInfoStage( connection, explicitMetadata )
					.handle( CompletionStages::handle )
					.thenCompose( handled -> {
						if ( handled.hasFailed() ) {
							// Something has already gone wrong: try to close the connection
							// and return the original failure
							return connection.close()
									.handle( (unused, throwable) -> handled.getResultAsCompletionStage() )
									.thenCompose( identity() );
						}
						else {
							return connection.close()
									.thenCompose( v -> handled.getResultAsCompletionStage() );
						}
					} );
		}

		/**
		 * @see org.hibernate.dialect.Database#POSTGRESQL for recognizing CockroachDB
		 */
		private static CompletionStage<ReactiveDialectResolutionInfo> resolutionInfoStage(ReactiveConnection connection, ExplicitMetadata explicitMetadata) {
			final DatabaseMetadata databaseMetadata = explicitMetadata != null
					? new ReactiveDatabaseMetadata( connection.getDatabaseMetadata(), explicitMetadata )
					: connection.getDatabaseMetadata();

			// If the product name is explicitly set to Postgres, we are not going to override it
			if ( ( explicitMetadata == null || explicitMetadata.productName == null )
					&& databaseMetadata.productName().equalsIgnoreCase( "PostgreSQL" ) ) {
				// CockroachDB returns "PostgreSQL" as product name in the metadata.
				// So, we need to check if the database is PostgreSQL or CockroachDB
				// We follow the same approach used by ORM: run a new query and check the full version metadata
				// See org.hibernate.dialect.Database.POSTGRESQL#createDialect
				return connection.select( "select version()" )
						.thenApply( DialectBuilder::readFullVersion )
						.thenApply( fullVersion -> fullVersion.startsWith( "Cockroach" )
								? new ReactiveDatabaseMetadata( "Cockroach", databaseMetadata )
								: databaseMetadata
						)
						.thenApply( ReactiveDialectResolutionInfo::new );
			}

			return completedFuture( new ReactiveDialectResolutionInfo( databaseMetadata ) );
		}

		private static String readFullVersion(ReactiveConnection.Result result) {
			return result.hasNext()
					? (String) result.next()[0]
					: "";
		}
	}

	/**
	 * Utility class to pass around explicit metadata properties.
	 * It's different from {@link DatabaseMetadata} because values can be null.
	 */
	private static class ExplicitMetadata {
		private final String productName;
		private final String fullVersion;
		private final Integer majorVersion;
		private final Integer minorVersion;

		public ExplicitMetadata(String explicitDatabaseName, Integer explicitDatabaseMajorVersion, Integer explicitDatabaseMinorVersion, String explicitDatabaseVersion ) {
			this.productName = explicitDatabaseName;
			this.fullVersion = explicitDatabaseVersion;
			this.majorVersion = explicitDatabaseMajorVersion;
			this.minorVersion = explicitDatabaseMinorVersion;
		}
	}

	private static class ReactiveDatabaseMetadata implements DatabaseMetadata {
		public final String productName;
		public final String fullVersion;
		public final int majorVersion;
		public final int minorVersion;

		public ReactiveDatabaseMetadata(String productName, DatabaseMetadata databaseMetadata) {
			this.productName = productName;
			this.fullVersion = databaseMetadata.productName();
			this.majorVersion = databaseMetadata.majorVersion();
			this.minorVersion = databaseMetadata.minorVersion();
		}

		public ReactiveDatabaseMetadata(DatabaseMetadata metadata, ExplicitMetadata explicitMetadata) {
			productName = requireNonNullElse( explicitMetadata.productName, metadata.productName() );
			fullVersion = requireNonNullElse( explicitMetadata.fullVersion, metadata.fullVersion() );
			majorVersion = requireNonNullElse( explicitMetadata.majorVersion, metadata.majorVersion() );
			minorVersion = requireNonNullElse( explicitMetadata.minorVersion, metadata.minorVersion() );
		}

		@Override
		public String productName() {
			return productName;
		}

		@Override
		public String fullVersion() {
			return fullVersion;
		}

		@Override
		public int majorVersion() {
			return majorVersion;
		}

		@Override
		public int minorVersion() {
			return minorVersion;
		}
	}

	private static class ReactiveDialectResolutionInfo implements DialectResolutionInfo {

		private final DatabaseMetadata metadata;

		private ReactiveDialectResolutionInfo(DatabaseMetadata metadata) {
			this.metadata = metadata;
		}

		@Override
		public String getDatabaseName() {
			return metadata.productName();
		}

		@Override
		public String getDatabaseVersion() {
			return metadata.fullVersion();
		}

		@Override
		public int getDatabaseMajorVersion() {
			return metadata.majorVersion();
		}

		@Override
		public int getDatabaseMinorVersion() {
			return metadata.minorVersion();
		}

		@Override
		public int getDatabaseMicroVersion() {
			return databaseMicroVersion( metadata.fullVersion(), metadata.majorVersion(), metadata.minorVersion() );
		}

		// We should move this in ORM and avoid duplicated code
		private static int databaseMicroVersion(String version, int major, int minor) {
			final String prefix = major + "." + minor + ".";
			if ( version.startsWith( prefix ) ) {
				try {
					final String substring = version.substring( prefix.length() );
					final String micro = new StringTokenizer( substring, " .,-:;/()[]" ).nextToken();
					return parseInt( micro );
				}
				catch (NumberFormatException nfe) {
					return 0;
				}
			}
			return 0;
		}

		@Override
		public String getDriverName() {
			return getDatabaseName();
		}

		@Override
		public int getDriverMajorVersion() {
			return getDatabaseMajorVersion();
		}

		@Override
		public int getDriverMinorVersion() {
			return getDatabaseMinorVersion();
		}

		@Override
		public String getSQLKeywords() {
			// Vert.x metadata doesn't have this info
			return null;
		}

		@Override
		public String toString() {
			return getDatabaseMajorVersion() + "." + getDatabaseMinorVersion();
		}
	}
}
