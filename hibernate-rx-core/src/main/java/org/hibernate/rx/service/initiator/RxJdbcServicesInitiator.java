package org.hibernate.rx.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.LobCreationContext;
import org.hibernate.engine.jdbc.LobCreator;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.env.spi.ExtractedDatabaseMetaData;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.internal.JdbcServicesImpl;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.ResultSetWrapper;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * provides an implementation of {@link JdbcServices} that never
 * accesses the database via JDBC.
 */
public class RxJdbcServicesInitiator implements StandardServiceInitiator<JdbcServices> {
	public static final RxJdbcServicesInitiator INSTANCE = new RxJdbcServicesInitiator();

	@Override
	public Class<JdbcServices> getServiceInitiated() {
		return JdbcServices.class;
	}

	@Override
	public JdbcServices initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new RxJdbcServices();
	}

	private static final class RxJdbcServices implements JdbcServices, ServiceRegistryAwareService, Configurable {
		public JdbcServicesImpl delegate = new JdbcServicesImpl();

		@Override
		public void configure(Map configurationValues) {
			configurationValues.put( "hibernate.temp.use_jdbc_metadata_defaults", Boolean.FALSE );
			delegate.configure( configurationValues );
		}

		@Override
		public void injectServices(ServiceRegistryImplementor serviceRegistry) {
			delegate.injectServices( serviceRegistry );
		}

		@Override
		public Dialect getDialect() {
			return delegate.getDialect();
		}

		@Override
		public SqlStatementLogger getSqlStatementLogger() {
			return delegate.getSqlStatementLogger();
		}

		@Override
		public SqlExceptionHelper getSqlExceptionHelper() {
			return delegate.getSqlExceptionHelper();
		}

		@Override
		public ExtractedDatabaseMetaData getExtractedMetaDataSupport() {
			return delegate.getExtractedMetaDataSupport();
		}

		@Override
		public LobCreator getLobCreator(LobCreationContext lobCreationContext) {
			return delegate.getLobCreator( lobCreationContext );
		}

		@Override
		public ResultSetWrapper getResultSetWrapper() {
			return delegate.getResultSetWrapper();
		}

		@Override
		public JdbcEnvironment getJdbcEnvironment() {
			return delegate.getJdbcEnvironment();
		}

		@Override
		public JdbcConnectionAccess getBootstrapJdbcConnectionAccess() {
			return delegate.getBootstrapJdbcConnectionAccess();
		}
	}
}
