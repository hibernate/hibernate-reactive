package org.hibernate.reactive.service;

import org.hibernate.JDBCException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentImpl;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.exception.JDBCConnectionException;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

/**
 * A {@link org.hibernate.engine.jdbc.env.spi.JdbcEnvironment} that
 * doesn't log errors to the console when there is no JDBC driver.
 */
public class ReactiveJdbcEnvironmentImpl extends JdbcEnvironmentImpl {

	public ReactiveJdbcEnvironmentImpl(ServiceRegistryImplementor registry, DialectFactory dialectFactory,
								Map configurationValues) {
		super( registry, dialectFactory.buildDialect( configurationValues, null ) );
	}

	public ReactiveJdbcEnvironmentImpl(ServiceRegistryImplementor serviceRegistry, Dialect dialect,
									   DatabaseMetaData databaseMetaData)
			throws SQLException {
		super( serviceRegistry, dialect, databaseMetaData );
	}

	SqlExceptionHelper superSqlExceptionHelper() {
		return super.getSqlExceptionHelper();
	}

	@Override
	public SqlExceptionHelper getSqlExceptionHelper() {
		return new SqlExceptionHelper( false ) {
			@Override
			public JDBCException convert(SQLException sqlException, String message) {
				if ( !"08001".equals( sqlException.getSQLState() ) ) {
					return superSqlExceptionHelper().convert( sqlException, message );
				}
				return new JDBCConnectionException( "no JDBC driver available", sqlException );
			}
			@Override
			public JDBCException convert(SQLException sqlException, String message, String sql) {
				if ( !"08001".equals( sqlException.getSQLState() ) ) {
					return superSqlExceptionHelper().convert( sqlException, message, sql );
				}
				return new JDBCConnectionException( "no JDBC driver available", sqlException );
			}
		};
	}
}
