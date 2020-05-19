package org.hibernate.reactive.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.DB297Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQL8Dialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentInitiator;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * provides an implementation of {@link JdbcEnvironment} that infers
 * the Hibernate {@link org.hibernate.dialect.Dialect} from the JDBC URL.
 */
public class DialectFromUrlJdbcEnvironmentInitiator extends JdbcEnvironmentInitiator {
	public static final DialectFromUrlJdbcEnvironmentInitiator INSTANCE = new DialectFromUrlJdbcEnvironmentInitiator();

	@Override
	public Class<JdbcEnvironment> getServiceInitiated() {
		return JdbcEnvironment.class;
	}

	@Override
	public JdbcEnvironment initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		if ( !configurationValues.containsKey( AvailableSettings.DIALECT ) ) {
			String url = configurationValues.getOrDefault( AvailableSettings.URL, "" ).toString();
			Class<? extends Dialect> dialectClass = null;
			if (url.startsWith("jdbc:mysql:")) {
				dialectClass = MySQL8Dialect.class;
			}
			else if (url.startsWith("jdbc:postgresql:")) {
				dialectClass = PostgreSQL10Dialect.class;
			}
			else if (url.startsWith( "jdbc:db2:" )) {
				dialectClass =  DB297Dialect.class;
			}
			//TODO etc
			if ( dialectClass != null ) {
				configurationValues.put( AvailableSettings.DIALECT, dialectClass.getName() );
			}
		}
		return super.initiateService( configurationValues, registry );
	}
}
