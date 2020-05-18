package org.hibernate.rx.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.cfg.AvailableSettings;
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
public class RxJdbcEnvironmentInitiator extends JdbcEnvironmentInitiator {
	public static final RxJdbcEnvironmentInitiator INSTANCE = new RxJdbcEnvironmentInitiator();

	@Override
	public Class<JdbcEnvironment> getServiceInitiated() {
		return JdbcEnvironment.class;
	}

	@Override
	public JdbcEnvironment initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		if ( !configurationValues.containsKey( AvailableSettings.DIALECT ) ) {
			String url = configurationValues.getOrDefault( AvailableSettings.URL, "" ).toString();
			if (url.startsWith("jdbc:mysql:")) {
				configurationValues.put( AvailableSettings.DIALECT, MySQL8Dialect.class.getName() );
			}
			if (url.startsWith("jdbc:postgresql:")) {
				configurationValues.put( AvailableSettings.DIALECT, PostgreSQL10Dialect.class.getName() );
			}
			//TODO etc
		}
		return super.initiateService( configurationValues, registry );
	}
}
