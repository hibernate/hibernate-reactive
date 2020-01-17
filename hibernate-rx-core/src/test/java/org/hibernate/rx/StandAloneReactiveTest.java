package org.hibernate.rx;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.PostgreSQL9Dialect;
import org.hibernate.rx.boot.RxHibernateSessionFactoryBuilder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StandAloneReactiveTest {

	@Test
	public void createReactiveSessionFactory() {
		StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.applySetting( AvailableSettings.TRANSACTION_COORDINATOR_STRATEGY, "jta" )
				.applySetting( AvailableSettings.DIALECT, PostgreSQL9Dialect.class.getName() )
				.build();

		RxHibernateSessionFactory rxf = new MetadataSources( registry )
				.buildMetadata()
				.getSessionFactoryBuilder()
				.unwrap( RxHibernateSessionFactoryBuilder.class )
				.build();

		assertThat( rxf ).isNotNull();
	}
}
