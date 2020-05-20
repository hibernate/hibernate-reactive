package org.hibernate.reactive;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.PostgreSQL9Dialect;
import org.hibernate.reactive.stage.Stage;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StandAloneReactiveTest {

	@Test
	public void createReactiveSessionFactory() {
		StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.applySetting( AvailableSettings.TRANSACTION_COORDINATOR_STRATEGY, "jta" )
				.applySetting( AvailableSettings.DIALECT, PostgreSQL9Dialect.class.getName() )
				.build();

		Stage.SessionFactory factory = new MetadataSources( registry )
				.buildMetadata()
				.getSessionFactoryBuilder()
				.build()
				.unwrap( Stage.SessionFactory.class );

		assertThat( factory ).isNotNull();
	}
}
