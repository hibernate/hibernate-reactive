/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.stage.Stage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StandAloneReactiveTest {

	@Test
	public void createReactiveSessionFactory() {
		StandardServiceRegistry registry = new ReactiveServiceRegistryBuilder()
				.applySetting( Settings.TRANSACTION_COORDINATOR_STRATEGY, "jta" )
				.applySetting( Settings.DIALECT, PostgreSQLDialect.class.getName() )
				.applySetting( Settings.URL, "jdbc:postgresql://localhost/hreact?user=none" )
				.build();

		Stage.SessionFactory factory = new MetadataSources( registry )
				.buildMetadata()
				.getSessionFactoryBuilder()
				.build()
				.unwrap( Stage.SessionFactory.class );

		assertThat( factory ).isNotNull();
	}
}
