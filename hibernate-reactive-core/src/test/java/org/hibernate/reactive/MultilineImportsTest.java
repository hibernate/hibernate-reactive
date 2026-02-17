/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test importing a SQL script which is using the multi-line format
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class MultilineImportsTest extends BaseReactiveTest {

	private static CompletionStage<List<Object>> runQuery(Stage.Session s) {
		return s.createSelectionQuery( "from Hero h where h.heroName = :name", Object.class )
				.setParameter( "name", "Galadriel" )
				.getResultList();
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Hero.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		// Testing the model from the Quarkus workshops (a smaller extract of):
		// https://github.com/quarkusio/quarkus-workshops/tree/master/quarkus-workshop-super-heroes/super-heroes/rest-hero
		configuration.setProperty( AvailableSettings.HBM2DDL_IMPORT_FILES, "/complexMultilineImports.sql" );
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		configuration.setProperty( Settings.HBM2DDL_IMPORT_FILES_SQL_EXTRACTOR, "org.hibernate.tool.schema.internal.script.MultiLineSqlScriptExtractor" );
		return configuration;
	}

	@Test
	public void verifyImportScriptHasRun(VertxTestContext context) {
		test(
				context,
				getSessionFactory()
						.withSession( MultilineImportsTest::runQuery )
						.thenAccept( list -> assertThat( list ).hasSize( 1 ) )
		);
	}

	@Entity(name = "Hero")
	@Table(name = "hero")
	public static class Hero {

		@Id
		@GeneratedValue
		public Integer id;

		@Column(unique = true)
		public String heroName;

		public String otherName;

		public int powerLevel;

		public String picture;

		public String powers;

	}

}
