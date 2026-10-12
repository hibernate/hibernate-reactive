/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.junit5.Timeout;
import java.util.Collection;
import java.util.List;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.Locale;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * The entity's table name and schema are both explicitly quoted (using the
 * classic Hibernate back-tick quoting convention), so that the generated
 * insert looks like:
 * <pre>
 *     insert into "H R"."F IL M" (title, id) values (?, default)
 * </pre>
 * which mirrors the {@code insert into "SAKILA"."FILM" (...) values (..., default)}
 * statement reported in the issue for an identity-generated entity on Oracle.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class QuotedSchemaAndTableIdentityTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( QuotedFilm.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.DEFAULT_SCHEMA, quotedSchemaName() );
		configuration.setProperty( Settings.JAKARTA_HBM2DDL_CREATE_SCHEMAS, "true" );
		return configuration;
	}

	@Test
	public void testInsertWithQuotedSchemaAndTable(VertxTestContext context) {
		final QuotedFilm film = new QuotedFilm( 1L, "Hibernate Reactive, the Movie" );
		test( context, getMutinySessionFactory().withTransaction( s -> s
						.persist( film ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s
						.find( QuotedFilm.class, film.getId() ) ) )
				.invoke( foundFilm -> assertThat( foundFilm.getTitle() ).isEqualTo( film.getTitle() ) )
		);
	}

	/**
	 * We are using the schema automatically created by Oracle Database for our user account,
	 * as we do not have the privileges to create a separate schema.
	 * For the other databases we test with a schema that doesn't match the username.
	 */
	private static String quotedSchemaName() {
		return dbType() == ORACLE
				? "`" + DatabaseConfiguration.USERNAME.toUpperCase( Locale.ROOT ) + "`"
				: "`h r`";
	}

	@Entity(name = "QuotedFilm")
	@Table(name = "`F IL M`")
	public static class QuotedFilm {
		@Id
		private Long id;

		private String title;

		public QuotedFilm() {
		}

		public QuotedFilm(Long id, String title) {
			this.id = id;
			this.title = title;
		}

		public Long getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}
	}
}
