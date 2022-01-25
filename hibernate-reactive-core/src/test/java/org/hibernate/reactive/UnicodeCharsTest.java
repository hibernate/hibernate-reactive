/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.cfg.AvailableSettings.USE_NATIONALIZED_CHARACTER_DATA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Test non ASCII Chars
 * <p>
 *     Sql Server changes the supported enconding based on the type of the column.
 *     The default type won't work, so we need a different entity with a column
 *     definition.
 * </p>
 */
public class UnicodeCharsTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		final Configuration configuration = super.constructConfiguration();
		configuration.getProperties().put( USE_NATIONALIZED_CHARACTER_DATA, dbType() != MARIA );
		configuration.addAnnotatedClass( UnicodeString.class );
		return configuration;
	}

	@Test
	public void testStringTypeWithUnicode(TestContext context) {
		final String expected = "\uD83D\uDD02 ﷽ 雲  (͡° ͜ʖ ͡ °) Č";
		Object original = new UnicodeString( expected );

		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( original ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.createQuery( "select unicodeString from UnicodeString" ).getSingleResultOrNull() ) )
				.thenAccept( found -> context.assertEquals( expected, found ) )
		);
	}

	@Entity(name = "UnicodeString")
	@Table(name = "UnicodeString")
	private static class UnicodeString {

		@Id
		@GeneratedValue
		public Integer id;

		public String unicodeString;

		public UnicodeString() {
		}

		public UnicodeString(String unicodeString) {
			this.unicodeString = unicodeString;
		}

	}
}
