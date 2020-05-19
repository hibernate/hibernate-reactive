/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.configuration;

import org.hibernate.reactive.util.impl.JdbcUrlParser;
import org.junit.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcUrlParserTest {

	@Test
	public void returnsNullForNull() {
		URI uri = JdbcUrlParser.parse( null );
		assertThat( uri ).isNull();
	}

	@Test
	public void uriCreation() {
		URI uri = JdbcUrlParser.parse("jdbc:postgresql://localhost:5432/hreactive");
		assertThat(uri).isNotNull();
	}

	@Test
	public void parsePort() {
		URI uri = JdbcUrlParser.parse("jdbc:postgresql://localhost:5432/hreactive");
		assertThat(uri).hasPort(5432);
	}

	@Test
	public void parseHost() {
		URI uri = JdbcUrlParser.parse("jdbc:postgresql://localhost:5432/hreactive");
		assertThat(uri).hasHost("localhost");
	}

	@Test
	public void parseScheme() {
		URI uri = JdbcUrlParser.parse("jdbc:postgresql://localhost:5432/hreactive");
		assertThat(uri).hasScheme("postgresql");
	}
}
