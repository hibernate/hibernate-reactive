/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.configuration;

import java.net.URI;

import static org.assertj.core.api.Assertions.*;
import org.junit.Test;

public class JdbcUrlParserTest {

	@Test
	public void returnsNullForNull() {
		URI uri = JdbcUrlParser.parse( null );
		assertThat( uri ).isNull();
	}

	@Test
	public void uriCreation() {
		URI uri = JdbcUrlParser.parse( "jdbc:postgresql://localhost:5432/hibernate-rx" );
		assertThat( uri ).isNotNull();
	}

	@Test
	public void parsePort() {
		URI uri = JdbcUrlParser.parse( "jdbc:postgresql://localhost:5432/hibernate-rx" );
		assertThat( uri ).hasPort( 5432 );
	}

	@Test
	public void parseHost() {
		URI uri = JdbcUrlParser.parse( "jdbc:postgresql://localhost:5432/hibernate-rx" );
		assertThat( uri ).hasHost( "localhost" );
	}

	@Test
	public void parseScheme() {
		URI uri = JdbcUrlParser.parse( "jdbc:postgresql://localhost:5432/hibernate-rx" );
		assertThat( uri ).hasScheme( "postgresql" );
	}
}
