package org.hibernate.reactive.util.impl;

import java.net.URI;

public class JdbcUrlParser {

	public static URI parse(String url) {
		if ( url == null ) {
			return null;
		}

		if ( url.startsWith( "jdbc:" ) ) {
			return URI.create( url.substring( 5 ) );
		}

		return URI.create( url );
	}
}
