/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.dialect.Dialect;
import org.hibernate.sql.ast.spi.JdbcParameterRenderer;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.type.descriptor.jdbc.JdbcType;

/**
 * Ensures the rendered SQL matches the syntax of parameters expected by
 * the Vert.x PgClient: parameters are rendered with "$1", "$2", etc..
 * rather than with "?" syntax.
 * We also post-process rendered SQL in PostgresParameters as the current
 * approach needs to be refined; as we close the gap, rendering correctly
 * directly improves performance as the Parameters step can then be skipped.
 * At the time of writing, this service is only invoked for SELECT operations;
 * INSERT and DELETE seem to render SQL over an alternative path; UPDATE untested.
 */
public final class PostgreSQLParameterRenderer implements JdbcParameterRenderer {

	/**
	 * Singleton access
	 */
	public static final PostgreSQLParameterRenderer INSTANCE = new PostgreSQLParameterRenderer();

	private static final int MAX_POSITION_PREPARED = 10;
	private static final String[] RENDERED_PARAM = initConstants( MAX_POSITION_PREPARED );

	private static String[] initConstants(int max) {
		String[] arr = new String[max];
		//index zero unused!
		for ( int i = 1; i < max; i++ ) {
			arr[i] = "$" + i;
		}
		return arr;
	}

	private static String render(final int position) {
		assert position != 0;
		if ( position < MAX_POSITION_PREPARED ) {
			return RENDERED_PARAM[position];
		}
		else {
			return "$" + position;
		}
	}

	@Override
	public void renderJdbcParameter(int position, JdbcType jdbcType, SqlAppender appender, Dialect dialect) {
		jdbcType.appendWriteExpression( render( position ), appender, dialect );
	}

}
