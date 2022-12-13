/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.internal;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.results.ResultSetMapping;
import org.hibernate.query.sql.internal.ResultSetMappingProcessor;
import org.hibernate.reactive.sql.results.ReactiveResultSetMapping;

public class ReactiveResultSetMappingProcessor extends ResultSetMappingProcessor {

	public ReactiveResultSetMappingProcessor(ResultSetMapping resultSetMapping, SessionFactoryImplementor factory) {
		super( resultSetMapping, factory );
	}

	@Override
	public ResultSetMapping generateResultMapping(boolean queryHadAliases) {
		ResultSetMapping resultSetMapping = super.generateResultMapping( queryHadAliases );
		return new ReactiveResultSetMapping( resultSetMapping );
	}
}
