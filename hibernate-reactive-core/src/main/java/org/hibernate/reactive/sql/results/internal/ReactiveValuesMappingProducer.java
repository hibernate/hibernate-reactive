/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.util.List;

import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesMappingProducerStandard;

public class ReactiveValuesMappingProducer extends JdbcValuesMappingProducerStandard  {

	public ReactiveValuesMappingProducer(List<SqlSelection> sqlSelections, List<DomainResult<?>> domainResults) {
		super( sqlSelections, domainResults );
	}
}
