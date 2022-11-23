/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm;

import org.hibernate.query.spi.SqmQuery;
import org.hibernate.reactive.query.ReactiveSelectionQuery;

/**
 * @see org.hibernate.query.sqm.SqmSelectionQuery
 */
public interface ReactiveSqmSelectionQuery extends ReactiveSelectionQuery, SqmQuery {
}
