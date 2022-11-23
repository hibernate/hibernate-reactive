package org.hibernate.reactive.query.sqm;

import org.hibernate.query.spi.SqmQuery;
import org.hibernate.reactive.query.ReactiveSelectionQuery;

/**
 * @see org.hibernate.query.sqm.SqmSelectionQuery
 */
public interface ReactiveSqmSelectionQuery extends ReactiveSelectionQuery, SqmQuery {
}
