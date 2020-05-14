package org.hibernate.rx.engine.query.spi;

import java.util.Map;

import org.hibernate.Filter;
import org.hibernate.MappingException;
import org.hibernate.QueryException;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.query.spi.QueryPlanCache;
import org.hibernate.engine.spi.SessionFactoryImplementor;

public class RxQueryPlanCache extends QueryPlanCache {

	/**
	 * Constructs the QueryPlanCache to be used by the given SessionFactory
	 *
	 * @param factory The SessionFactory
	 */
	public RxQueryPlanCache(SessionFactoryImplementor factory) {
		super( factory );
	}

	public RxHQLQueryPlan getHQLRxQueryPlan(String queryString, boolean shallow, Map<String, Filter> enabledFilters) throws QueryException, MappingException {
		HQLQueryPlan hqlQueryPlan = super.getHQLQueryPlan( queryString, shallow, enabledFilters );
		return (RxHQLQueryPlan) hqlQueryPlan;
	}

	@Override
	protected HQLQueryPlan createHQLQueryPlan(String queryString, boolean shallow, Map<String, Filter> enabledFilters, SessionFactoryImplementor factory) {
		return new RxHQLQueryPlan( queryString, shallow, enabledFilters, factory );
	}
}
