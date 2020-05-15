package org.hibernate.rx.hql.spi;

import java.util.Map;

import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.spi.FilterTranslator;
import org.hibernate.hql.spi.QueryTranslator;
import org.hibernate.hql.spi.QueryTranslatorFactory;
import org.hibernate.rx.hql.internal.ast.RxQueryTranslatorImpl;

public class RxASTQueryTranslatorFactory implements QueryTranslatorFactory {

	@Override
	public QueryTranslator createQueryTranslator(
			String queryIdentifier,
			String queryString,
			Map filters,
			SessionFactoryImplementor factory,
			EntityGraphQueryHint entityGraphQueryHint) {
		return new RxQueryTranslatorImpl( queryIdentifier, queryString, filters, factory, entityGraphQueryHint );
	}

	@Override
	public FilterTranslator createFilterTranslator(
			String queryIdentifier, String queryString, Map filters, SessionFactoryImplementor factory) {
		return new RxQueryTranslatorImpl( queryIdentifier, queryString, filters, factory  );
	}
}
