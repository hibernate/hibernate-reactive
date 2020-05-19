package org.hibernate.reactive.service.initiator;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.hql.spi.QueryTranslatorFactory;
import org.hibernate.reactive.hql.spi.RxASTQueryTranslatorFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class RxQueryTranslatorFactoryInitiator implements StandardServiceInitiator<QueryTranslatorFactory> {
	public static final RxQueryTranslatorFactoryInitiator INSTANCE = new RxQueryTranslatorFactoryInitiator();

	@Override
	public QueryTranslatorFactory initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new RxASTQueryTranslatorFactory();
	}

	@Override
	public Class<QueryTranslatorFactory> getServiceInitiated() {
		return QueryTranslatorFactory.class;
	}
}
