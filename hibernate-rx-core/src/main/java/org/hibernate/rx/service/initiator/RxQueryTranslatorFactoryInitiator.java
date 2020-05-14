package org.hibernate.rx.service.initiator;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.hql.spi.QueryTranslatorFactory;
import org.hibernate.rx.hql.spi.RxASTQueryTranslatorFactory;
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
