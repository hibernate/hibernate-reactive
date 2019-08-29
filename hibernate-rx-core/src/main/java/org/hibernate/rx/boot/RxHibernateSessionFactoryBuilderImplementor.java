package org.hibernate.rx.boot;

import java.util.Map;

import org.hibernate.ConnectionReleaseMode;
import org.hibernate.CustomEntityDirtinessStrategy;
import org.hibernate.EntityNameResolver;
import org.hibernate.Interceptor;
import org.hibernate.MultiTenancyStrategy;
import org.hibernate.NullPrecedence;
import org.hibernate.SessionFactoryObserver;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.loader.BatchFetchStyle;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.rx.RxHibernateSessionFactory;

/**
 * Builder for the {@link org.hibernate.rx.RxHibernateSessionFactory}
 */
public interface RxHibernateSessionFactoryBuilderImplementor
		extends SessionFactoryBuilderImplementor, RxHibernateSessionFactoryBuilder {

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyValidatorFactory(Object validatorFactory);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyBeanManager(Object beanManager);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyName(String sessionFactoryName);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyNameAsJndiName(boolean isJndiName);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyAutoClosing(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyAutoFlushing(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyStatisticsSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyInterceptor(Interceptor interceptor);

	@Override
	RxHibernateSessionFactoryBuilderImplementor addSessionFactoryObservers(SessionFactoryObserver... observers);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyCustomEntityDirtinessStrategy(CustomEntityDirtinessStrategy strategy);

	@Override
	RxHibernateSessionFactoryBuilderImplementor addEntityNameResolver(EntityNameResolver... entityNameResolvers);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyEntityNotFoundDelegate(EntityNotFoundDelegate entityNotFoundDelegate);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyIdentifierRollbackSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyNullabilityChecking(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyLazyInitializationOutsideTransaction(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyBatchFetchStyle(BatchFetchStyle style);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyDefaultBatchFetchSize(int size);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyMaximumFetchDepth(int depth);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyDefaultNullPrecedence(NullPrecedence nullPrecedence);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyOrderingOfInserts(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyOrderingOfUpdates(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyMultiTenancyStrategy(MultiTenancyStrategy strategy);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyCurrentTenantIdentifierResolver(CurrentTenantIdentifierResolver resolver);

	@Override
	@Deprecated
	RxHibernateSessionFactoryBuilderImplementor applyJtaTrackingByThread(boolean enabled);

	@Override
	@Deprecated
	RxHibernateSessionFactoryBuilderImplementor applyQuerySubstitutions(Map substitutions);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyStrictJpaQueryLanguageCompliance(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyNamedQueryCheckingOnStartup(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applySecondLevelCacheSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyQueryCacheSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyCacheRegionPrefix(String prefix);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyMinimalPutsForCaching(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyStructuredCacheEntries(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyDirectReferenceCaching(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyAutomaticEvictionOfCollectionCaches(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyJdbcBatchSize(int size);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyJdbcBatchingForVersionedEntities(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyScrollableResultsSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyResultSetsWrapping(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyGetGeneratedKeysSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyJdbcFetchSize(int size);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applyConnectionReleaseMode(ConnectionReleaseMode connectionReleaseMode);

	@Override
	RxHibernateSessionFactoryBuilderImplementor applySqlComments(boolean enabled);

	@Override
	RxHibernateSessionFactory build();
}
