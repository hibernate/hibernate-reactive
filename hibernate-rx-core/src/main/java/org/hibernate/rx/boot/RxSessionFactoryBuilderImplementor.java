package org.hibernate.rx.boot;

import org.hibernate.*;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.loader.BatchFetchStyle;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.rx.RxSessionFactory;

import java.util.Map;

/**
 * Mixin of {@link RxSessionFactoryBuilder} with {@link SessionFactoryBuilderImplementor}.
 * This is needed in {@link org.hibernate.rx.boot.impl.RxSessionFactoryBuilderImpl} as a
 * self type argument to {@code AbstractDelegatingSessionFactoryBuilderImplementor}.
 *
 * @see org.hibernate.rx.boot.impl.RxSessionFactoryBuilderImpl
 */
public interface RxSessionFactoryBuilderImplementor
		extends SessionFactoryBuilderImplementor, RxSessionFactoryBuilder {

	@Override
	RxSessionFactoryBuilderImplementor applyValidatorFactory(Object validatorFactory);

	@Override
	RxSessionFactoryBuilderImplementor applyBeanManager(Object beanManager);

	@Override
	RxSessionFactoryBuilderImplementor applyName(String sessionFactoryName);

	@Override
	RxSessionFactoryBuilderImplementor applyNameAsJndiName(boolean isJndiName);

	@Override
	RxSessionFactoryBuilderImplementor applyAutoClosing(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyAutoFlushing(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyStatisticsSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyInterceptor(Interceptor interceptor);

	@Override
	RxSessionFactoryBuilderImplementor addSessionFactoryObservers(SessionFactoryObserver... observers);

	@Override
	RxSessionFactoryBuilderImplementor applyCustomEntityDirtinessStrategy(CustomEntityDirtinessStrategy strategy);

	@Override
	RxSessionFactoryBuilderImplementor addEntityNameResolver(EntityNameResolver... entityNameResolvers);

	@Override
	RxSessionFactoryBuilderImplementor applyEntityNotFoundDelegate(EntityNotFoundDelegate entityNotFoundDelegate);

	@Override
	RxSessionFactoryBuilderImplementor applyIdentifierRollbackSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyNullabilityChecking(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyLazyInitializationOutsideTransaction(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyBatchFetchStyle(BatchFetchStyle style);

	@Override
	RxSessionFactoryBuilderImplementor applyDefaultBatchFetchSize(int size);

	@Override
	RxSessionFactoryBuilderImplementor applyMaximumFetchDepth(int depth);

	@Override
	RxSessionFactoryBuilderImplementor applyDefaultNullPrecedence(NullPrecedence nullPrecedence);

	@Override
	RxSessionFactoryBuilderImplementor applyOrderingOfInserts(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyOrderingOfUpdates(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyMultiTenancyStrategy(MultiTenancyStrategy strategy);

	@Override
	RxSessionFactoryBuilderImplementor applyCurrentTenantIdentifierResolver(CurrentTenantIdentifierResolver resolver);

	@Override
	@Deprecated
	RxSessionFactoryBuilderImplementor applyJtaTrackingByThread(boolean enabled);

	@Override
	@Deprecated
	RxSessionFactoryBuilderImplementor applyQuerySubstitutions(Map substitutions);

	@Override
	RxSessionFactoryBuilderImplementor applyStrictJpaQueryLanguageCompliance(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyNamedQueryCheckingOnStartup(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applySecondLevelCacheSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyQueryCacheSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyCacheRegionPrefix(String prefix);

	@Override
	RxSessionFactoryBuilderImplementor applyMinimalPutsForCaching(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyStructuredCacheEntries(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyDirectReferenceCaching(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyAutomaticEvictionOfCollectionCaches(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyJdbcBatchSize(int size);

	@Override
	RxSessionFactoryBuilderImplementor applyJdbcBatchingForVersionedEntities(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyScrollableResultsSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyResultSetsWrapping(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyGetGeneratedKeysSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilderImplementor applyJdbcFetchSize(int size);

	@Override
	RxSessionFactoryBuilderImplementor applyConnectionReleaseMode(ConnectionReleaseMode connectionReleaseMode);

	@Override
	RxSessionFactoryBuilderImplementor applySqlComments(boolean enabled);

	@Override
	RxSessionFactory build();
}
