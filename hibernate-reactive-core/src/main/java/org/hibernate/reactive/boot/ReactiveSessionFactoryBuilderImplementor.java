package org.hibernate.reactive.boot;

import org.hibernate.*;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.loader.BatchFetchStyle;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.reactive.boot.impl.ReactiveSessionFactoryBuilderImpl;
import org.hibernate.reactive.stage.Stage;

import java.util.Map;

/**
 * Mixin of {@link ReactiveSessionFactoryBuilder} with {@link SessionFactoryBuilderImplementor}.
 * This is needed in {@link ReactiveSessionFactoryBuilderImpl} as a
 * self type argument to {@code AbstractDelegatingSessionFactoryBuilderImplementor}.
 *
 * @see ReactiveSessionFactoryBuilderImpl
 */
public interface ReactiveSessionFactoryBuilderImplementor
		extends SessionFactoryBuilderImplementor, ReactiveSessionFactoryBuilder {

	@Override
	ReactiveSessionFactoryBuilderImplementor applyValidatorFactory(Object validatorFactory);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyBeanManager(Object beanManager);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyName(String sessionFactoryName);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyNameAsJndiName(boolean isJndiName);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyAutoClosing(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyAutoFlushing(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyStatisticsSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyInterceptor(Interceptor interceptor);

	@Override
	ReactiveSessionFactoryBuilderImplementor addSessionFactoryObservers(SessionFactoryObserver... observers);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyCustomEntityDirtinessStrategy(CustomEntityDirtinessStrategy strategy);

	@Override
	ReactiveSessionFactoryBuilderImplementor addEntityNameResolver(EntityNameResolver... entityNameResolvers);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyEntityNotFoundDelegate(EntityNotFoundDelegate entityNotFoundDelegate);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyIdentifierRollbackSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyNullabilityChecking(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyLazyInitializationOutsideTransaction(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyBatchFetchStyle(BatchFetchStyle style);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyDefaultBatchFetchSize(int size);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyMaximumFetchDepth(int depth);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyDefaultNullPrecedence(NullPrecedence nullPrecedence);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyOrderingOfInserts(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyOrderingOfUpdates(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyMultiTenancyStrategy(MultiTenancyStrategy strategy);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyCurrentTenantIdentifierResolver(CurrentTenantIdentifierResolver resolver);

	@Override
	@Deprecated
	ReactiveSessionFactoryBuilderImplementor applyJtaTrackingByThread(boolean enabled);

	@Override
	@Deprecated
	ReactiveSessionFactoryBuilderImplementor applyQuerySubstitutions(Map substitutions);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyStrictJpaQueryLanguageCompliance(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyNamedQueryCheckingOnStartup(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applySecondLevelCacheSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyQueryCacheSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyCacheRegionPrefix(String prefix);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyMinimalPutsForCaching(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyStructuredCacheEntries(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyDirectReferenceCaching(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyAutomaticEvictionOfCollectionCaches(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyJdbcBatchSize(int size);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyJdbcBatchingForVersionedEntities(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyScrollableResultsSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyResultSetsWrapping(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyGetGeneratedKeysSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyJdbcFetchSize(int size);

	@Override
	ReactiveSessionFactoryBuilderImplementor applyConnectionReleaseMode(ConnectionReleaseMode connectionReleaseMode);

	@Override
	ReactiveSessionFactoryBuilderImplementor applySqlComments(boolean enabled);

	@Override
	Stage.SessionFactory build();
}
