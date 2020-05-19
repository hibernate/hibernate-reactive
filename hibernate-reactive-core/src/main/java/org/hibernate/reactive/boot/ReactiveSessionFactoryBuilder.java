/*
 * Hibernate Reactive
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.boot;

import org.hibernate.*;
import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.loader.BatchFetchStyle;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.reactive.boot.impl.ReactiveSessionFactoryBuilderImpl;
import org.hibernate.reactive.stage.Stage;

import java.util.Map;

/**
 * A {@link SessionFactoryBuilder builder} for the {@link Stage.SessionFactory}.
 *
 * @see Stage.SessionFactory
 * @see ReactiveSessionFactoryBuilderImpl
 */
public interface ReactiveSessionFactoryBuilder extends SessionFactoryBuilder {

	@Override
	ReactiveSessionFactoryBuilder applyValidatorFactory(Object validatorFactory);

	@Override
	ReactiveSessionFactoryBuilder applyBeanManager(Object beanManager);

	@Override
	ReactiveSessionFactoryBuilder applyName(String sessionFactoryName);

	@Override
	ReactiveSessionFactoryBuilder applyNameAsJndiName(boolean isJndiName);

	@Override
	ReactiveSessionFactoryBuilder applyAutoClosing(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyAutoFlushing(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyStatisticsSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyInterceptor(Interceptor interceptor);

	@Override
	ReactiveSessionFactoryBuilder addSessionFactoryObservers(SessionFactoryObserver... observers);

	@Override
	ReactiveSessionFactoryBuilder applyCustomEntityDirtinessStrategy(CustomEntityDirtinessStrategy strategy);

	@Override
	ReactiveSessionFactoryBuilder addEntityNameResolver(EntityNameResolver... entityNameResolvers);

	@Override
	ReactiveSessionFactoryBuilder applyEntityNotFoundDelegate(EntityNotFoundDelegate entityNotFoundDelegate);

	@Override
	ReactiveSessionFactoryBuilder applyIdentifierRollbackSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyNullabilityChecking(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyLazyInitializationOutsideTransaction(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyBatchFetchStyle(BatchFetchStyle style);

	@Override
	ReactiveSessionFactoryBuilder applyDefaultBatchFetchSize(int size);

	@Override
	ReactiveSessionFactoryBuilder applyMaximumFetchDepth(int depth);

	@Override
	ReactiveSessionFactoryBuilder applyDefaultNullPrecedence(NullPrecedence nullPrecedence);

	@Override
	ReactiveSessionFactoryBuilder applyOrderingOfInserts(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyOrderingOfUpdates(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyMultiTenancyStrategy(MultiTenancyStrategy strategy);

	@Override
	ReactiveSessionFactoryBuilder applyCurrentTenantIdentifierResolver(CurrentTenantIdentifierResolver resolver);

	@Override
	@Deprecated
	ReactiveSessionFactoryBuilder applyJtaTrackingByThread(boolean enabled);

	@Override
	@Deprecated
	ReactiveSessionFactoryBuilder applyQuerySubstitutions(Map substitutions);

	@Override
	ReactiveSessionFactoryBuilder applyStrictJpaQueryLanguageCompliance(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyNamedQueryCheckingOnStartup(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applySecondLevelCacheSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyQueryCacheSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyCacheRegionPrefix(String prefix);

	@Override
	ReactiveSessionFactoryBuilder applyMinimalPutsForCaching(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyStructuredCacheEntries(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyDirectReferenceCaching(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyAutomaticEvictionOfCollectionCaches(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyJdbcBatchSize(int size);

	@Override
	ReactiveSessionFactoryBuilder applyJdbcBatchingForVersionedEntities(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyScrollableResultsSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyResultSetsWrapping(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyGetGeneratedKeysSupport(boolean enabled);

	@Override
	ReactiveSessionFactoryBuilder applyJdbcFetchSize(int size);

	@Override
	ReactiveSessionFactoryBuilder applyConnectionReleaseMode(ConnectionReleaseMode connectionReleaseMode);

	@Override
	ReactiveSessionFactoryBuilder applySqlComments(boolean enabled);

	@Override
	Stage.SessionFactory build();
}
