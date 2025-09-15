/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bythecode.spi;

import org.hibernate.boot.Metadata;
import org.hibernate.bytecode.enhance.spi.interceptor.BytecodeLazyAttributeInterceptor;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeLoadingInterceptor;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributesMetadata;
import org.hibernate.bytecode.internal.BytecodeEnhancementMetadataPojoImpl;
import org.hibernate.bytecode.spi.NotInstrumentedException;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.reactive.bythecode.enhance.spi.interceptor.ReactiveEnhancementAsProxyLazinessInterceptor;
import org.hibernate.reactive.bythecode.enhance.spi.internal.ReactiveLazyAttributeLoadingInterceptor;
import org.hibernate.type.CompositeType;

import java.util.Set;

import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptableType;

/**
 * Extends {@link BytecodeEnhancementMetadataPojoImpl} to inject Reactive versions of {@link BytecodeLazyAttributeInterceptor}
 */
public class ReactiveBytecodeEnhancementMetadataPojoImplAdapter extends BytecodeEnhancementMetadataPojoImpl {

	public static BytecodeEnhancementMetadataPojoImpl from(
			PersistentClass persistentClass,
			Set<String> identifierAttributeNames,
			CompositeType nonAggregatedCidMapper,
			boolean collectionsInDefaultFetchGroupEnabled,
			Metadata metadata) {
		final Class<?> mappedClass = persistentClass.getMappedClass();
		final boolean enhancedForLazyLoading = isPersistentAttributeInterceptableType( mappedClass );
		final LazyAttributesMetadata lazyAttributesMetadata = enhancedForLazyLoading
				? LazyAttributesMetadata.from( persistentClass, true, collectionsInDefaultFetchGroupEnabled, metadata )
				: LazyAttributesMetadata.nonEnhanced( persistentClass.getEntityName() );

		return new ReactiveBytecodeEnhancementMetadataPojoImplAdapter(
				persistentClass.getEntityName(),
				mappedClass,
				identifierAttributeNames,
				nonAggregatedCidMapper,
				enhancedForLazyLoading,
				lazyAttributesMetadata
		);
	}

	ReactiveBytecodeEnhancementMetadataPojoImplAdapter(String entityName, Class<?> mappedClass, Set<String> identifierAttributeNames, CompositeType nonAggregatedCidMapper, boolean enhancedForLazyLoading, LazyAttributesMetadata lazyAttributesMetadata) {
		super(
				entityName,
				mappedClass,
				identifierAttributeNames,
				nonAggregatedCidMapper,
				enhancedForLazyLoading,
				lazyAttributesMetadata
		);
	}

	@Override
	public LazyAttributeLoadingInterceptor injectInterceptor(
			Object entity,
			Object identifier,
			SharedSessionContractImplementor session) throws NotInstrumentedException {
		if ( !isEnhancedForLazyLoading() ) {
			throw new NotInstrumentedException( "Entity class [" + getEntityClass()
					.getName() + "] is not enhanced for lazy loading" );
		}

		if ( !getEntityClass().isInstance( entity ) ) {
			throw new IllegalArgumentException(
					String.format(
							"Passed entity instance [%s] is not of expected type [%s]",
							entity,
							getEntityName()
					)
			);
		}
		final LazyAttributeLoadingInterceptor interceptor = new ReactiveLazyAttributeLoadingInterceptor(
				getLazyAttributeLoadingInterceptorState(),
				identifier,
				session
		);

		injectInterceptor( entity, interceptor, session );

		return interceptor;
	}

	@Override
	public void injectEnhancedEntityAsProxyInterceptor(
			Object entity,
			EntityKey entityKey,
			SharedSessionContractImplementor session) {
		final EnhancementAsProxyLazinessInterceptor.EntityRelatedState meta =
				getEnhancementAsProxyLazinessInterceptorMetastate( session );
		injectInterceptor(
				entity,
				new ReactiveEnhancementAsProxyLazinessInterceptor( meta, entityKey, session ),
				session
		);
	}
}
