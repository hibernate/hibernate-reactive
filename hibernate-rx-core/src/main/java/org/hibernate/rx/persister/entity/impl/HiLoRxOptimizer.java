package org.hibernate.rx.persister.entity.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.id.enhanced.HiLoOptimizer;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class HiLoRxOptimizer extends HiLoOptimizer implements RxOptimizer<Long> {

	public HiLoRxOptimizer(Class<?> integerType, int incrementSize) {
		super( integerType, incrementSize );
	}

	@Override
	public synchronized CompletionStage<Optional<Long>> generate(RxAccessCallback callback) {

		final GenerationState generationState = locateGenerationState( callback.getTenantIdentifier() );

		if ( !generationState.isInitialized() ) {
			return getValidFirstValueFromDatabase( callback )
					.thenCompose( validFirstValue -> getActualNextValue( generationState, validFirstValue ) );
		}
		else if ( generationState.requiresNewSourceValue() ) {
			return callback.getNextValue()
					.thenCompose( valueFromDatabase -> getActualNextValue( generationState, valueFromDatabase ) );
		}
		else {
			return CompletableFuture.completedFuture(Optional.of(generationState.makeValueThenIncrement().longValue()));
		}
	}

	private CompletionStage<Optional<Long>> getValidFirstValueFromDatabase(RxAccessCallback callback) {
		// TODO: increment if it is < 1
		// while ( generationState.lastSourceValue.lt( 1 ) ) {
		//	generationState.lastSourceValue = callback.getNextValue();
		//}

		return callback.getNextValue();
	}

	private CompletionStage<Optional<Long>> getActualNextValue(GenerationState generationState, Optional<Long> valueFromDataBase) {

		if (!valueFromDataBase.isPresent()) {
			throw new AssertionFailure("No sequence value present!");
		}

		generationState.updateFromNewSourceValue(
				IdentifierGeneratorHelper.getIntegralDataTypeHolder( Long.class ).initialize( valueFromDataBase.get() )
		);

		return CompletableFuture.completedFuture( Optional.of(generationState.makeValueThenIncrement().longValue()));
	}
}
