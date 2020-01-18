package org.hibernate.rx;

import org.hibernate.SessionBuilder;
import org.hibernate.SessionFactory;
import org.hibernate.rx.engine.spi.RxSessionBuilderImplementor;

/**
 * Factory for {@link RxSessionInternal}
 */
public interface RxSessionFactory extends SessionFactory {

	RxSession openRxSession();

	@Override
	RxSessionBuilderImplementor withOptions();

	interface RxSessionBuilder<T extends RxSessionBuilder> extends SessionBuilder<T> {
		RxSession openRxSession();
	}
}
