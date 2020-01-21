package org.hibernate.rx;

import org.hibernate.SessionBuilder;
import org.hibernate.SessionFactory;
import org.hibernate.rx.engine.spi.RxSessionBuilderImplementor;

/**
 * Factory for {@link RxSession reactive sessions}.
 *
 * @see org.hibernate.rx.engine.impl.RxSessionFactoryImpl
 */
public interface RxSessionFactory extends SessionFactory {

	/**
	 * Obtain a new {@link RxSession reactive session}, the
	 * main interaction point between the user's program and
	 * Hibernate RX.
	 */
	RxSession openRxSession();

	@Override
	RxSessionBuilderImplementor withOptions();

	/**
	 * A Hibernate {@link SessionBuilder} allowing specification
	 * of options for a {@link RxSession reactive session}.
	 */
	//TODO: Hibernate RX-specific options go here
	interface RxSessionBuilder<T extends RxSessionBuilder> extends SessionBuilder<T> {
		/**
		 * Obtain a new {@link RxSession reactive session}
		 * with these options.
		 */
		RxSession openRxSession();
	}
}
