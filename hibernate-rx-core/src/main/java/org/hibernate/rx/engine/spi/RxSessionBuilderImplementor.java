package org.hibernate.rx.engine.spi;

import org.hibernate.engine.spi.SessionBuilderImplementor;
import org.hibernate.rx.RxSessionFactory;

public interface RxSessionBuilderImplementor
		extends RxSessionFactory.RxSessionBuilder<RxSessionBuilderImplementor>,
		SessionBuilderImplementor<RxSessionBuilderImplementor> {
}
