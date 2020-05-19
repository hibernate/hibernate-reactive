package org.hibernate.reactive.engine.spi;

import org.hibernate.engine.spi.SessionBuilderImplementor;
import org.hibernate.reactive.stage.RxSessionFactory;

public interface RxSessionBuilderImplementor
		extends RxSessionFactory.RxSessionBuilder<RxSessionBuilderImplementor>,
		SessionBuilderImplementor<RxSessionBuilderImplementor> {
}
