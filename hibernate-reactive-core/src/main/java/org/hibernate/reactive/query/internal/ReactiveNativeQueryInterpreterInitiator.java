package org.hibernate.reactive.query.internal;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.engine.query.spi.NativeQueryInterpreter;
import org.hibernate.reactive.query.sqm.spi.ReactiveNativeQueryInterpreter;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class ReactiveNativeQueryInterpreterInitiator implements StandardServiceInitiator<NativeQueryInterpreter> {

	public static ReactiveNativeQueryInterpreterInitiator INSTANCE = new ReactiveNativeQueryInterpreterInitiator();
	@Override
	public NativeQueryInterpreter initiateService(
			Map<String, Object> configurationValues,
			ServiceRegistryImplementor registry) {
		return ReactiveNativeQueryInterpreter.INSTANCE;
	}

	@Override
	public Class<NativeQueryInterpreter> getServiceInitiated() {
		return NativeQueryInterpreter.class;
	}
}
