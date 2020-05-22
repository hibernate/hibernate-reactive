package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.impl.ReactiveSessionInternal;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.util.Properties;
import java.util.concurrent.CompletionStage;

/**
 * Support for JPA's {@link javax.persistence.SequenceGenerator}.
 */
public class SequenceReactiveIdentifierGenerator implements ReactiveIdentifierGenerator<Long> {

	private final String sql;

	SequenceReactiveIdentifierGenerator(PersistentClass persistentClass, PersisterCreationContext creationContext) {

		MetadataImplementor metadata = creationContext.getMetadata();
		SessionFactoryImplementor sessionFactory = creationContext.getSessionFactory();
		Database database = metadata.getDatabase();
		JdbcEnvironment jdbcEnvironment = database.getJdbcEnvironment();

		Properties props = IdentifierGeneration.identifierGeneratorProperties(
				jdbcEnvironment.getDialect(),
				sessionFactory,
				persistentClass
		);

		QualifiedName logicalQualifiedSequenceName =
				IdentifierGeneration.determineSequenceName( props, jdbcEnvironment, database );
		final Namespace namespace = database.locateNamespace(
				logicalQualifiedSequenceName.getCatalogName(),
				logicalQualifiedSequenceName.getSchemaName()
		);
		org.hibernate.boot.model.relational.Sequence sequence =
				namespace.locateSequence( logicalQualifiedSequenceName.getObjectName() );

		if (sequence != null) {
			final Dialect dialect = jdbcEnvironment.getDialect();
			String finalSequenceName = jdbcEnvironment.getQualifiedObjectNameFormatter().format(
					sequence.getName(),
					dialect
			);
			sql = dialect.getSequenceNextValString(finalSequenceName);
		}
		else {
			sql = null;
		}
	}

	@Override
	public CompletionStage<Long> generate(ReactiveSessionInternal session) {
		return sql==null ? CompletionStages.nullFuture()
				: session.getReactiveConnection().selectLong( sql, new Object[0] );
	}
}
