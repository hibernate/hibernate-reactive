package org.hibernate.rx.persister.entity.impl;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.util.impl.RxUtil;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

public class SequenceRxIdentifierGenerator implements RxIdentifierGenerator {

	private static final RxQueryExecutor queryExecutor = new RxQueryExecutor();

	private final String sql;

	SequenceRxIdentifierGenerator(PersistentClass persistentClass, PersisterCreationContext creationContext) {

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

	public CompletionStage<Optional<Integer>> generate(SessionFactoryImplementor factory) {
		return sql==null ? RxUtil.completedFuture(Optional.empty())
				: queryExecutor.selectInteger(sql, new Object[0], factory);
	}
}
