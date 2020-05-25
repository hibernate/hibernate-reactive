package org.hibernate.reactive.session.impl;

import org.hibernate.jpa.spi.HibernateEntityManagerImplementor.QueryOptions;
import org.hibernate.query.criteria.internal.SelectionImplementor;
import org.hibernate.query.criteria.internal.ValueHandlerFactory;
import org.hibernate.query.criteria.internal.compile.ImplicitParameterBinding;
import org.hibernate.type.Type;

import javax.persistence.criteria.Selection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link QueryOptions} for reactive criteria queries.
 *
 * @author Gavin King
 */
class CriteriaQueryOptions implements QueryOptions {

	private final SelectionImplementor<?> selection;
	private final Map<String, Class> implicitParameterTypes;

	@SuppressWarnings("rawtypes")
	public CriteriaQueryOptions(Selection<?> selection,
								List<ImplicitParameterBinding> implicitParameterBindings) {
		this.selection = (SelectionImplementor<?>) selection;
		this.implicitParameterTypes = extractTypeMap( implicitParameterBindings );
	}

	@Override @SuppressWarnings("rawtypes")
	public List<ValueHandlerFactory.ValueHandler> getValueHandlers() {
		return selection == null ? null : selection.getValueHandlers();
	}

	@Override @SuppressWarnings("rawtypes")
	public Map<String, Class> getNamedParameterExplicitTypes() {
		return implicitParameterTypes;
	}

	@Override
	public ResultMetadataValidator getResultMetadataValidator() {
		return returnTypes -> validate( returnTypes, selection );
	}

	private static void validate(Type[] returnTypes, SelectionImplementor<?> selection) {
		if (selection != null) { //only for queries
			if ( selection.isCompoundSelection() ) {
				if ( returnTypes.length != selection.getCompoundSelectionItems().size() ) {
					throw new IllegalStateException(
							"Number of return values [" + returnTypes.length +
									"] did not match expected [" +
									selection.getCompoundSelectionItems().size() + "]"
					);
				}
			}
			else {
				if (returnTypes.length > 1) {
					throw new IllegalStateException(
							"Number of return values [" + returnTypes.length +
									"] did not match expected [1]"
					);
				}
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private static Map<String, Class> extractTypeMap(List<ImplicitParameterBinding> implicitParameterBindings) {
		final HashMap<String,Class> map = new HashMap<>();
		for ( ImplicitParameterBinding implicitParameter : implicitParameterBindings ) {
			map.put( implicitParameter.getParameterName(), implicitParameter.getJavaType() );
		}
		return map;
	}

}
