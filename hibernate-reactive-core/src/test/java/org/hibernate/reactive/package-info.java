@FilterDef(name = "current", defaultCondition = "deleted = false")
@FilterDef(name = "region", defaultCondition = "region = :region",
		parameters = @ParamDef(name="region", type = "string"))
package org.hibernate.reactive;

import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.ParamDef;
