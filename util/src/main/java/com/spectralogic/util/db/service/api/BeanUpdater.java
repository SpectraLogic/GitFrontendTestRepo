/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service.api;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.WhereClause;

import java.util.function.Consumer;

public interface BeanUpdater< T extends SimpleBeanSafeToProxy >
{
    public void update( final T bean, final String... propertiesToUpdate );

    public void update(final WhereClause whereClause, final Consumer<T> beanUpdater, final String... propertiesToUpdate);
}
