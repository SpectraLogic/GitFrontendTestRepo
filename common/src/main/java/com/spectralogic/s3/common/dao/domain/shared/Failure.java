/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.Date;

import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.db.lang.DatabasePersistable;

public interface Failure< T extends DatabasePersistable, E extends Enum< E > > 
    extends SimpleBeanSafeToProxy, ErrorMessageObservable< T >
{
    String DATE = "date";
    @DefaultToCurrentDate
    @SortBy( direction = Direction.DESCENDING )
    Date getDate();
    
    T setDate( final Date value );
    
    
    String TYPE = "type";
    
    E getType();
    
    T setType( final E value );
}
