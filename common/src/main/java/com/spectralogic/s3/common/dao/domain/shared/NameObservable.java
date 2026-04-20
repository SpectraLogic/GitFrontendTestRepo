/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.MustMatchRegularExpression;

public interface NameObservable< T >
{
    String NAME = "name";
    
    @MustMatchRegularExpression( ".+" )
    @SortBy( 10 )
    String getName();
    
    T setName( final String value );
}
