/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes( @Unique( FeatureKey.KEY ) )
public interface FeatureKey extends DatabasePersistable, ErrorMessageObservable< FeatureKey >
{
    String KEY = "key";
    
    FeatureKeyType getKey();
    
    FeatureKey setKey( final FeatureKeyType value );
    
    
    String LIMIT_VALUE = "limitValue";
    
    @Optional
    Long getLimitValue();
    
    FeatureKey setLimitValue( final Long value );
    
    
    String CURRENT_VALUE = "currentValue";
    
    @Optional 
    Long getCurrentValue();
    
    FeatureKey setCurrentValue( final Long value );
    
    
    String EXPIRATION_DATE = "expirationDate";
    
    @Optional
    Date getExpirationDate();
    
    FeatureKey setExpirationDate( final Date value );
}
