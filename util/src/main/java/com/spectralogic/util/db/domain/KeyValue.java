/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Schema;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@Schema( "framework" )
@UniqueIndexes( 
{
    @Unique( KeyValue.KEY )
} )
public interface KeyValue extends DatabasePersistable
{
    String KEY = "key";
    
    String getKey();
    
    KeyValue setKey( final String key );
    
    
    String LONG_VALUE = "longValue";
    
    @Optional
    Long getLongValue();
    
    KeyValue setLongValue( final Long value );
    
    
    String BOOLEAN_VALUE = "booleanValue";

    @Optional
    Boolean getBooleanValue();
    
    KeyValue setBooleanValue( final Boolean value );
    
    
    String STRING_VALUE = "stringValue";

    @Optional
    String getStringValue();
    
    KeyValue setStringValue( final String value );
    
    
    String DOUBLE_VALUE = "doubleValue";

    @Optional
    Double getDoubleValue();
    
    KeyValue setDoubleValue( final Double value );
}
