/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.ConfigureSqlLogLevels;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Schema;
import com.spectralogic.util.db.lang.SqlLogLevels;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@Schema( "framework" )
@UniqueIndexes( 
{
    @Unique( TestRecord.KEY )
} )
@ConfigureSqlLogLevels( SqlLogLevels.ALL_OPERATIONS_LOGGED_AT_DEBUG_LEVEL )
public interface TestRecord extends DatabasePersistable
{
    String KEY = "key";
    
    String getKey();
    
    TestRecord setKey( final String key );
    
    
    String LONG_VALUE = "longValue";
    
    @Optional
    Long getLongValue();
    
    TestRecord setLongValue( final Long value );
    
    
    String BOOLEAN_VALUE = "booleanValue";

    @Optional
    Boolean getBooleanValue();
    
    TestRecord setBooleanValue( final Boolean value );
    
    
    String STRING_VALUE = "stringValue";

    @Optional
    String getStringValue();
    
    TestRecord setStringValue( final String value );
    
    
    String DOUBLE_VALUE = "doubleValue";

    @Optional
    Double getDoubleValue();
    
    TestRecord setDoubleValue( final Double value );
}
