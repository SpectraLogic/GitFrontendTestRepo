/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockdomain;

import java.util.UUID;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;

public interface ExtremelyLongBeanExceedingPostgresMaximumCharacterLimit extends DatabasePersistable
{
    String SOME_LONG_PROPERTY_NAME = "someLongPropertyName";
    
    @References( County.class )
    UUID getSomeLongPropertyName();
    
    void setSomeLongPropertyName( final UUID value );
}
