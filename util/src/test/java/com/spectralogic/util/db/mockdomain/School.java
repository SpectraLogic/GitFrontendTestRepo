/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockdomain;

import java.util.UUID;

import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

@Indexes(
{
    @Index( School.NAME )
})
public interface School extends DatabasePersistable
{
    String TYPE = "type";
    
    SchoolType getType();
    
    School setType( final SchoolType name );
    
    
    String NAME = "name";
    
    String getName();
    
    School setName( final String name );
    
    
    String ADDRESS = "address";
    
    @Optional
    String getAddress();
    
    School setAddress( final String address );
    
    
    String COUNTY_ID = "countyId";
    
    @Optional
    @References( County.class )
    UUID getCountyId();
    
    School setCountyId( final UUID countyId );


    String ACTIVE = "active";

    boolean getActive();

    School setActive( final boolean active );
}
