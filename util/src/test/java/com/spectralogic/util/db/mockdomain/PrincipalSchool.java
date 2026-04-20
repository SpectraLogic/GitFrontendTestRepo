/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockdomain;

import java.util.UUID;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique({ PrincipalSchool.PRINCIPAL_ID, PrincipalSchool.SCHOOL_ID })
})
public interface PrincipalSchool extends DatabasePersistable
{
    String SCHOOL_ID = "schoolId";

    @References( School.class )
    UUID getSchoolId();
    
    void setSchoolId( final UUID schoolId );
    

    String PRINCIPAL_ID = "principalId";

    @References( Principal.class )
    UUID getPrincipalId();
    
    void setPrincipalId( final UUID principalId );
}
