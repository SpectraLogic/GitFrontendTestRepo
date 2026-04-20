/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockdomain;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;

@Indexes( @Index( Principal.NAME ) )
public interface Principal extends DatabasePersistable
{
    String NAME = "name";
    
    String getName();
    
    void setName( final String name );
    
    
    String TYPE = "type";
    
    TeacherType getType();
    
    void setType( final TeacherType name );
}
