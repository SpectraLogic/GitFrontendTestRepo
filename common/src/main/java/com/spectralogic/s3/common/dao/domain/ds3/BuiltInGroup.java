/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum BuiltInGroup
{
    EVERYONE( "Everyone" ),
    ADMINISTRATORS( "Administrators" ),
    TAPE_ADMINS( "Tape Admins" ),
    ;
    
    
    private BuiltInGroup( final String name )
    {
        m_name = name;
    }
    
    
    public String getName()
    {
        return m_name;
    }
    
    
    private final String m_name;
}
