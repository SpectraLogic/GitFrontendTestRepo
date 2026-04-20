/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

public final class SecretSqlParameter
{
    public SecretSqlParameter( final Object parameter )
    {
        m_parameter = parameter;
    }
    
    
    public Object getParameter()
    {
        return m_parameter;
    }
    
    
    private final Object m_parameter;
}
