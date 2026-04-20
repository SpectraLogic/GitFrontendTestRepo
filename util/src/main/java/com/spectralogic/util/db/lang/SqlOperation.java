/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import org.apache.log4j.Priority;

public enum SqlOperation
{
    SELECT( Priority.DEBUG_INT ),
    INSERT( Priority.INFO_INT ),
    UPDATE( Priority.INFO_INT ),
    DELETE( Priority.INFO_INT ),
    ;
    
    
    private SqlOperation( final int defaultLogLevel )
    {
        m_defaultLogLevel = defaultLogLevel;
    }
    
    
    public int getDefaultLogLevel()
    {
        return m_defaultLogLevel;
    }
    
    
    public String toSql()
    {
        return name();
    }
    
    
    private final int m_defaultLogLevel;
}
