/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.servlet;

import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.lang.NamingConventionType;

final class ServletResponseStrategyImpl implements ServletResponseStrategy
{
    ServletResponseStrategyImpl( final Class< ? > clazz )
    {
        m_name = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( clazz.getSimpleName() );
    }


    public String getServletNameToProvideResponseWith()
    {
        return m_name;
    }
    
    
    private final String m_name;
}
