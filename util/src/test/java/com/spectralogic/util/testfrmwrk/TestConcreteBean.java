/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;


public final class TestConcreteBean
{
    String getStringProp()
    {
        return m_stringProp;
    }
    
    void setStringProp( final String value )
    {
        m_stringProp = value;
    }
    
    int getIntProp()
    {
        return m_intProp;
    }
    
    void setIntProp( final int value )
    {
        m_intProp = value;
    }
    
    
    private volatile String m_stringProp;
    private volatile int m_intProp;
}
