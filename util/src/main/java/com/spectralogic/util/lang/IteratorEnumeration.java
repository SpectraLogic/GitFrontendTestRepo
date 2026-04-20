/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.util.Enumeration;
import java.util.Iterator;

public final class IteratorEnumeration< T > implements Enumeration< T >
{
    public IteratorEnumeration( final Iterator< T > iterator )
    {
        m_iterator = iterator;
    }


    public T nextElement() 
    {
        return m_iterator.next();
    }


    public boolean hasMoreElements() 
    {
        return m_iterator.hasNext();
    }


    private final Iterator< T > m_iterator;
}
