/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.comparator;

import java.util.Comparator;

public final class NullHandlingComparator implements Comparator< Object >
{
    public NullHandlingComparator( final Comparator< Object > decoratedComparator )
    {
        m_decoratedComparator = decoratedComparator;
    }
    
    
    public int compare( final Object o1, final Object o2 )
    {
        if ( null == o1 && null == o2 )
        {
            return 0;
        }
        if ( null == o1 )
        {
            return -1;
        }
        if ( null == o2 )
        {
            return 1;
        }
        
        if ( null != m_decoratedComparator )
        {
            return m_decoratedComparator.compare( o1, o2 );
        }
        if ( Comparable.class.isAssignableFrom( o1.getClass() ) )
        {
            @SuppressWarnings( "unchecked" )
            final Comparable< Object > comparable = (Comparable< Object >)o1;
            return comparable.compareTo( o2 );
        }
        
        throw new UnsupportedOperationException(
                "A decorated comparator was not provided and " + o1.getClass().getName() + " is not " 
                + Comparable.class.getSimpleName() + "." );
    }
    
    
    private final Comparator< Object > m_decoratedComparator;
}
