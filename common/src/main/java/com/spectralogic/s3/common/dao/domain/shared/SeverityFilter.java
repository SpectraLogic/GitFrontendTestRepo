/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.HashSet;
import java.util.Set;

import com.spectralogic.util.lang.CollectionFactory;

public final class SeverityFilter
{
    private SeverityFilter()
    {
        // singleton
    }
    

    public static < T extends Enum< ? > & SeverityObservable > Set< T > by(
            final Severity severity, 
            final Class< T > clazz )
    {
        return by( severity, CollectionFactory.toSet( clazz.getEnumConstants() ) );
    }
    
    
    public static < T extends SeverityObservable > Set< T > by(
            final Severity severity, 
            final Set< T > severityObservables )
    {
        final Set< T > retval = new HashSet<>();
        for ( final T o : severityObservables )
        {
            if ( o.getSeverity() == severity )
            {
                retval.add( o );
            }
        }
        
        return retval;
    }
}
