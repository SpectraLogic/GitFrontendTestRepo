/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen;

import java.util.Comparator;
import java.util.List;

final class StringListComparator implements Comparator< List< String > >
{
    private StringListComparator()
    {
        //empty
    }
    
    
    public static Comparator< List< String > > instance()
    {
        return INSTANCE;
    }


    public int compare( final List< String > listA, final List< String > listB )
    {
        final int maxSize = Math.min( listA.size(), listB.size() );
        for ( int i = 0; i < maxSize; ++i )
        {
            final int compareResult = listA.get( i ).compareTo( listB.get( i ) );
            if ( compareResult != 0 )
            {
                return compareResult;
            }
        }
        return Integer.compare( listA.size(), listB.size() );
    }
    
    
    private static final StringListComparator INSTANCE = new StringListComparator();
}
