/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.comparator;

import java.util.Comparator;

public final class ClassComparator implements Comparator< Class< ? > >
{
    public int compare( final Class< ? > o1, final Class< ? > o2 )
    {
        return o1.getName().compareTo( o2.getName() );
    }
}
