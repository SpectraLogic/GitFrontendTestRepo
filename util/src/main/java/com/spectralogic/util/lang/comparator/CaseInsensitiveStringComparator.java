/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.comparator;

import java.util.Comparator;

public final class CaseInsensitiveStringComparator implements Comparator< Object >
{
    public int compare( final Object arg0, final Object arg1 )
    {
        final String s0 = ( (String)arg0 ).toLowerCase();
        final String s1 = ( (String)arg1 ).toLowerCase();
        return s0.compareTo( s1 );
    }
}
