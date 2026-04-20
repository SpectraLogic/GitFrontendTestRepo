/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.predicate;

public final class TruePredicate< E > implements UnaryPredicate< E >
{
    public boolean test( final E element )
    {
        return true;
    }
}
