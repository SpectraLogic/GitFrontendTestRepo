/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.predicate;


/**
 * An interface that defines a method for a unary predicate.
 * 
 * @param <E> The type of element that will be tested.
 */
public interface UnaryPredicate< E >
{
    /**
     * Returns a boolean for an element. This interface is intended to be used
     * with algorithms such as find_if or remove_if.
     * 
     * @param element to test
     * @return Boolean result of the test
     */
    boolean test( E element );
}
