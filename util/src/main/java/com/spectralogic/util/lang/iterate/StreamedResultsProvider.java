/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;


public interface StreamedResultsProvider< T > extends AutoCloseable
{
    /**
     * @return null if there are no more results, or non-null for the next result
     */
    T getNextResult();
    
    
    void close();
}
