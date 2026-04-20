/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;

import java.util.Iterator;

/**
 * An {@link Iterator} that is also supports {@link PreProcessing}.
 */
public interface EnhancedIterator< T > extends Iterator< T >, PreProcessing< T >, AutoCloseable
{
    void close();
}
