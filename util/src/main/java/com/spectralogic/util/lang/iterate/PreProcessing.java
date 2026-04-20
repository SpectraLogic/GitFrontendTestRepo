/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;


/**
 * Supports having {@link PreProcessor} instances registered on it.
 */
public interface PreProcessing< T >
{
    void register( final PreProcessor< T > preProcessor );
}
