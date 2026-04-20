/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io.lang;

import java.io.InputStream;

public interface InputStreamProvider
{
    /**
     * @return the next {@link InputStream}, or null if there isn't a next {@link InputStream}
     */
    InputStream getNextInputStream();
}
