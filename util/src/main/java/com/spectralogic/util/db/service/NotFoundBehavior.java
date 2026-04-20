/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

/**
 * Behavior to perform when the requested bean could not be found.
 */
enum NotFoundBehavior
{
    /**
     * Throws an exception if the requested bean could not be found
     */
    THROW_EXCEPTION,
    
    /**
     * Returns null if the requested bean could not be found
     */
    RETURN_NULL
}
