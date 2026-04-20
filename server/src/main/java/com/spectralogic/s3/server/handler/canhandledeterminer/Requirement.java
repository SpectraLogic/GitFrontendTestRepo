/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import com.spectralogic.s3.server.request.api.DS3Request;

/**
 * A requirement that must be met of the S3Request for this handler to be able to handle the request.
 */
interface Requirement
{
    /**
     * @return TRUE if the request meets this requirement to allow a handler to handle it
     */
    boolean meetsRequirement( final DS3Request request );
    
    
    String getRequirementDescription();
}