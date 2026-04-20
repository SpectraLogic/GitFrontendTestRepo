/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import java.util.List;

import com.spectralogic.s3.server.request.api.DS3Request;

/**
 * Determines whether or not a request can be handled.
 */
public interface CanHandleRequestDeterminer
{
    /**
     * @return <b>null</b>, if the request can be handled, or <br><br>
     *         
     *         <b>a zero-length {@link String}</b>, if the request cannot be handled and wasn't close to being
     *         able to be handled, or <br><br>
     *         
     *         <b>a non-zero-length {@link String}</b>, if the request cannot be handled and was close to 
     *         being able to be handled, in which case, the {@link String} returned is the reason or reasons
     *         why the request could not be handled
     */
    String getFailureToHandle( final DS3Request request );
    
    
    List< String > getHandlingRequirements();
    
    
    String getSampleRequest();
    
    
    RequestHandlerRequestContract getRequestContract();
    
    
    QueryStringRequirement getQueryStringRequirement();
    
    
    String SAMPLE_REQUEST_PREFIX = "http[s]://datapathdnsnameofappliance/";
}
