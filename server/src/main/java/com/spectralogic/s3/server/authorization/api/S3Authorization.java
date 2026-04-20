/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.authorization.api;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.User;

public interface S3Authorization
{
    /**
     * @return the id part of the authorization header, or null if the authorization header was not provided
     */
    public String getId();
    
    
    /**
     * @return the user the request is authorized under, or null if there is not a user the request is
     * authorized under
     */
    public User getUser();
    
    
    /**
     * @return the user's id the request is authorized under, or null if there is not a user the request
     * is authorized under
     */
    public UUID getUserId();
    
    
    /**
     * Throws an exception if an id and signature are provided, but there is an issue validating the
     * id and signature using the authorizationValidationStrategy
     * 
     * Also populates the owner with an actual owner.
     */
    public void validate( final AuthorizationValidationStrategy authorizationValidationStrategy );
}
