/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.authorization.api;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.util.http.HttpRequest;

public interface AuthorizationValidationStrategy
{
    /**
     * @return non-null if authorization is present and valid, null if authorization is not present
     * (anonymous login), or throws an exception if authorization is present and invalid
     */
    public User getAuthorization( 
            final HttpRequest httpRequest,
            final String id,
            final String signature );
}
