/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;


/**
 * Service that provides {@link BucketAcl}-based authorization verification for {@link BucketAccessRequest}s.
 */
public interface BucketAclAuthorizationService
{
    public enum AdministratorOverride
    {
        /**
         * If ACLs do not permit the operation, the request will be denied regardless as to whether or not the
         * user is an administrator.
         */
        NO,
        
        /**
         * If ACLs do not permit the operation, but the user is an administrator, access will be granted.
         */
        YES
    } // end inner class def
    
    
    /**
     * @throws {@link FailureTypeObservableException} with code {@link GenericFailure#FORBIDDEN} if access
     * is not allowed.
     */
    public void verifyHasAccess(
            final BucketAccessRequest request, 
            final AdministratorOverride administratorOverride );
    
    
    public boolean hasAccess( 
            final BucketAccessRequest request, 
            final AdministratorOverride administratorOverride );
}
