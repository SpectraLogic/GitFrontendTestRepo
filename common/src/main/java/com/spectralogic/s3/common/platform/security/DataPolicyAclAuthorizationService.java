/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.util.exception.GenericFailure;

/**
 * Service that provides {@link DataPolicyAcl}-based authorization verification for 
 * {@link DataPolicyAccessRequest}s.
 */
public interface DataPolicyAclAuthorizationService
{
    /**
     * @throws {@link FailureTypeObservableException} with code {@link GenericFailure#FORBIDDEN} if access
     * is not allowed.
     */
    public void verifyHasAccess( final DataPolicyAccessRequest request );
    
    
    public boolean hasAccess( final DataPolicyAccessRequest request );
}
