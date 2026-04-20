/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.auth;


/**
 * Authorization strategy that ensures that the operation is never allowed on a system bucket, unless it is 
 * as an internal request.  For non system buckets, the only security check performed is to ensure that the
 * request is not being made anonymously.
 */
public final class SystemBucketProtectedAuthorizationStrategy extends BucketAuthorizationStrategy
{
    public SystemBucketProtectedAuthorizationStrategy()
    {
        super();
    }
}
