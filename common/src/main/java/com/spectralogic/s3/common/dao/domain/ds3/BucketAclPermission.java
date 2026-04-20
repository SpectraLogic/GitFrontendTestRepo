/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;


/**
 * A list of the possible permissions that can be granted on a bucket.
 */
public enum BucketAclPermission
{
    /**
     * User may list objects.
     */
    LIST,
    
    
    /**
     * User may read objects (and may create GET jobs).
     */
    READ,
    
    
    /**
     * User may write objects (and may create PUT jobs).
     */
    WRITE,
    
    
    /**
     * User may delete objects, or the entire bucket.
     */
    DELETE,
    
    
    /**
     * User can modify, view, and cancel jobs that were started by another user.
     */
    JOB,
    
    
    /**
     * User has permission to do anything, including all other permissions in this enum, reading and modifying
     * ACLs, and everything else.
     */
    OWNER
    ;
}
