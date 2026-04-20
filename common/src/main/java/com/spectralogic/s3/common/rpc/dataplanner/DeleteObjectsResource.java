/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;

public interface DeleteObjectsResource
{
    @RpcMethodReturnType( DeleteObjectsResult.class )
    RpcFuture< DeleteObjectsResult > deleteObjects( 
            @NullAllowed final UUID userId,
            final PreviousVersions previousVersions, 
            final UUID [] objectIds );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteBucket( 
            @NullAllowed final UUID userId,
            final UUID bucketId,
            final boolean deleteObjects );
            
    
    @RpcMethodReturnType( void.class )        
    RpcFuture< ? > undeleteObject( final UUID userId, final S3Object object );
}
