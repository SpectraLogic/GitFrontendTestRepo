/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ImportAzureTargetDirective;
import com.spectralogic.s3.common.dao.domain.target.ImportS3TargetDirective;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.rpc.dataplanner.domain.Ds3TargetDataPolicies;
import com.spectralogic.util.net.rpc.frmwrk.QuiescableRpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

/**
 * The {@link RpcResource} for the data planner to manage replication targets.
 */
@RpcResourceName( "TargetManager" )
public interface TargetManagementResource
    extends RpcResource, QuiescableRpcResource, DeleteObjectsResource, JobResource, CreateBucketResource
{
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > registerDs3Target( final Ds3Target target );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyDs3Target( final Ds3Target target, final String [] propertiesToUpdate );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > pairBack( final UUID targetId, final Ds3Target pairBackTarget );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > verifyDs3Target( final UUID targetId, final boolean fullyVerify );
    
    
    @RpcMethodReturnType( Ds3TargetDataPolicies.class )
    RpcFuture< Ds3TargetDataPolicies > getDataPolicies( final UUID targetId );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > createUser( final boolean force, final User user );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyUser( final boolean force, final User user, final String [] propertiesToUpdate );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteUser( final boolean force, final UUID userId );


    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > registerAzureTarget( final AzureTarget target );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyAzureTarget( final AzureTarget target, final String [] propertiesToUpdate );
    
    
    void verifyPublicCloudTarget( 
            final Class< ? extends PublicCloudReplicationTarget< ? > > targetType,
            final UUID targetId, 
            final boolean fullyVerify );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > verifyAzureTarget( final UUID targetId, final boolean fullyVerify );
    
    
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > registerS3Target( final S3Target target );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyS3Target( final S3Target target, final String [] propertiesToUpdate );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > verifyS3Target( final UUID targetId, final boolean fullyVerify );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > importAzureTarget( final ImportAzureTargetDirective importDirective );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > importS3Target( final ImportS3TargetDirective importDirective );

    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createAzureDataReplicationRule( final AzureDataReplicationRule rule );

	@RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createS3DataReplicationRule( final S3DataReplicationRule rule );
}
