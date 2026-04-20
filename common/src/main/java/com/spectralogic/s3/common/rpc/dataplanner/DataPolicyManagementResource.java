/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

/**
 * The {@link RpcResource} for the data planner to manage data policies.  Data policy management, bucket
 * creation, and other operations must be synchronized since there are validation constraints surrounding
 * the interaction between policy dao types that must be enforced in an atomically-correct manner.  <br><br>
 * 
 * Synchronizing on an instance of {@link DataPolicyManagementResource} shall lock the resource in a manner
 * such that the locking client may invoke methods on the resource without fear of interleaving of other
 * operations until the lock on the resource is released.
 */
@RpcResourceName( "DataPolicyManager" )
public interface DataPolicyManagementResource extends RpcResource, CreateBucketResource
{
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyBucket( final UUID bucketId, final UUID newDataPolicyId );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyDataPolicy( final DataPolicy dataPolicy, final String [] propertiesToUpdate );
    
    
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createDataPersistenceRule( final DataPersistenceRule rule );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyDataPersistenceRule(
            final DataPersistenceRule rule,
            final String [] propertiesToUpdate );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteDataPersistenceRule( final UUID ruleId );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > convertStorageDomainToDs3Target( final UUID storageDomainId, final UUID ds3TargetId );
    
    
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createDs3DataReplicationRule( final Ds3DataReplicationRule rule );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyDs3DataReplicationRule(
            final Ds3DataReplicationRule rule,
            final String [] propertiesToUpdate );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteDs3DataReplicationRule( final UUID ruleId );
    
    
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createAzureDataReplicationRule( final AzureDataReplicationRule rule );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyAzureDataReplicationRule(
            final AzureDataReplicationRule rule,
            final String [] propertiesToUpdate );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteAzureDataReplicationRule( final UUID ruleId );
    
    
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createS3DataReplicationRule( final S3DataReplicationRule rule );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyS3DataReplicationRule(
            final S3DataReplicationRule rule,
            final String [] propertiesToUpdate );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteS3DataReplicationRule( final UUID ruleId );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< UUID > modifyStorageDomain( 
            final StorageDomain storageDomain,
            final String [] propertiesToUpdate );
    

    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createStorageDomainMember( final StorageDomainMember member );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyStorageDomainMember(
            final StorageDomainMember storageDomainMember,
            final String [] propertiesToUpdate );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteStorageDomainMember( final UUID storageDomainMemberId );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyPool( final UUID poolId, @NullAllowed final UUID newPoolPartitionId );
}
