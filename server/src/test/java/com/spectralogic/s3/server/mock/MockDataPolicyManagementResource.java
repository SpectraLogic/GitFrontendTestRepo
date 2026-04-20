/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.service.ds3.AzureDataReplicationRuleService;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.DataPersistenceRuleService;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.dao.service.ds3.Ds3DataReplicationRuleService;
import com.spectralogic.s3.common.dao.service.ds3.S3DataReplicationRuleService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainMemberService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class MockDataPolicyManagementResource 
    extends BaseRpcResource implements DataPolicyManagementResource
{
    public MockDataPolicyManagementResource( final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        Validations.verifyNotNull( "Service manager", m_serviceManager );
    }


    public RpcFuture< UUID > createBucket( final Bucket bucket )
    {
        m_serviceManager.getService( BucketService.class ).create( bucket );
        return new RpcResponse<>( bucket.getId() );
    }


    public RpcFuture< ? > modifyBucket( final UUID bucketId, final UUID newDataPolicyId )
    {
        final Bucket bucket = BeanFactory.newBean( Bucket.class ).setDataPolicyId( newDataPolicyId );
        bucket.setId( bucketId );
        m_serviceManager.getService( BucketService.class ).update( bucket, Bucket.DATA_POLICY_ID );
        return new RpcResponse<>();
    }


    public RpcFuture< ? > modifyDataPolicy( final DataPolicy dataPolicy, final String[] propertiesToUpdate )
    {
        m_serviceManager.getService( DataPolicyService.class ).update( dataPolicy, propertiesToUpdate );
        return new RpcResponse<>();
    }


    public RpcFuture< UUID > createDataPersistenceRule( final DataPersistenceRule rule )
    {
        m_serviceManager.getService( DataPersistenceRuleService.class ).create( rule );
        return new RpcResponse<>( rule.getId() );
    }


    public RpcFuture< ? > modifyDataPersistenceRule(
            final DataPersistenceRule rule,
            final String [] propertiesToUpdate )
    {
        m_serviceManager.getService( DataPersistenceRuleService.class ).update(
                rule, propertiesToUpdate );
        return new RpcResponse<>();
    }


    public RpcFuture< ? > deleteDataPersistenceRule( final UUID ruleId )
    {
        m_serviceManager.getService( DataPersistenceRuleService.class ).delete( ruleId );
        return new RpcResponse<>();
    }


    public RpcFuture< UUID > createDs3DataReplicationRule( final Ds3DataReplicationRule rule )
    {
        m_serviceManager.getService( Ds3DataReplicationRuleService.class ).create( rule );
        return new RpcResponse<>( rule.getId() );
    }


    public RpcFuture< ? > modifyDs3DataReplicationRule(
            final Ds3DataReplicationRule rule,
            final String[] propertiesToUpdate )
    {
        m_serviceManager.getService( Ds3DataReplicationRuleService.class ).update(
                rule, propertiesToUpdate );
        return new RpcResponse<>();
    }


    public RpcFuture< ? > deleteDs3DataReplicationRule( final UUID ruleId )
    {
        m_serviceManager.getService( Ds3DataReplicationRuleService.class ).delete( ruleId );
        return new RpcResponse<>();
    }
    
    
    public RpcFuture< UUID > modifyStorageDomain( 
            final StorageDomain storageDomain,
            final String [] propertiesToUpdate )
    {
        m_serviceManager.getService( StorageDomainService.class ).update(
                storageDomain, propertiesToUpdate );
        return new RpcResponse<>();
    }


    public RpcFuture< UUID > createStorageDomainMember( final StorageDomainMember member )
    {
        m_serviceManager.getService( StorageDomainMemberService.class ).create( member );
        return new RpcResponse<>( member.getId() );
    }


    synchronized public RpcFuture< ? > modifyStorageDomainMember(
            final StorageDomainMember storageDomainMember, 
            final String [] propertiesToUpdate )
    {
        m_serviceManager.getService( StorageDomainMemberService.class ).update(
                storageDomainMember,
                propertiesToUpdate );
        return new RpcResponse<>();
    }


    public RpcFuture< ? > deleteStorageDomainMember( final UUID storageDomainMemberId )
    {
        m_serviceManager.getService( StorageDomainMemberService.class ).delete( storageDomainMemberId );
        return new RpcResponse<>();
    }


    public RpcFuture< ? > modifyPool( final UUID poolId, final UUID newPoolPartitionId )
    {
        final Pool pool = BeanFactory.newBean( Pool.class )
                .setPartitionId( newPoolPartitionId );
        pool.setId( poolId );
        m_serviceManager.getService( PoolService.class ).update(
                pool, Pool.PARTITION_ID );
        return new RpcResponse<>();
    }
    
    
    public RpcFuture< ? > convertStorageDomainToDs3Target( 
            final UUID storageDomainId,
            final UUID ds3TargetId )
    {
        return new RpcResponse<>( null );
    }


    public RpcFuture< UUID > createAzureDataReplicationRule( final AzureDataReplicationRule rule )
    {
        m_serviceManager.getService( AzureDataReplicationRuleService.class ).create( rule );
        return new RpcResponse<>( rule.getId() );
    }


    public RpcFuture< ? > modifyAzureDataReplicationRule(
            final AzureDataReplicationRule rule,
            final String [] propertiesToUpdate )
    {
        m_serviceManager.getService( AzureDataReplicationRuleService.class ).update(
                rule, propertiesToUpdate );
        return new RpcResponse<>();
    }


    public RpcFuture< ? > deleteAzureDataReplicationRule( final UUID ruleId )
    {
        m_serviceManager.getService( AzureDataReplicationRuleService.class ).delete( ruleId );
        return new RpcResponse<>();
    }


    public RpcFuture< UUID > createS3DataReplicationRule( final S3DataReplicationRule rule )
    {
        m_serviceManager.getService( S3DataReplicationRuleService.class ).create( rule );
        return new RpcResponse<>( rule.getId() );
    }


    public RpcFuture< ? > modifyS3DataReplicationRule(
            final S3DataReplicationRule rule,
            final String [] propertiesToUpdate )
    {
        m_serviceManager.getService( S3DataReplicationRuleService.class ).update(
                rule, propertiesToUpdate );
        return new RpcResponse<>();
    }


    public RpcFuture< ? > deleteS3DataReplicationRule( final UUID ruleId )
    {
        m_serviceManager.getService( S3DataReplicationRuleService.class ).delete( ruleId );
        return new RpcResponse<>();
    }
    
    
    private final BeansServiceManager m_serviceManager;
}
