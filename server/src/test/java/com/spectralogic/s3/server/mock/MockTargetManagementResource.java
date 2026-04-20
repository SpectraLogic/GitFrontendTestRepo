/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ImportAzureTargetDirective;
import com.spectralogic.s3.common.dao.domain.target.ImportS3TargetDirective;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.common.dao.service.target.AzureTargetService;
import com.spectralogic.s3.common.dao.service.target.Ds3TargetService;
import com.spectralogic.s3.common.dao.service.target.S3TargetService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreatePutJobParams;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.s3.common.rpc.dataplanner.domain.Ds3TargetDataPolicies;
import com.spectralogic.s3.common.rpc.dataplanner.domain.S3ObjectToCreate;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class MockTargetManagementResource
    extends BaseRpcResource implements TargetManagementResource
{
    //NOTE: generally our mocked resources do not know about
    //each other, but here we pass a data policy mgmt resource
    //(presumably a mocked one) into our target mgmt resource.
    //We do this in order to facilitate meaningful create bucket
    //unit tests where the bucket actually gets created instead
    //of merely testing the return code. - Kyle Hughart 05/25/17
    public MockTargetManagementResource(
            final BeansServiceManager serviceManager,
            final DataPolicyManagementResource dataPolicyResource )
    {
        m_serviceManager = serviceManager;
        m_dataPolicyManagementResource = dataPolicyResource;
        Validations.verifyNotNull( "Service manager", m_serviceManager );
    }
    
    
    public RpcResponse< UUID > registerDs3Target( final Ds3Target target )
    {
        m_serviceManager.getService( Ds3TargetService.class ).create( target );
        return new RpcResponse<>( target.getId() );
    }


    public RpcResponse< ? > modifyDs3Target( final Ds3Target target, final String [] propertiesToUpdate )
    {
        m_serviceManager.getService( Ds3TargetService.class ).update( target, propertiesToUpdate );
        return new RpcResponse<>( null );
    }   
    
    
    public RpcFuture< ? > pairBack( final UUID targetId, final Ds3Target pairBackTarget )
    {
        return new RpcResponse<>( null );
    }


    public RpcFuture< ? > verifyDs3Target( final UUID targetId, final boolean fullyVerify )
    {
        return new RpcResponse<>( null );
    }


    public RpcResponse< Ds3TargetDataPolicies > getDataPolicies( final UUID ds3TargetId )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public RpcResponse< ? > createUser( final boolean force, final User user )
    {
        m_serviceManager.getService( UserService.class ).create( user );
        return new RpcResponse<>( null );
    }


    public RpcResponse< ? > modifyUser( 
            final boolean force, 
            final User user,
            final String[] propertiesToUpdate )
    {
        m_serviceManager.getService( UserService.class ).update( user, propertiesToUpdate );
        return new RpcResponse<>( null );
    }


    public RpcResponse< ? > deleteUser( final boolean force, final UUID userId )
    {
        m_serviceManager.getService( UserService.class ).delete( userId );
        return new RpcResponse<>( null );
    }


    public RpcFuture< DeleteObjectsResult > deleteObjects(
            final UUID userId,
            final PreviousVersions previousVersions,
            final UUID[] objectIds )
    {
        return new RpcResponse<>( null );
    }
    
    
    public RpcFuture< ? > deleteBucket( final UUID userId, final UUID bucketId, final boolean deleteObjects )
    {
        return new RpcResponse<>( null );
    }


    public RpcFuture< UUID > createPutJob( final CreatePutJobParams params )
    {
        return new RpcResponse<>( fakeCreatePutJob( params ) );
    }
    
    
    private UUID fakeCreatePutJob( final CreatePutJobParams params )
    {
        final Job job = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT )
                .setBucketId( params.getBucketId() )
                .setUserId( params.getUserId() )
                .setPriority( params.getPriority() )
                .setMinimizeSpanningAcrossMedia( params.isMinimizeSpanningAcrossMedia() )
                .setAggregating( params.isAggregating() )
                .setNaked( params.isNaked() )
                .setImplicitJobIdResolution( params.isImplicitJobIdResolution() )
                .setName( params.getName() )
                .setProtected( params.isProtected() )
                .setDeadJobCleanupAllowed( params.isDeadJobCleanupAllowed() );
        m_serviceManager.getService( JobService.class ).create( job );
        boolean createdJob = false;
        
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            for ( final S3ObjectToCreate otc : params.getObjectsToCreate() )
            {
                createdJob = true;
                
                final S3Object o = BeanFactory.newBean( S3Object.class );
                o.setBucketId( params.getBucketId() );
                o.setName( otc.getName() );
                transaction.getService( S3ObjectService.class ).create( 
                        CollectionFactory.toSet( o ) );
                
                final Blob blob = BeanFactory.newBean( Blob.class );
                blob.setObjectId( o.getId() );
                blob.setLength( otc.getSizeInBytes() );
                transaction.getService( BlobService.class ).create( CollectionFactory.toSet( blob ) );

                final int chunkNumber = (int)transaction.getRetriever( JobEntry.class ).getMax(
                        JobEntry.CHUNK_NUMBER,
                        Require.nothing() ) + 1;
                final JobEntry chunk = BeanFactory.newBean( JobEntry.class )
                        .setChunkNumber( chunkNumber ).setJobId( job.getId() ).setBlobId(blob.getId());
                transaction.getService( JobEntryService.class ).create( chunk );
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        if ( createdJob )
        {
            return job.getId();
        }
        return null;
    }
    
    
    public RpcFuture< ? > cancelJob( final UUID userId, final UUID jobId, final boolean force )
    {
        return new RpcResponse<>( null );
    }


    @Override
    public RpcFuture<?> cancelJobQuietly(UUID userId, UUID jobId, boolean force) {
        return new RpcResponse<>( null );
    }


    public RpcFuture< UUID > registerAzureTarget( final AzureTarget target )
    {
        m_serviceManager.getService( AzureTargetService.class ).create( target );
        return new RpcResponse<>( target.getId() );
    }


    public RpcFuture< ? > modifyAzureTarget( final AzureTarget target, final String[] propertiesToUpdate )
    {
        m_serviceManager.getService( AzureTargetService.class ).update( target, propertiesToUpdate );
        return new RpcResponse<>();
    }
    
    
    public void verifyPublicCloudTarget( 
            final Class< ? extends PublicCloudReplicationTarget< ? > > targetType,
            final UUID targetId, 
            final boolean fullyVerify )
    {
        throw new UnsupportedOperationException( "No code to support method invocation." );
    }


    public RpcFuture< ? > verifyAzureTarget( final UUID targetId, final boolean fullyVerify )
    {
        return new RpcResponse<>();
    }


    public RpcFuture< UUID > registerS3Target( final S3Target target )
    {
        m_serviceManager.getService( S3TargetService.class ).create( target );
        return new RpcResponse<>( target.getId() );
    }


    public RpcFuture< ? > modifyS3Target( final S3Target target, final String[] propertiesToUpdate )
    {
        m_serviceManager.getService( S3TargetService.class ).update( target, propertiesToUpdate );
        return new RpcResponse<>();
    }


    public RpcFuture< ? > verifyS3Target( final UUID targetId, final boolean fullyVerify )
    {
        return new RpcResponse<>();
    }


    public RpcFuture< ? > importAzureTarget( final ImportAzureTargetDirective directive )
    {
        return new RpcResponse<>();
    }


    public RpcFuture< ? > importS3Target( final ImportS3TargetDirective directive )
    {
        return new RpcResponse<>();
    }
    
    
    public RpcFuture<UUID> createBucket(Bucket bucket)
    {
        return m_dataPolicyManagementResource.createBucket( bucket );
    }
    
    
    @Override
    public RpcFuture< ? > quiesceAndPrepareForShutdown(boolean force)
    {
        return new RpcResponse<>();
    }


    @Override
    public RpcFuture< ? > cleanUpCompletedJobsAndJobChunks()
    {
        return new RpcResponse<>();
    }


    @Override
    public RpcFuture<?> modifyJob(UUID jobId, BlobStoreTaskPriority priority) {
        return new RpcResponse<>();
    }


    public RpcFuture< UUID > createDs3DataReplicationRule( final Ds3DataReplicationRule rule )
    {
    	return m_dataPolicyManagementResource.createDs3DataReplicationRule( rule );
    }


    public RpcFuture< UUID > createAzureDataReplicationRule( final AzureDataReplicationRule rule )
    {
    	return m_dataPolicyManagementResource.createAzureDataReplicationRule( rule );
    }


    public RpcFuture< UUID > createS3DataReplicationRule( final S3DataReplicationRule rule )
    {
    	return m_dataPolicyManagementResource.createS3DataReplicationRule( rule );
    }
    
    
	public RpcFuture< ? > undeleteObject(UUID userId, S3Object object)
	{
		return new RpcResponse<>();
	}
	
	
	private final BeansServiceManager m_serviceManager;
    private final DataPolicyManagementResource m_dataPolicyManagementResource;
}
