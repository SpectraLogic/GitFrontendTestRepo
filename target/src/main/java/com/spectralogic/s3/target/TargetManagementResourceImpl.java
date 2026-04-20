/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.target.*;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.orm.AzureDataReplicationRuleRM;
import com.spectralogic.s3.common.dao.orm.S3DataReplicationRuleRM;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.common.dao.service.temp.BlobAzureTargetToVerifyService;
import com.spectralogic.s3.common.dao.service.temp.BlobS3TargetToVerifyService;
import com.spectralogic.s3.common.platform.replicationtarget.TargetInitializationUtil;
import com.spectralogic.s3.common.platform.spectrads3.BaseQuiescableJobResource;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.platform.target.PublicCloudBucketSupport;
import com.spectralogic.s3.common.platform.target.PublicCloudBucketSupportImpl;
import com.spectralogic.s3.common.rpc.dataplanner.CancelJobFailedException;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreatePutJobParams;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.s3.common.rpc.dataplanner.domain.Ds3TargetDataPolicies;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnectionFactory;
import com.spectralogic.s3.common.rpc.target.PublicCloudTargetImportScheduler;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.target.verify.PublicCloudTargetVerifier;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservable;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;

public final class TargetManagementResourceImpl
    extends BaseQuiescableJobResource implements TargetManagementResource
{
    public TargetManagementResourceImpl(
            final RpcServer rpcServer, 
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final AzureConnectionFactory azureConnectionFactory,
            final S3ConnectionFactory s3ConnectionFactory,
            final DataPlannerResource dataPlannerResource,
            final DataPolicyManagementResource dataPolicyManagementResource,
            final PublicCloudTargetImportScheduler< ImportAzureTargetDirective > azureTargetImporter,
            final PublicCloudTargetImportScheduler< ImportS3TargetDirective > s3TargetImporter,
            final BeansServiceManager serviceManager )
    {
        super( serviceManager );
        m_ds3ConnectionFactory = ds3ConnectionFactory;
        m_azureConnectionFactory = azureConnectionFactory;
        m_s3ConnectionFactory = s3ConnectionFactory;
        m_dataPlannerResource = dataPlannerResource;
        m_dataPolicyManagementResource = dataPolicyManagementResource;
        m_azureTargetImporter = azureTargetImporter;
        m_s3TargetImporter = s3TargetImporter;
        Validations.verifyNotNull( "DS3 connection factory", m_ds3ConnectionFactory );
        Validations.verifyNotNull( "Azure connection factory", m_azureConnectionFactory );
        Validations.verifyNotNull( "S3 connection factory", m_s3ConnectionFactory );
        Validations.verifyNotNull( "Data planner resource", m_dataPlannerResource );
        Validations.verifyNotNull( "Azure target importer", m_azureTargetImporter );
        Validations.verifyNotNull( "S3 target importer", m_s3TargetImporter );
        
        

        rpcServer.register( null, this );
    }
    
    
    synchronized public RpcResponse< UUID > registerDs3Target( final Ds3Target target )
    {
        if ( null != m_serviceManager.getRetriever( Ds3Target.class ).retrieve(
                NameObservable.NAME, target.getName() ) )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONFLICT,
                    "Already registered another DS3 instance with name " + target.getName() + "." );
        }

        target.setId( null );
        final Ds3Connection connection = m_ds3ConnectionFactory.discover( target );
        try
        {
            final Ds3Target existing = 
                    m_serviceManager.getRetriever( Ds3Target.class ).retrieve( target.getId() );
            if ( null != existing )
            {
                throw new FailureTypeObservableException( 
                        GenericFailure.CONFLICT,
                        "Already registered DS3 instance " + target.getId()
                        + " as " + existing.getName() + "." );
            }
            final DataPathBackend dpb = 
                    m_serviceManager.getRetriever( DataPathBackend.class ).attain( Require.nothing() );
            if ( dpb.getInstanceId().equals( target.getId() ) )
            {
                throw new FailureTypeObservableException( 
                        GenericFailure.CONFLICT,
                        "Target has the same DS3 instance ID as source." );
            }
            switch ( target.getAccessControlReplication() )
            {
                case NONE:
                    break;
                case USERS:
                    replicateUsers( target, connection );
                    break;
                default:
                    throw new UnsupportedOperationException( 
                            "No code to support " + target.getAccessControlReplication() + "." );
            }
            
            m_serviceManager.getService( Ds3TargetService.class ).create( target );
            return new RpcResponse<>( target.getId() );
        }
        finally
        {
            connection.shutdown();
        }
    }
    
    
    private void replicateUsers( final Ds3Target target, Ds3Connection connection )
    {
        try
        {
            final Map< String, User > targetUsers = new HashMap<>();
            for ( final User user : connection.getUsers() )
            {
                targetUsers.put( user.getName(), user );
            }
            
            for ( final User user : m_serviceManager.getRetriever( User.class ).retrieveAll().toSet() )
            {
                if ( !targetUsers.containsKey( user.getName() ) )
                {
                    connection.createUser( user, target.getReplicatedUserDefaultDataPolicy() );
                }
                else if ( !targetUsers.get( user.getName() ).getSecretKey().equals( user.getSecretKey() ) )
                {
                    connection.updateUser( 
                            targetUsers.get( user.getName() ).setSecretKey( user.getSecretKey() ) );
                    if ( updateAdminSecretKey( target, user.getAuthId(), user.getSecretKey() ) )
                    {
                        connection.shutdown();
                        connection = m_ds3ConnectionFactory.connect( null, target );
                    }
                }
            }
        }
        finally
        {
            connection.shutdown();
        }
    }

    
    synchronized public RpcResponse< ? > modifyDs3Target( 
            final Ds3Target target, 
            final String [] propertiesToUpdate )
     {
        final boolean onlyPropsModifyableOffline = 
                ( propertiesToUpdate != null )
                && TARGET_PROPS_MODIFYABLE_OFFLINE.containsAll( 
                        CollectionFactory.toSet( propertiesToUpdate ) );
        if ( !onlyPropsModifyableOffline )
        {
            m_ds3ConnectionFactory.discover( target ).shutdown();        
            
            if ( CollectionFactory.toSet( propertiesToUpdate ).contains( 
                    Ds3Target.ACCESS_CONTROL_REPLICATION ) )
            {
                if ( Ds3TargetAccessControlReplication.USERS == target.getAccessControlReplication() )
                {
                    final Ds3Connection connection = m_ds3ConnectionFactory.connect( null, target );
                    try
                    {
                        replicateUsers( target, connection );
                    }
                    finally
                    {
                        connection.shutdown();
                    }
                }
            }
        }
        
        m_serviceManager.getService( Ds3TargetService.class ).update( target, propertiesToUpdate );
        
        return null;
    }
    

    public RpcFuture< ? > pairBack( final UUID targetId, final Ds3Target pairBackTarget )
    {
        final Ds3Target target = m_serviceManager.getRetriever( Ds3Target.class ).attain( targetId );
        final Ds3Connection connection = m_ds3ConnectionFactory.connect( null, target );
        try
        {
            connection.createDs3Target( pairBackTarget );
        }
        finally
        {
            connection.shutdown();
        }
        return null;
    }


    private WorkPool getVerifierWorkpool() {
        // TODO: this should never be shut down in production, but our we have problems with teh AspectJ
        // ThreadLeakHunter code in test shutting our workpool down on us and causing errantly failed tests.
        // We should ultimately fix the AspectJ code to not make sure it's not tearing down test N while test
        // N + 1 is already running. Once we do that we can make m_targetVerifierWorkpool final and remove this function
        if (m_targetVerifierWorkpool == null || m_targetVerifierWorkpool.isShutdown()) {
            m_targetVerifierWorkpool = WorkPoolFactory.createWorkPool( 5, "TargetVerifier" );
        }
        return m_targetVerifierWorkpool;
    }
    
    public RpcFuture< ? > verifyDs3Target( final UUID targetId, final boolean fullyVerify )
    {
        final Ds3Target target = m_serviceManager.getRetriever( Ds3Target.class ).attain( targetId );
        m_ds3ConnectionFactory.connect( null, target ).shutdown();
        if ( !fullyVerify )
        {
            return null;
        }

        getVerifierWorkpool().submit( () -> {
            int count = 0;
            int missingCount = 0;
            try ( final EnhancedIterable< BlobDs3Target > iterable =
                          m_serviceManager.getRetriever( BlobDs3Target.class ).retrieveAll(
                                  BlobTarget.TARGET_ID, targetId ).toIterable() )
            {
                final Set< BlobDs3Target > bts = new HashSet<>();
                for ( final BlobDs3Target bt : iterable )
                {
                    ++count;
                    bts.add( bt );
                    if ( 10000 == bts.size() )
                    {
                        missingCount += verifyDs3TargetSegment( target, bts );
                    }
                }
                missingCount += verifyDs3TargetSegment( target, bts );
            }

            final String msg =
                    "Completed verification of " + count + " " + BlobDs3Target.class.getSimpleName()
                            + "s (" + missingCount + " were missing).";
            if ( 0 < missingCount )
            {
                LOG.warn( msg );
            }

            LOG.info( msg );
            m_serviceManager.getService( Ds3TargetFailureService.class ).create( targetId, TargetFailureType.VERIFY_COMPLETE, "Verify Complete", null );
        });
        return null;
    }
    
    
    private int verifyDs3TargetSegment( final Ds3Target target, final Set< BlobDs3Target > bts )
    {
        if ( bts.isEmpty() )
        {
            return 0;
        }
        
        final Map< UUID, BlobDs3Target > missingBlobTargets = new HashMap<>();
        for ( final BlobDs3Target bt : bts )
        {
            missingBlobTargets.put( bt.getBlobId(), bt );
        }
        
        final Ds3Connection connection = m_ds3ConnectionFactory.connect( null, target );
        try
        {
            final BlobPersistenceContainer response = 
                    connection.getBlobPersistence( UUID.randomUUID(), missingBlobTargets.keySet() );
            for ( final BlobPersistence bp : response.getBlobs() )
            {
                if ( null == bp.getChecksum() || null == bp.getChecksumType() )
                {
                    continue;
                }
                
                missingBlobTargets.remove( bp.getId() );
            }
        }
        finally
        {
            connection.shutdown();
            bts.clear();
        }
        
        if ( missingBlobTargets.isEmpty() )
        {
            return 0;
        }
        
        final Set< SuspectBlobDs3Target > suspectBlobTargets = new HashSet<>();
        for ( final BlobDs3Target bt : missingBlobTargets.values() )
        {
            final SuspectBlobDs3Target suspectBlobTarget = BeanFactory.newBean( SuspectBlobDs3Target.class );
            BeanCopier.copy( suspectBlobTarget, bt );
            suspectBlobTargets.add( suspectBlobTarget );
        }
        
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getService( SuspectBlobDs3TargetService.class ).create( suspectBlobTargets );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        return missingBlobTargets.size();
    }
    
    
    public RpcResponse< Ds3TargetDataPolicies > getDataPolicies( final UUID targetId )
    {
        final Ds3Connection connection = m_ds3ConnectionFactory.connect(
                null, m_serviceManager.getRetriever( Ds3Target.class ).attain( targetId ) );
        try
        {
            final Ds3TargetDataPolicies retval = BeanFactory.newBean( Ds3TargetDataPolicies.class );
            retval.setDataPolicies(
                    CollectionFactory.toArray( DataPolicy.class, connection.getDataPolicies() ) );
            return new RpcResponse<>( retval );
        }
        finally
        {
            connection.shutdown();
        }
    }


    synchronized public RpcResponse< ? > createUser( final boolean force, final User user )
    {
        new Ds3ReplicatedOperation(
                force, null, getUserReplicatingDs3Targets(), "create user " + user.getName() )
        {
            @Override
            protected void commit()
            {
                m_serviceManager.getService( UserService.class ).create( user );
            }

            @Override
            protected void commit( final Ds3Target target, Ds3Connection connection )
            {
                final Map< String, User > targetUsers = new HashMap<>();
                for ( final User user : connection.getUsers() )
                {
                    targetUsers.put( user.getName(), user );
                }

                if ( !targetUsers.containsKey( user.getName() ) )
                {
                    connection.createUser( user, target.getReplicatedUserDefaultDataPolicy() );
                }
                else if ( !targetUsers.get( user.getName() ).getSecretKey().equals( user.getSecretKey() ) )
                {
                    connection.updateUser(
                            targetUsers.get( user.getName() ).setSecretKey( user.getSecretKey() ) );
                    if ( updateAdminSecretKey( target, user.getAuthId(), user.getSecretKey() ) )
                    {
                        connection.shutdown();
                        connection = m_ds3ConnectionFactory.connect( null, target );
                    }
                }
            }
        };
        
        return null;
    }


    synchronized public RpcResponse< ? > modifyUser( 
            final boolean force, 
            final User user,
            final String[] propertiesToUpdate )
    {
        new Ds3ReplicatedOperation( 
                force, null, getUserReplicatingDs3Targets(), "modify user " + user.getName() )
        {
            @Override
            protected void commit()
            {
                m_serviceManager.getService( UserService.class ).update( user, propertiesToUpdate );
            }

            @Override
            protected void commit( final Ds3Target target, final Ds3Connection connection )
            {
                connection.updateUser( user );
                updateAdminSecretKey( target, user.getAuthId(), user.getSecretKey() );
            }

            @Override
            protected void willNotCommit( final Ds3Target target, final RuntimeException ex )
            {
                if ( !FailureTypeObservable.class.isAssignableFrom( ex.getClass() ) )
                {
                    return;
                }
                if ( 403 == ( (FailureTypeObservable)ex ).getFailureType().getHttpResponseCode() )
                {
                    updateAdminSecretKey( target, user.getAuthId(), user.getSecretKey() );
                    try
                    {
                        m_ds3ConnectionFactory.connect( null, target );
                    }
                    catch ( final RuntimeException e )
                    {
                        LOG.warn( "Failed to connect to target " + target.getId()
                                  + " once admin secret key updated.", e );
                    }
                }
            }
        };
        
        return null;
    }


    synchronized public RpcResponse< ? > deleteUser( final boolean force, final UUID userId )
    {
        final User user = m_serviceManager.getRetriever( User.class ).attain( userId );
        new Ds3ReplicatedOperation( 
                force, null, getUserReplicatingDs3Targets(), "delete user " + user.getName() )
        {
            @Override
            protected void commit()
            {
                m_serviceManager.getService( UserService.class ).delete( userId );
            }

            @Override
            protected void commit( final Ds3Target target, final Ds3Connection connection )
            {
                connection.deleteUser( user.getName() );
            }
        };
        return null;
    }
    
    
    private boolean updateAdminSecretKey( 
            final Ds3Target target,
            final String authId, 
            final String secretKey )
    {
        if ( target.getAdminAuthId().equals( authId ) && !target.getAdminSecretKey().equals( secretKey ) )
        {
            LOG.info( "Target " + target.getName() + "'s " + Ds3Target.ADMIN_SECRET_KEY 
                      + " will be updated since the secret key is being modified on the target." );
            m_serviceManager.getService( Ds3TargetService.class ).update(
                    target.setAdminSecretKey( secretKey ),
                    Ds3Target.ADMIN_SECRET_KEY );
            return true;
        }
        return false;
    }
    
    
    public RpcFuture< DeleteObjectsResult > deleteObjects(
            final UUID userId,
            final PreviousVersions previousVersions, 
            final UUID [] arrObjectIds )
    {
        if ( 0 == arrObjectIds.length )
        {
            return new RpcResponse<>( BeanFactory.newBean( DeleteObjectsResult.class ) );
        }
        
        final DeleteObjectsResult [] retval = new DeleteObjectsResult[ 1 ];
        final Set<UUID> objectIds = CollectionFactory.toSet( arrObjectIds );
        final Set< S3Object > objects =
        		m_serviceManager.getRetriever( S3Object.class ).retrieveAll( objectIds ).toSet();
        final Bucket bucket = m_serviceManager.getRetriever( Bucket.class ).attain( Require.exists( 
                S3Object.class, 
                S3Object.BUCKET_ID,
                Require.beanPropertyEqualsOneOf( Identifiable.ID, objectIds ) ) );
        new Ds3ReplicatedOperation( userId,
                                 getDs3TargetsToReplciateDeleteToForDataPolicy( bucket.getDataPolicyId() ),
                                 "delete " + objectIds.size() + " objects in bucket " 
                                 + bucket.getId() + " (" + bucket.getName() + ")" )
        {
            @Override
            protected void commit()
            {
                retval[ 0 ] = m_dataPlannerResource.deleteObjects( 
                        userId,
                        previousVersions,
                        arrObjectIds ).getWithoutBlocking();
            }

            @Override
            protected void commit( final Ds3Target target, final Ds3Connection connection )
            {
                if ( retval[ 0 ].isDaoModified() )
                {
                    connection.deleteObjects( previousVersions, bucket, objects );
                }
            }
        };
        return new RpcResponse<>( retval[ 0 ] );
    }
    
    
    public RpcFuture< ? > deleteBucket( final UUID userId, final UUID bucketId, final boolean deleteObjects )
    {
        final Bucket bucket = m_serviceManager.getRetriever( Bucket.class ).attain( bucketId );        
        deletePublicCloudBucket(
                S3Target.class,
                S3DataReplicationRule.class,
                m_s3ConnectionFactory,
                S3TargetBucketNameService.class,
                bucket );
        deletePublicCloudBucket(
                AzureTarget.class,
                AzureDataReplicationRule.class,
                m_azureConnectionFactory,
                AzureTargetBucketNameService.class,
                bucket );
        new Ds3ReplicatedOperation( userId,
                                 getDs3TargetsToReplciateDeleteToForDataPolicy( bucket.getDataPolicyId() ),
                                 "delete bucket " + bucket.getId() + " (" + bucket.getName() + ")" )
        {
            @Override
            protected void commit()
            {
                m_dataPlannerResource.deleteBucket( userId, bucketId, deleteObjects );
            }

            @Override
            protected void commit( final Ds3Target target, final Ds3Connection connection )
            {
                connection.deleteBucket( bucket.getName(), deleteObjects );
            }
        };
        return new RpcResponse<>( null );
    }
    
    
    public RpcFuture< ? > undeleteObject( final UUID userId, final S3Object object )
    {
        final Bucket bucket = m_serviceManager.getRetriever( Bucket.class )
                                              .attain( object.getBucketId() );
    
        new Ds3ReplicatedOperation( userId,
                                 getDs3TargetsToReplciateDeleteToForDataPolicy( bucket.getDataPolicyId() ),
                                 "undelete object " + object.getName() + " version " + object.getId() + "." )
        {
            @Override
            protected void commit()
            {
                m_dataPlannerResource.undeleteObject( userId, object );
            }

            @Override
            protected void commit( final Ds3Target target, final Ds3Connection connection )
            {
                connection.undeleteObject( object, bucket.getName() );
            }
        };
        return new RpcResponse<>( null );
    }
        
    
    public RpcFuture< UUID > createPutJob( final CreatePutJobParams params )
    {
        final Bucket bucket = m_serviceManager.getRetriever( Bucket.class ).attain( params.getBucketId() );
        final Set< Ds3Target > targets = m_serviceManager.getRetriever( Ds3Target.class ).retrieveAll( 
                TargetInitializationUtil.getInstance().getDs3TargetsToReplicateTo( 
                        m_serviceManager, bucket.getDataPolicyId() ) ).toSet();
        if ( params.isAggregating() )
        {
            LOG.info( "Cannot replicate the job creation against its " + targets.size() 
                      + " targets upfront since the job is aggregating." );
            targets.clear();
        }
        if ( params.isForce() )
        {
            for ( final Ds3Target target : targets )
            {
                target.setPermitGoingOutOfSync( true );
            }
        }

        final RuntimeException [] ex = new RuntimeException[ 1 ];
        final Job [] job = new Job[ 1 ];
        final UUID [] objectId = new UUID[ 1 ];
        new Ds3ReplicatedOperation( params.getUserId(), 
                                 targets,
                                 "create put job for bucket " + bucket.getId()
                                 + " (" + bucket.getName() + ")" )
        {
            @Override
            protected void commit()
            {
            	final UUID jobId;
        		jobId = m_dataPlannerResource.createPutJob( params ).getWithoutBlocking();
                job[ 0 ] = m_serviceManager.getRetriever( Job.class ).attain( jobId );
            }

            @Override
            protected void commit( final Ds3Target target, final Ds3Connection connection )
            {
                try
                {
                    TargetInitializationUtil.getInstance().prepareForPutReplication(
                            m_serviceManager, target, job[ 0 ], params.isForce(), connection );
                }
                catch ( final RuntimeException e )
                {
                    if ( !params.isForce() )
                    {
                        ex[ 0 ] = e;
                    }
                    throw e;
                }
            }
        };
        
        if ( null != ex[ 0 ] )
        {
            LOG.info( "Will cancel job due to failure.", ex[ 0 ] );
            cancelJob( params.getUserId(), job[ 0 ].getId(), false );
            throw ex[ 0 ];
        }
        return new RpcResponse<>( job[ 0 ].getId() );
    }


    public RpcFuture< ? > cancelJob( final UUID userId, final UUID jobId, final boolean force )
    {
        return cancelJob( userId, jobId, force, true );
    }


    public RpcFuture< ? > cancelJobQuietly( final UUID userId, final UUID jobId, final boolean force )
    {
        return cancelJob( userId, jobId, force, false );
    }


    private RpcFuture< ? > cancelJob(final UUID userId, final UUID jobId, final boolean force, final boolean createCanceledJob )
    {
        final Job job = m_serviceManager.getRetriever( Job.class ).attain( jobId );
        final Bucket bucket = m_serviceManager.getRetriever( Bucket.class ).attain( job.getBucketId() );
        final Map< UUID, S3Object > objects = BeanUtils.toMap(
        		m_serviceManager.getRetriever( S3Object.class ).retrieveAll(
        				Require.exists(
        						Blob.class,
        						Blob.OBJECT_ID,
        						Require.exists(
        								JobEntry.class,
        								BlobObservable.BLOB_ID,
        								Require.beanPropertyEquals( JobEntry.JOB_ID, jobId ) ) ) ).toSet() );
        
        final Set< UUID > nonUploadedBlobIds = ( force || JobRequestType.PUT != job.getRequestType() ) ? 
                new HashSet< UUID >() 
                : BeanUtils.toMap( m_serviceManager.getRetriever( Blob.class ).retrieveAll( Require.all( 
                        Require.beanPropertyEquals( ChecksumObservable.CHECKSUM, null ),
                        Require.exists(
                                JobEntry.class,
                                BlobObservable.BLOB_ID, 
                                Require.beanPropertyEquals( 
                                        JobEntry.JOB_ID, job.getId() ) ) ) ).toSet() ).keySet();
        final RuntimeException [] ex = new RuntimeException[ 1 ];
        final Set< UUID > retval = new HashSet<>();
        new Ds3ReplicatedOperation( 
                userId,
                getDs3TargetsForDataPolicy( bucket.getDataPolicyId() ),
                "cancel job " + jobId + " for bucket " + bucket.getName() + " (force=" + force + ")" )
        {
            @Override
            protected void validate( final Ds3Target target, final Ds3Connection connection )
            {
                if ( nonUploadedBlobIds.isEmpty() )
                {
                    return;
                }
                
                int uploadedBlobsOnTarget = 0;
                for ( final BlobPersistence bp 
                        : connection.getBlobPersistence( job.getId(), nonUploadedBlobIds ).getBlobs() )
                {
                    if ( null != bp.getChecksum() )
                    {
                        ++uploadedBlobsOnTarget;
                    }
                }
                if ( 0 == uploadedBlobsOnTarget )
                {
                    return;
                }
                
                throw new FailureTypeObservableException(
                        GenericFailure.FORCE_FLAG_REQUIRED, 
                        "Canceling this job will result in " + uploadedBlobsOnTarget 
                        + " blobs fully uploaded on target " + target.getId() + " (" + target.getName() 
                        + ") being deleted.  You must use the force flag to proceed or cancel this job on "
                        + target.getId() + " (" + target.getName() + ")." );
            }

            @Override
            protected void commit()
            {
                try
                {
                    if ( createCanceledJob )
                    {
                        retval.addAll( m_dataPlannerResource.cancelJobInternal( jobId, force ) );
                    }
                    else
                    {
                        retval.addAll( m_dataPlannerResource.cancelJobQuietlyInternal( jobId, force ) );
                    }

                    objects.keySet().retainAll( retval );
                }
                catch ( final CancelJobFailedException e )
                {
                    ex[ 0 ] = e;
                    retval.addAll( e.getDeletedObjectIds() );
                    objects.keySet().retainAll( retval );
                }
            }

            @Override
            protected void commit( final Ds3Target target, final Ds3Connection connection )
            {
                if ( !retval.isEmpty() )
                {
                    connection.deleteObjects( PreviousVersions.DELETE_SPECIFIC_VERSION, bucket, objects.values() );
                }
                else if ( JobRequestType.GET == job.getRequestType() )
                {
                    connection.cancelGetJob( job.getId() );
                }
            }
        };
        
        if ( null != ex[ 0 ] )
        {
            throw ex[ 0 ];
        }
        
        return null;
    }
    
    
    abstract class Ds3ReplicatedOperation
    {
        protected Ds3ReplicatedOperation( 
                final boolean force,
                final UUID userId, 
                final Set< Ds3Target > targets,
                final String operationDescription )
        {
            run( force, userId, targets, operationDescription );
        }
        
        
        protected Ds3ReplicatedOperation(
                final UUID userId, 
                final Set< Ds3Target > targets,
                final String operationDescription )
        {
            run( false, userId, targets, operationDescription );
        }
        
        
        final void run( 
                final boolean force,
                final UUID userId, 
                final Set< Ds3Target > targets, 
                final String operationDescription )
        {
            LOG.info( operationDescription.substring( 0, 1 ).toUpperCase() 
                      + operationDescription.substring( 1 ) 
                      + " requires replication to " + targets.size() + " DS3 targets." );
            final Map< Ds3Target, Ds3Connection > connections = new HashMap<>();
            try
            {
                for ( final Ds3Target target : targets )
                {
                    try
                    {
                        if ( Quiesced.NO == target.getQuiesced() )
                        {
                            final Ds3Connection connection = 
                                    m_ds3ConnectionFactory.connect( userId, target );
                            connections.put( target, connection );    
                        }
                        else
                        {
                            throw new FailureTypeObservableException(
                                    GenericFailure.CONFLICT, 
                                    "Target quiesced state must be 'NO' when " +
                                    "performing replicating operations." );
                        }
                        
                    }
                    catch ( final RuntimeException ex )
                    {
                        if ( target.isPermitGoingOutOfSync() )
                        {
                            LOG.warn( "Will proceed, even though cannot connect to DS3 target " 
                                      + target.getId() + " (" + target.getName() 
                                      + "), since target permitted to go out-of-sync.", ex );
                        }
                        else if ( force )
                        {
                            LOG.info( "Will proceed, even though cannot connect to DS3 target " 
                                    + target.getId() + " (" + target.getName() 
                                    + "), since operation is forced.", ex );
                        }
                        else
                        {
                            throw ex;
                        }
                        willNotCommit( target, ex );
                    }
                }

                for ( final Map.Entry< Ds3Target, Ds3Connection > e : connections.entrySet() )
                {
                    validate( e.getKey(), e.getValue() );
                }
                
                commit();
            }
            catch ( final Exception ex )
            {
                shutdownConnections( connections.values() );
                throw ExceptionUtil.toRuntimeException( ex );
            }
            
            for ( final Map.Entry< Ds3Target, Ds3Connection > e : connections.entrySet() )
            {
                final Ds3Target target = e.getKey();
                final Ds3Connection connection = e.getValue();
                try
                {
                    commit( target, connection );
                }
                catch ( final RuntimeException ex )
                {
                    final String msg = "Will ignore failure attempting to " + operationDescription
                            + " on " + target.getName() + " and continue.";
                    LOG.info( msg );
                    TargetLogger.LOG.info( msg, ex );
                }
            }
            
            shutdownConnections( connections.values() );
        }
        
        
        private void shutdownConnections( final Collection< Ds3Connection > connections )
        {
            for ( final Ds3Connection connection : connections )
            {
                connection.shutdown();
            }
        }
        
        
        /**
         * Validate remotely using the specified connection for the specified target prior to commits.
         */
        protected void validate( 
                @SuppressWarnings( "unused" ) final Ds3Target target, 
                @SuppressWarnings( "unused" ) final Ds3Connection connection )
        {
            // by default, do nothing
        }
        
        
        /**
         * Commit locally against local dao.
         */
        protected abstract void commit();
        
        
        protected void willNotCommit(
                @SuppressWarnings( "unused" ) final Ds3Target target,
                @SuppressWarnings( "unused" ) final RuntimeException ex )
        {
            // do nothing
        }
        
        
        /**
         * Commit remotely using the specified connection for the specified target.
         */
        protected abstract void commit( final Ds3Target target, final Ds3Connection connection );
    } // end inner class def

    
    private < C extends PublicCloudConnection,
        T extends PublicCloudReplicationTarget< T > & DatabasePersistable,
        R extends DataReplicationRule< R > & DatabasePersistable,
        CF extends PublicCloudConnectionFactory< C, T >,
        BNS extends PublicCloudBucketNameService< ? > >
                void deletePublicCloudBucket(
                    final Class< T > targetType,
                    final Class< R > ruleType,
                    final CF connectionFactory,
                    final Class< BNS > bucketNameServiceType,
                    final Bucket bucket )
    {
        final Set<T> targetsToDeleteBucketsOn = getTargetsToReplciateDeleteToForDataPolicy(
                targetType,
                ruleType,
                bucket.getDataPolicyId() );
        for ( T target : targetsToDeleteBucketsOn )
        {
            final C connection = connectionFactory.connect( target );
            try
            {
                final BNS bucketNameService = m_serviceManager.getService( bucketNameServiceType );
                final String bucketName =
                        bucketNameService.attainTargetBucketName( bucket.getId(), target.getId() );
                if ( connection.getExistingBucketInformation( bucketName ) != null )
                {
                    final PublicCloudBucketSupportImpl cloudBucketSupport = 
                            new PublicCloudBucketSupportImpl( bucketNameServiceType, m_serviceManager );
                    cloudBucketSupport.verifyBucket( connection, bucket.getId(), target.getId() );
                    connection.deleteBucket( bucketName );
                }
            }
            finally
            {
                connection.shutdown();
            }
        }
    }
    
    
    private < C extends PublicCloudConnection,
    T extends PublicCloudReplicationTarget< T > & DatabasePersistable,
    R extends PublicCloudDataReplicationRule< R > & DatabasePersistable,
    CF extends PublicCloudConnectionFactory< C, T >,
    BNS extends PublicCloudBucketNameService< ? > >
            void createPublicCloudBucket(
                final Class< T > targetType,
                final Class< R > ruleType,
                final CF connectionFactory,
                final Class< BNS > bucketNameServiceType,
                final Bucket bucket )
    {
        final Set< R > rules =
                m_serviceManager.getRetriever( ruleType ).retrieveAll(
                            Require.beanPropertyEquals(
                                    DataPlacement.DATA_POLICY_ID,
                                    bucket.getDataPolicyId() ) ).toSet();
        for ( R rule : rules )
        {
            final T target = m_serviceManager.getRetriever( targetType ).attain( rule.getTargetId() );
            final C connection = connectionFactory.connect( target );
            try
            {
                final BNS bucketNameService = m_serviceManager.getService( bucketNameServiceType );
                final String bucketName =
                        bucketNameService.generateTargetBucketName( bucket.getName(), target.getId() );
                final PublicCloudBucketInformation existingInfo =
                		connection.getExistingBucketInformation( bucketName );
                if ( null == existingInfo || null == existingInfo.getOwnerId() )
                {
                    connection.createOrTakeoverBucket(
                            getInitialDataPlacement( bucket, rule ),
                            BeanFactory.newBean( PublicCloudBucketInformation.class )
                            .setName( bucketName )
                            .setLocalBucketName( bucket.getName() )
                            .setOwnerId( m_serviceManager.getRetriever( DataPathBackend.class ).attain(
                                    Require.nothing() ).getInstanceId() )
                            .setVersion( PublicCloudBucketSupport.CLOUD_VERSION ) );
                }
            }
            finally
            {
                connection.shutdown();
            }
        }
    }
    
    
    /*
     * NOTE: The use of instanceof here and the explicit special handling of S3DataReplicationRules in a
     * non-polymorphic way is a necessary evil since non-S3 replication rules do not possess initial data
     * placement attributes. One possible alternative would be to include a bogus, always-null attribute
     * for this in the replication rules definitions for non-s3 cloud providers. - Kyle Hughart 05/25/17
     */
    protected Object getInitialDataPlacement( final Bucket bucket, final PublicCloudDataReplicationRule< ? > rule )
    {
        if ( rule instanceof S3DataReplicationRule )
        {
            return ( (S3DataReplicationRule)rule ).getInitialDataPlacement();
        }
        return null;
    }
    
    
    private Set< Ds3Target > getUserReplicatingDs3Targets()
    {
        return m_serviceManager.getRetriever( Ds3Target.class ).retrieveAll( 
                Ds3Target.ACCESS_CONTROL_REPLICATION,
                Ds3TargetAccessControlReplication.USERS ).toSet();
    }
    
    
    private Set< Ds3Target > getDs3TargetsForDataPolicy( final UUID dataPolicyId )
    {
        return m_serviceManager.getRetriever( Ds3Target.class ).retrieveAll( Require.exists( 
                Ds3DataReplicationRule.class,
                DataReplicationRule.TARGET_ID,
                Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ) ) ).toSet();
    }
    
    
    private Set< Ds3Target > getDs3TargetsToReplciateDeleteToForDataPolicy( final UUID dataPolicyId )
    {
        return getTargetsToReplciateDeleteToForDataPolicy(
                Ds3Target.class,
                Ds3DataReplicationRule.class,
                dataPolicyId );
    }
    
    
    private < T extends ReplicationTarget< T > & DatabasePersistable,
            M extends DataReplicationRule< M > & DatabasePersistable >
                Set< T > getTargetsToReplciateDeleteToForDataPolicy(
                        final Class< T > targetType,
                        final Class< M > ruleType,
                        final UUID dataPolicyId )
    {
        return m_serviceManager.getRetriever( targetType ).retrieveAll( Require.exists( 
                ruleType,
                DataReplicationRule.TARGET_ID,
                Require.all(
                        Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ), 
                        Require.beanPropertyEquals( DataReplicationRule.REPLICATE_DELETES, Boolean.TRUE ) ) )
                ).toSet();
    }


    public RpcFuture< UUID > registerAzureTarget( final AzureTarget target )
    {
        if ( null != m_serviceManager.getRetriever( AzureTarget.class ).retrieve(
                NameObservable.NAME, target.getName() ) )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONFLICT,
                    "Already registered another Azure instance with name " + target.getName() + "." );
        }

        m_azureConnectionFactory.connect( target ).shutdown();
        m_serviceManager.getService( AzureTargetService.class ).create( target );
        return new RpcResponse<>( target.getId() );
    }


    public RpcFuture< ? > modifyAzureTarget( final AzureTarget target, final String[] propertiesToUpdate )
    {
        final boolean onlyPropsModifyableOffline = 
                ( propertiesToUpdate != null )
                && TARGET_PROPS_MODIFYABLE_OFFLINE.containsAll( 
                        CollectionFactory.toSet( propertiesToUpdate ) );
        if ( !onlyPropsModifyableOffline )
        {
            m_azureConnectionFactory.connect( target ).shutdown();  
        }
        
        m_serviceManager.getService( AzureTargetService.class ).update( target, propertiesToUpdate );
        
        return null;
    }
    
    
    public void verifyPublicCloudTarget( 
            final Class< ? extends PublicCloudReplicationTarget< ? > > targetType,
            final UUID targetId, 
            final boolean fullyVerify )
    {
        if ( AzureTarget.class.isAssignableFrom( targetType ) )
        {
            verifyAzureTarget( targetId, fullyVerify );
        }
        else if ( S3Target.class.isAssignableFrom( targetType ) )
        {
            verifyS3Target( targetId, fullyVerify );
        }
        else
        {
            throw new UnsupportedOperationException( "No code to support: " + targetType );
        }
    }


    public RpcFuture< ? > verifyAzureTarget( final UUID targetId, final boolean fullyVerify )
    {
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>( 
                AzureTarget.class, 
                AzureDataReplicationRule.class, 
                targetId, 
                m_azureConnectionFactory,
                m_serviceManager,
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        if ( fullyVerify )
        {
            getVerifierWorkpool().submit(verifier);
        }
        return null;
    }


    public RpcFuture< UUID > registerS3Target( final S3Target target )
    {
        if ( null != m_serviceManager.getRetriever( S3Target.class ).retrieve(
                NameObservable.NAME, target.getName() ) )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONFLICT,
                    "Already registered another S3 instance with name " + target.getName() + "." );
        }

        m_s3ConnectionFactory.connect( target ).shutdown();
        m_serviceManager.getService( S3TargetService.class ).create( target );
        return new RpcResponse<>( target.getId() );
    }


    public RpcFuture< ? > modifyS3Target( final S3Target target, final String [] propertiesToUpdate )
    {
        final boolean onlyPropsModifyableOffline = 
                ( propertiesToUpdate != null )
                && TARGET_PROPS_MODIFYABLE_OFFLINE.containsAll( 
                        CollectionFactory.toSet( propertiesToUpdate ) );
        if ( !onlyPropsModifyableOffline )
        {
            m_s3ConnectionFactory.connect( target ).shutdown();  
        }
        
        m_serviceManager.getService( S3TargetService.class ).update( target, propertiesToUpdate );
        
        return null;
    }


    public RpcFuture< ? > verifyS3Target( final UUID targetId, final boolean fullyVerify )
    {
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>( 
                S3Target.class, 
                S3DataReplicationRule.class, 
                targetId, 
                m_s3ConnectionFactory,
                m_serviceManager,
                BlobS3TargetToVerifyService.class, 
                S3TargetBucketNameService.class,
                S3TargetFailureService.class );
        if ( fullyVerify )
        {
            getVerifierWorkpool().submit(verifier);
        }
        return null;
    }
    
    
    public RpcFuture< ? > importAzureTarget( final ImportAzureTargetDirective importDirective )
    {
        m_azureTargetImporter.importTarget( importDirective );
        return null;
    }


    public RpcFuture< ? > importS3Target( final ImportS3TargetDirective importDirective )
    {
        m_s3TargetImporter.importTarget( importDirective );
        return null;
    }
    
    
    @Override
    public RpcFuture<UUID> createBucket( final Bucket bucket )
    {       
    	createBucketOnCloudTargets( bucket );
        return m_dataPolicyManagementResource.createBucket( bucket );
    }
    
    
    private void createBucketOnCloudTargets( final Bucket bucket )
    {       
        createPublicCloudBucket(
                S3Target.class,
                S3DataReplicationRule.class,
                m_s3ConnectionFactory,
                S3TargetBucketNameService.class,
                bucket );
        createPublicCloudBucket(
                AzureTarget.class,
                AzureDataReplicationRule.class,
                m_azureConnectionFactory,
                AzureTargetBucketNameService.class,
                bucket );
    }
    
    
    @Override
    public RpcFuture< ? > cleanUpCompletedJobsAndJobChunks()
    {
        return m_dataPlannerResource.cleanUpCompletedJobsAndJobChunks();
    }

    @Override
    public RpcFuture<?> modifyJob(UUID jobId, BlobStoreTaskPriority priority) {

        return m_dataPlannerResource.modifyJob(jobId, priority);
    }


    public RpcFuture< UUID > createAzureDataReplicationRule( final AzureDataReplicationRule rule )
    {
    	final RpcFuture<UUID> retval = m_dataPolicyManagementResource.createAzureDataReplicationRule( rule );
    	try ( final EnhancedIterable< Bucket > buckets =
    			new AzureDataReplicationRuleRM( rule, m_serviceManager ).getDataPolicy().getBuckets().toIterable() )
    	{ 
	    	for ( final Bucket b : buckets )
	    	{
	    		createBucketOnCloudTargets( b );
	    	}
    	}
    	catch ( final RuntimeException e )
    	{ 
    		//NOTE: We delete instead of rolling back a transaction because we don't want to create an RPC call in
    		//the data policy mgmt resource that allows passing in a transaction. That interface is available to the
    		//server, so passing transactions through it could get weird and ugly.
    		m_dataPolicyManagementResource.deleteAzureDataReplicationRule( rule.getId() );
    		throw e;
    	}
    	return retval;
    }


    public RpcFuture< UUID > createS3DataReplicationRule( final S3DataReplicationRule rule )
    {
    	final RpcFuture<UUID> retval = m_dataPolicyManagementResource.createS3DataReplicationRule( rule );
    	try ( final EnhancedIterable< Bucket > buckets =
    			new S3DataReplicationRuleRM( rule, m_serviceManager ).getDataPolicy().getBuckets().toIterable() )
    	{ 
	    	for ( final Bucket b : buckets )
	    	{
	    		createBucketOnCloudTargets( b );
	    	}
    	}
    	catch ( final RuntimeException e )
    	{
    		//NOTE: We delete instead of rolling back a transaction because we don't want to create an RPC call in
    		//the data policy mgmt resource that allows passing in a transaction. That interface is available to the
    		//server, so passing transactions through it could get weird and ugly.
    		m_dataPolicyManagementResource.deleteS3DataReplicationRule( rule.getId() );
    		throw e;
    	}
    	return retval;
    }
    
    
    private final Ds3ConnectionFactory m_ds3ConnectionFactory;
    private final AzureConnectionFactory m_azureConnectionFactory;
    private final S3ConnectionFactory m_s3ConnectionFactory;
    private final DataPlannerResource m_dataPlannerResource;
    private final DataPolicyManagementResource m_dataPolicyManagementResource;
    private final PublicCloudTargetImportScheduler< ImportAzureTargetDirective > m_azureTargetImporter;
    private final PublicCloudTargetImportScheduler< ImportS3TargetDirective > m_s3TargetImporter;

    private final static Set< String > TARGET_PROPS_MODIFYABLE_OFFLINE = CollectionFactory.toSet(  
            DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB,
            PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX,
            PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX,
            NameObservable.NAME,
            ReplicationTarget.DEFAULT_READ_PREFERENCE, 
            ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC, 
            ReplicationTarget.QUIESCED );
    private final static Logger LOG = Logger.getLogger( TargetManagementResourceImpl.class );
    private static WorkPool m_targetVerifierWorkpool = WorkPoolFactory.createWorkPool( 5, "TargetVerifier" );
}
