/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.command;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.JobCreatedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainMemberService;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.JobCreatedNotificationPayloadGenerator;
import com.spectralogic.s3.common.rpc.dataplanner.domain.*;
import com.spectralogic.s3.server.WireLogger;
import com.spectralogic.s3.server.domain.*;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared.JobResponseBuilder;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.manager.DatabasePhysicalSpaceState;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.marshal.sax.SaxException;
import com.spectralogic.util.marshal.sax.SaxParser;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.tunables.Tunables;
import org.apache.commons.io.IOUtils;

public final class CreateJob extends BaseCommand< ServletResponseStrategy >
{
    public CreateJob(
            final Job job,
            final BlobbingPolicy blobbingPolicy,
            final S3ObjectToJobApiBean objectToCreateNakedJobFor )
    {
        m_job = job;
        m_blobbingPolicy = blobbingPolicy;
        if ( job.isNaked() )
        {
            job.setImplicitJobIdResolution( true );
        }
        
        Validations.verifyNotNull( "Bucket id", m_job.getBucketId() );
        if ( null == objectToCreateNakedJobFor )
        {
            m_objectsToCreateJobFor = null;
        }
        else
        {
            m_objectsToCreateJobFor = BeanFactory.newBean( JobToCreateApiBean.class );

            final S3ObjectToJobApiBean obj = BeanFactory.newBean( S3ObjectToJobApiBean.class )
                    .setName( objectToCreateNakedJobFor.getName() )
                    .setSize( objectToCreateNakedJobFor.getSize() )
                    .setVersionId( objectToCreateNakedJobFor.getVersionId() );

            m_objectsToCreateJobFor.setObjects( new S3ObjectToJobApiBean[] { obj } );
        }
    }
    

    /**
     * For operations on multiple objects.
     */
    @Override
    protected ServletResponseStrategy executeInternal( final CommandExecutionParams params )
    {
        if ( !params.getServiceManager().getRetriever( DataPathBackend.class )
                                .attain( Require.nothing() )
                                .isAllowNewJobRequests() )
        {
            throw new S3RestException( GenericFailure.FORBIDDEN, "New job creation is currently disabled." );
        }
    
        final AtomicInteger numObjectsToJob = new AtomicInteger( ( null == m_objectsToCreateJobFor ) ?
            Tunables.createJobMaxNumberOfObjectsPerJob() : m_objectsToCreateJobFor.getObjects().length );
        
        try
        {
            CURRENT_CONCURRENT_OBJECTS_TO_JOB.acquire( numObjectsToJob.get() );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        try
        {
            return executeInternalInternal( params, numObjectsToJob );
        }
        finally
        {
            CURRENT_CONCURRENT_OBJECTS_TO_JOB.release( numObjectsToJob.get() );
        }
    }
    

    private ServletResponseStrategy executeInternalInternal(
            final CommandExecutionParams params, final AtomicInteger numObjectsToJob )
    {

        if ( null == params.getRequest().getAuthorization().getUser() )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST, 
                    "You must either authenticate as a user or impersonate a user." );
        }
        final DatabasePhysicalSpaceState databaseSpaceState = params.getServiceManager()
                                                                    .getDatabaseSpaceState();
        if ( ( JobRequestType.PUT == m_job.getRequestType() ) &&
                ( ( DatabasePhysicalSpaceState.LOW == databaseSpaceState ) ||
                        ( DatabasePhysicalSpaceState.CRITICAL == databaseSpaceState ) ) )
        {
            throw new S3RestException( 
                    AWSFailure.DATABASE_OUT_OF_SPACE,
                    "Free to total disk space ratio is low, thus the database can run out of physical space."
                    + " Contact Spectra." );
        }
        
        final JobToCreateApiBean jobToCreate;
        final BeansRetriever< Bucket > bucketRetriever =
                params.getServiceManager().getRetriever( Bucket.class );
        final Bucket bucket = ( params.getRequest().getRestRequest().isValidRestRequest() ) ?
                params.getRequest().getRestRequest().getBean( bucketRetriever )
                : bucketRetriever.attain( Bucket.NAME, params.getRequest().getBucketName() );
        final DataPolicy dataPolicy = params.getServiceManager().getRetriever( DataPolicy.class ).attain( 
                bucket.getDataPolicyId() );
        final S3ObjectsToJobApiBeanParser beansParser = new S3ObjectsToJobApiBeanParser( params.getServiceManager() );
        try 
        {
            if ( null == m_objectsToCreateJobFor )
            {
                WireLogger.LOG.info( "Receiving job request from client..." );
                final Duration duration = new Duration();

                final String contentTypeHeader = params.getRequest().getHttpRequest().getHeader(S3HeaderType.CONTENT_TYPE);
                if ( contentTypeHeader != null && contentTypeHeader.contains("/json")) {
                    jobToCreate = JsonMarshaler.unmarshal(
                            JobToCreateApiBean.class,
                            IOUtils.toString( params.getRequest().getHttpRequest().getInputStream(), Charset.defaultCharset() ) );
                    validateCreateJobObjects( jobToCreate, m_job.getRequestType() );
                } else {
                    final SaxParser sparser = new SaxParser();
                    final S3ObjectsToJobApiBeanSaxHandler bfh = new S3ObjectsToJobApiBeanSaxHandler( m_job.getRequestType() );
                    sparser.addHandler( bfh );
                    sparser.setInputStream( params.getRequest().getHttpRequest().getInputStream() );
                    sparser.parse();
                    jobToCreate = bfh.getJobToCreate();
                }
                
                LOG.info( "Received client request to create a job with " + jobToCreate.getObjects()
                                                                                       .length + " objects." );
                if ( 10000 > jobToCreate.getObjects().length )
                {
                    WireLogger.LOG.info( 
                            "Received client request in " + duration + " to create a job with " 
                                    + jobToCreate.getObjects().length + " objects: " + Platform.NEWLINE +
                                    Platform.NEWLINE
                                    + LogUtil.getShortVersion( JsonMarshaler.formatPretty( 
                                            jobToCreate.toJson() ) )
                                            + Platform.NEWLINE );
                }
                else
                {
                    WireLogger.LOG.info( "Received client request to create a job with " + jobToCreate.getObjects()
                                                                                                      .length +
                            " objects." );
                }
                if ( jobToCreate.getObjects().length > Tunables.createJobMaxNumberOfObjectsPerJob() )
                {
                    throw new S3RestException( 
                            GenericFailure.BAD_REQUEST, 
                            jobToCreate.getObjects().length
                            + " objects were specified for a single job, but "
                            + Tunables.createJobMaxNumberOfObjectsPerJob() + " is the maximum.  Please split your job up." );
                }
                CURRENT_CONCURRENT_OBJECTS_TO_JOB.release(
                        Tunables.createJobMaxNumberOfObjectsPerJob() - jobToCreate.getObjects().length );
                numObjectsToJob.set( jobToCreate.getObjects().length );
            }
            else
            {
                jobToCreate = m_objectsToCreateJobFor;
            }

            if ( jobToCreate == null 
                    || jobToCreate.getObjects() == null 
                    || jobToCreate.getObjects().length == 0 )
            {
                throw new S3RestException( 
                        GenericFailure.BAD_REQUEST,
                        "Empty job request not allowed." );
            }
            
            validateObjectNames( params.getServiceManager(), jobToCreate );

            if ( null == m_job.getPriority() )
            {
                m_job.setPriority( ( JobRequestType.GET == m_job.getRequestType() ) ? 
                        dataPolicy.getDefaultGetJobPriority() 
                        : ( JobRequestType.VERIFY == m_job.getRequestType() ) ?
                                dataPolicy.getDefaultVerifyJobPriority()
                                : dataPolicy.getDefaultPutJobPriority() );
            }
            
            final RpcFuture< UUID > future;
            if ( JobRequestType.GET == m_job.getRequestType() )
            {
                if ( null != m_objectsToCreateJobFor )
                {
                    jobToCreate.setObjects( m_objectsToCreateJobFor.getObjects() );
                }
                final Set< UUID > blobIds;
                if ( null == params.getRequest().getHttpRequest().getHeader( S3HeaderType.SPECIFY_BY_ID ) )
                {
                    blobIds = BeanUtils.toMap( beansParser.parseBlobsToGet( jobToCreate, bucket.getId(), true ) ).keySet();
                }
                else
                {
                    blobIds = beansParser.parseBlobIdsToGet( jobToCreate );
                }
                final String specifyByIdHeader = 
                        params.getRequest().getHttpRequest().getHeader( S3HeaderType.SPECIFY_BY_ID );
                final CreateGetJobParams getJobParams = BeanFactory.newBean( CreateGetJobParams.class )
                        .setName( getJobName( params ) )
                        .setReplicatedJobId( ( null == specifyByIdHeader ) ?
                                null
                                : UUID.fromString( specifyByIdHeader ) )
                        .setUserId( params.getRequest().getAuthorization().getUser().getId() )
                        .setPriority( m_job.getPriority() )
                        .setChunkOrderGuarantee( m_job.getChunkClientProcessingOrderGuarantee() )
                        .setAggregating( m_job.isAggregating() )
                        .setNaked( m_job.isNaked() )
                        .setImplicitJobIdResolution( m_job.isImplicitJobIdResolution() )
                        .setBlobIds( CollectionFactory.toArray( UUID.class, blobIds ) )
                        .setIomType( m_job.getIomType() == null ? IomType.NONE : m_job.getIomType() )
                        .setProtected( m_job.isProtected() );
                if ( params.getRequest().getBeanPropertyValueMapFromRequestParameters().containsKey( Job.DEAD_JOB_CLEANUP_ALLOWED ) )
                {
                    getJobParams.setDeadJobCleanupAllowed( Boolean.parseBoolean(
                            params.getRequest().getBeanPropertyValueMapFromRequestParameters().get( Job.DEAD_JOB_CLEANUP_ALLOWED ) ) );
                }

                if ( m_job.getIomType() == IomType.STAGE)
                {
                    final Set<StorageDomainMember> members = params.getServiceManager().getService(StorageDomainMemberService.class)
                            .getStorageDomainMembersToWriteTo(bucket.getDataPolicyId(), m_job.getIomType());
                    final Set<UUID> storageDomainIds = BeanUtils.extractPropertyValues(members, StorageDomainMember.STORAGE_DOMAIN_ID);
                    final PersistenceTargetInfo pti = BeanFactory.newBean(PersistenceTargetInfo.class)
                            .setStorageDomainIds(CollectionFactory.toArray(UUID.class, storageDomainIds))
                            .setDs3TargetIds(new UUID[0])
                            .setS3TargetIds(new UUID[0])
                            .setAzureTargetIds(new UUID[0]);;
                	future = params.getPlannerResource().createIomJob( getJobParams, pti);
                }
                else if ( m_job.getIomType() == null || m_job.getIomType() == IomType.NONE)
                {
	                future = params.getPlannerResource().createGetJob( getJobParams );
                } else {
                    throw new IllegalStateException("Only normal jobs and stage jobs can be created in this way.");
                }
            }
            else if ( JobRequestType.VERIFY == m_job.getRequestType() )
            {
                if ( null != m_objectsToCreateJobFor )
                {
                    jobToCreate.setObjects( m_objectsToCreateJobFor.getObjects() );
                }
                future = params.getPlannerResource().createVerifyJob( 
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setName( getJobName( params ) )
                        .setUserId( params.getRequest().getAuthorization().getUser().getId() )
                        .setPriority( m_job.getPriority() )
                        .setAggregating( m_job.isAggregating() )
                        .setBlobIds( CollectionFactory.toArray(
                                UUID.class, 
                                BeanUtils.toMap( beansParser.parseBlobsToGet( jobToCreate, bucket.getId(), true ) ).keySet() ) ) );
            }
            else
            {
            	final int nativeS3Targets = params.getServiceManager().getRetriever( S3Target.class ).getCount(
            			Require.all(
		            			Require.beanPropertyEquals(
		            					PublicCloudReplicationTarget.NAMING_MODE,
		            					CloudNamingMode.AWS_S3 ),
		    					Require.exists(
		    							S3DataReplicationRule.class,
		    							DataReplicationRule.TARGET_ID,
		    							Require.exists(
		    									DataPlacement.DATA_POLICY_ID,
		    									Require.beanPropertyEquals(
		    											Identifiable.ID,
		    											dataPolicy.getId() ) ) ) ) );            
                final Set< S3ObjectToCreate > objectsToCreate = new HashSet<>();
                for ( final S3ObjectToJobApiBean bob : jobToCreate.getObjects() )
                {
                    objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                            .setName( bob.getName() )
                            .setSizeInBytes( bob.getSize() ) );
                    if ( AWS_MULTI_PART_UPLOAD_MAXIMUM_TOTAL_SIZE_IN_BYTES < bob.getSize() && 0 < nativeS3Targets )
                    {
                    	throw new S3RestException( 
                                GenericFailure.CONFLICT,
                                "Object " + bob.getName() + " is " + bob.getSize() + " bytes long, which exceeds the"
                                		+ " maximum size of " + AWS_MULTI_PART_UPLOAD_MAXIMUM_TOTAL_SIZE_IN_BYTES
                            			+ " bytes for s3 cloud targets." );
                    }
                }
                
                Long maxUploadSizeInBytes = null;
                if ( params.getRequest().hasRequestParameter( RequestParameterType.MAX_UPLOAD_SIZE ) )
                {
                    maxUploadSizeInBytes = Long.valueOf( params.getRequest().getRequestParameter( 
                            RequestParameterType.MAX_UPLOAD_SIZE ).getLong() );
                }

                final CreatePutJobParams putJobParams = BeanFactory.newBean( CreatePutJobParams.class )
                        .setName( getJobName( params ) )
                        .setUserId( params.getRequest().getAuthorization().getUser().getId() )
                        .setPriority( m_job.getPriority() )
                        .setAggregating( m_job.isAggregating() )
                        .setNaked( m_job.isNaked() )
                        .setImplicitJobIdResolution( m_job.isImplicitJobIdResolution() )
                        .setBlobbingPolicy( m_blobbingPolicy )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( maxUploadSizeInBytes )
                        .setVerifyAfterWrite( m_job.isVerifyAfterWrite() )
                        .setMinimizeSpanningAcrossMedia( m_job.isMinimizeSpanningAcrossMedia()
                                || dataPolicy.isAlwaysMinimizeSpanningAcrossMedia() )
                        .setIgnoreNamingConflicts( params.getRequest().hasRequestParameter(
                                RequestParameterType.IGNORE_NAMING_CONFLICTS ) )
                        .setPreAllocateJobSpace( params.getRequest().hasRequestParameter(
                                RequestParameterType.PRE_ALLOCATE_JOB_SPACE ) )
                        .setObjectsToCreate( CollectionFactory.toArray(
                                S3ObjectToCreate.class, objectsToCreate ) )
                        .setForce( dataPolicy.isAlwaysForcePutJobCreation()
                                || params.getRequest().hasRequestParameter( RequestParameterType.FORCE ) )
                        .setProtected( m_job.isProtected() );
                if ( params.getRequest().getBeanPropertyValueMapFromRequestParameters().containsKey( Job.DEAD_JOB_CLEANUP_ALLOWED ) )
                {
                    putJobParams.setDeadJobCleanupAllowed( Boolean.parseBoolean(
                            params.getRequest().getBeanPropertyValueMapFromRequestParameters().get( Job.DEAD_JOB_CLEANUP_ALLOWED ) ) );
                }
                future = params.getTargetResource().createPutJob( putJobParams );
            }

            m_job.setId( future.get( Timeout.VERY_LONG ) );
            
            params.getServiceManager().getNotificationEventDispatcher().fire( 
                    new JobNotificationEvent(
                        params.getServiceManager().getRetriever( Job.class ).attain( m_job.getId() ),
                        params.getServiceManager().getRetriever( JobCreatedNotificationRegistration.class ),
                        new JobCreatedNotificationPayloadGenerator( m_job.getId() ) ) );
            
            return BeanServlet.serviceRequest( 
                    params,
                    HttpServletResponse.SC_OK,
                    new JobResponseBuilder(
                            m_job.getId(),
                            params ).buildFromDatabase() );
        }
        catch ( final SaxException se )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST, 
                    "Failed to parse request payload as xml.  " 
                    + "Please ensure you provided a valid XML structure to describe the objects in your " 
                    + "HTTP request to create a job.", se );
        }
        catch ( final IOException ioe )
        {
            throw new RuntimeException( ioe );
        }
    }
    
    
    private String getJobName( final CommandExecutionParams params )
    {
        if ( params.getRequest().getBeanPropertyValueMapFromRequestParameters().containsKey(
                NameObservable.NAME ) )
        {
            return params.getRequest().getBeanPropertyValueMapFromRequestParameters().get( 
                    NameObservable.NAME );
        }
        
        if ( null != m_job.getName() && !m_job.getName().equals(JobObservable.DEFAULT_JOB_NAME) )
        {
        	return m_job.getName();
        }
        
        return m_job.getRequestType() + " by " + params.getRequest().getHttpRequest().getRemoteHost();
    }


    /*
     * Input parsed via JSON payloads need to be validated for correct input based on job type.
     * Input parsed via XML payloads are validated within the S3ObjectsToJobApiBeanSaxHandler at time of parsing.
     */
    private void validateCreateJobObjects( final JobToCreateApiBean jobToCreate, final JobRequestType requestType )
    {
        for ( final S3ObjectToJobApiBean jobObject : jobToCreate.getObjects() )
        {
            final S3ObjectType objectType = S3ObjectType.fromObjectName( jobObject.getName() );
            switch (requestType)
            {
                case PUT:
                    if ( S3ObjectType.FOLDER == objectType && 0 != jobObject.getSize() )
                    {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Folders cannot contain data (size of " + jobObject.getSize() + " is invalid): "
                                        + jobObject.getName() );
                    }
                    break;
                case GET:
                case VERIFY:
                    if ( 0 > jobObject.getLength() )
                    {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Length for " + jobObject.getName() + " cannot be negative." );
                    }
                    if ( 0 > jobObject.getOffset() )
                    {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST,
                                "Offset for " + jobObject.getName() + " cannot be negative." );
                    }
                    break;
            }
        }
    }


    private void validateObjectNames( 
            final BeansServiceManager serviceManager, 
            JobToCreateApiBean jobToCreate )
    {
        final Bucket bucket = serviceManager.getRetriever( Bucket.class ).attain( m_job.getBucketId() );
        final DataPolicy dataPolicy = 
                serviceManager.getRetriever( DataPolicy.class ).attain( bucket.getDataPolicyId() );
        for ( final S3ObjectToJobApiBean jobObject : jobToCreate.getObjects() )
        {
            S3ObjectValidator.verify( serviceManager, dataPolicy, jobObject.getName() );
        }
    }
    
    
    UUID getJobId()
    {
        return m_job.getId();
    }
    
    
    private final JobToCreateApiBean m_objectsToCreateJobFor;
    private final BlobbingPolicy m_blobbingPolicy;
    private final Job m_job;
    
    private final static Semaphore CURRENT_CONCURRENT_OBJECTS_TO_JOB;
    private final static long AWS_MULTI_PART_UPLOAD_MAXIMUM_TOTAL_SIZE_IN_BYTES = 5 * 1024L * 1024 * 1024 * 1024;
    static
    {
        CURRENT_CONCURRENT_OBJECTS_TO_JOB = new Semaphore( Tunables.createJobMaxConcurrentObjectsToJob() );

        LOG.info( "The maximum number of objects per job is " + Tunables.createJobMaxNumberOfObjectsPerJob() + "." );
        LOG.info( "The maximum number of concurrent objects to job is "
                  + Tunables.createJobMaxConcurrentObjectsToJob() + "." );
    }
}
