/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.auth;

import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.frmwrk.UserInputValidations;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestRequest;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

/**
 * Authorization strategy that verifies that {@link BucketAcl}s exist that permit the operation to proceed,
 * or that the request is being made by an administrator.  Also ensures that the operation is never allowed 
 * on a system bucket, unless it is as an internal request. 
 */
public class BucketAuthorizationStrategy implements AuthenticationStrategy
{
    public BucketAuthorizationStrategy(
            final SystemBucketAccess systemBucketAuthorization,
            final BucketAclPermission permissionRequired,
            final AdministratorOverride administratorOverride )
    {
        m_systemBucketAuthorization = systemBucketAuthorization;
        m_permissionRequired = permissionRequired;
        m_administratorOverride = administratorOverride;
        Validations.verifyNotNull( "System bucket authorization", m_systemBucketAuthorization );
        Validations.verifyNotNull( "Permission required", m_permissionRequired );
        Validations.verifyNotNull( "Administrator override", m_administratorOverride );
    }
    
    
    /**
     * Constructor that ensures that the operation is never allowed on a system bucket, unless it is as an 
     * internal request.  For non system buckets, the only security check performed is to ensure that the
     * request is not being made anonymously.
     */
    protected BucketAuthorizationStrategy()
    {
        m_systemBucketAuthorization = SystemBucketAccess.INTERNAL_ONLY;
        m_permissionRequired = null;
        m_administratorOverride = null;
        Validations.verifyNotNull( "System bucket authorization", m_systemBucketAuthorization );
    }


    final public void authenticate( final CommandExecutionParams commandExecutionParams )
    {
        if ( INTERNAL_ACCESS_ONLY.isRequestInternal( commandExecutionParams.getRequest().getHttpRequest() ) )
        {
            return;
        }
        if ( null == commandExecutionParams.getRequest().getAuthorization().getUser() )
        {
            throw new S3RestException( 
                    GenericFailure.FORBIDDEN,
                    "Operation not permitted for anonymous logon." );
        }
        
        if ( commandExecutionParams.getRequest().getRestRequest().isValidRestRequest() )
        {
            final Map< String, String > beanProperties =
                    commandExecutionParams.getRequest().getBeanPropertyValueMapFromRequestParameters();
            if ( !commandExecutionParams.getRequest().hasRequestParameter( RequestParameterType.OPERATION ) 
                    && beanProperties.containsKey( NameObservable.NAME )
                    && RestDomainType.BUCKET 
                       == commandExecutionParams.getRequest().getRestRequest().getDomain() )
            {
                BucketAuthorization.verify(
                        m_systemBucketAuthorization,
                        m_permissionRequired,
                        m_administratorOverride,
                        commandExecutionParams,
                        beanProperties.get( NameObservable.NAME ) );
            }
            else if ( beanProperties.containsKey( "bucketId" ) )
            {
                final Bucket bucket = commandExecutionParams.getServiceManager().getRetriever(
                        Bucket.class ).discover( beanProperties.get( "bucketId" ) );
                BucketAuthorization.verify(
                        m_systemBucketAuthorization,
                        m_permissionRequired,
                        m_administratorOverride,
                        commandExecutionParams,
                        bucket );
            }
            else
            {
                final Bucket bucket = extractBucketFromRestPath( commandExecutionParams );
                if ( null != bucket )
                {
                    BucketAuthorization.verify(
                            m_systemBucketAuthorization,
                            m_permissionRequired,
                            m_administratorOverride,
                            commandExecutionParams,
                            bucket );
                }
            }
        }
        else
        {
            BucketAuthorization.verify(
                    m_systemBucketAuthorization,
                    m_permissionRequired,
                    m_administratorOverride,
                    commandExecutionParams,
                    commandExecutionParams.getRequest().getBucketName() );
        }
        
        m_decoratedStrategy.authenticate( commandExecutionParams );
    }
    
    
    private Bucket extractBucketFromRestPath( final CommandExecutionParams params )
    {
        final RestRequest restRequest = params.getRequest().getRestRequest();
        final BeansServiceManager serviceManager = params.getServiceManager();
        final Map< String, String > beanProperties =
                params.getRequest().getBeanPropertyValueMapFromRequestParameters();
        if ( beanProperties.containsKey( "job" ) )
        {
            return extractBucketFromJob( 
                    params, 
                    UserInputValidations.toUuid( beanProperties.get( "job" ) ) );
        }
        if ( beanProperties.containsKey( "jobId" ) )
        {
            return extractBucketFromJob( 
                    params, 
                    UserInputValidations.toUuid( beanProperties.get( "jobId" ) ) );
        }
        
        if ( RestDomainType.BUCKET == restRequest.getDomain() )
        {
            return restRequest.getBean( serviceManager.getRetriever( Bucket.class ) );
        }
        if ( RestDomainType.JOB == restRequest.getDomain()
                || RestDomainType.ACTIVE_JOB == restRequest.getDomain() )
        {
            return extractBucketFromJob(
                    params,
                    restRequest.getId( serviceManager.getRetriever( Job.class ) ) );
        }
        if ( RestDomainType.CANCELED_JOB == restRequest.getDomain() )
        {
            return extractBucketFromJob(
                    params,
                    restRequest.getId( serviceManager.getRetriever( CanceledJob.class ) ) );
        }
        if ( RestDomainType.COMPLETED_JOB == restRequest.getDomain() )
        {
            return extractBucketFromJob(
                    params,
                    restRequest.getId( serviceManager.getRetriever( CompletedJob.class ) ) );
        }
        if ( RestDomainType.JOB_CHUNK == restRequest.getDomain()
                || RestDomainType.JOB_CHUNK_DAO == restRequest.getDomain() )
        {
            return extractBucketFromJob( 
                    params, 
                    restRequest.getBean( serviceManager.getRetriever( JobEntry.class ) ).getJobId() );
        }

        if ( AdministratorOverride.YES == m_administratorOverride 
                && params.getGroupMembershipCache().isMember(
                        params.getRequest().getAuthorization().getUser().getId(), 
                        BuiltInGroup.ADMINISTRATORS ) )
        {
            LOG.info( "No bucket or job was specified, but will proceed since user is an administrator." );
            return null;
        }
        
        throw new S3RestException(
                GenericFailure.FORBIDDEN,
                "Neither a bucket nor job was specified." );
    }
    
    
    private Bucket extractBucketFromJob( final CommandExecutionParams params, final UUID jobId )
    {
        final User user = params.getRequest().getAuthorization().getUser();
        JobObservable< ? > job = params.getServiceManager().getRetriever( Job.class ).retrieve( jobId );
        if ( null == job )
        {
            job = params.getServiceManager().getRetriever( CompletedJob.class ).retrieve( jobId );
        }
        if ( null == job )
        {
            job = params.getServiceManager().getRetriever( CanceledJob.class ).retrieve( jobId );
        }
        if ( null == job )
        {
            throw new S3RestException( GenericFailure.NOT_FOUND, "Job doesn't exist: " + jobId );
        }
        if ( job.getUserId().equals( user.getId() ) )
        {
            return null;
        }
        return params.getServiceManager().getRetriever( Bucket.class ).attain( job.getBucketId() );
    }
    
    
    private final SystemBucketAccess m_systemBucketAuthorization;
    private final BucketAclPermission m_permissionRequired;
    private final AdministratorOverride m_administratorOverride;
    private final AuthenticationStrategy m_decoratedStrategy = 
            new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER );
    private final static InternalAccessOnlyAuthenticationStrategy INTERNAL_ACCESS_ONLY =
            new InternalAccessOnlyAuthenticationStrategy();
    private final static Logger LOG = Logger.getLogger( BucketAuthorizationStrategy.class );
}
