/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.auth;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.security.BucketAccessRequest;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

public final class BucketAuthorization
{
    public enum SystemBucketAccess
    {
        /**
         * Only Spectra internal requests will be permitted to invoke the operation on system buckets.
         */
        INTERNAL_ONLY,
        
        /**
         * Any standard user may invoke the operation on system buckets, provided that standard ACL security
         * checks pass.
         */
        STANDARD
    }
    
    
    public static void verify( 
            final SystemBucketAccess alwaysDenyForSystemBuckets,
            final BucketAclPermission permissionRequired,
            final AdministratorOverride administratorOverride,
            final CommandExecutionParams commandExecutionParams )
    {
        verify( alwaysDenyForSystemBuckets,
                permissionRequired,
                administratorOverride,
                commandExecutionParams, 
                commandExecutionParams.getRequest().getBucketName() );
    }
    
    
    public static void verify( 
            final SystemBucketAccess alwaysDenyForSystemBuckets,
            final BucketAclPermission permissionRequired,
            final AdministratorOverride administratorOverride,
            final CommandExecutionParams commandExecutionParams, 
            final String bucketName )
    {
        verify( alwaysDenyForSystemBuckets,
                permissionRequired,
                administratorOverride,
                commandExecutionParams, 
                ( null == administratorOverride ) ?
                        BeanFactory.newBean( Bucket.class ).setName( bucketName )
                        : commandExecutionParams.getServiceManager().getRetriever( Bucket.class ).attain( 
                                Bucket.NAME, bucketName ) );
    }
    
    
    public static void verify( 
            final SystemBucketAccess alwaysDenyForSystemBuckets,
            final BucketAclPermission permissionRequired,
            final AdministratorOverride administratorOverride,
            final CommandExecutionParams commandExecutionParams, 
            final UUID bucketId )
    {
        verify( alwaysDenyForSystemBuckets,
                permissionRequired,
                administratorOverride,
                commandExecutionParams, 
                commandExecutionParams.getServiceManager().getRetriever( Bucket.class ).attain( bucketId ) );
    }
    
    
    public static void verify( 
            final SystemBucketAccess alwaysDenyForSystemBuckets,
            final BucketAclPermission permissionRequired,
            final AdministratorOverride administratorOverride,
            final CommandExecutionParams commandExecutionParams, 
            final Bucket bucket )
    {
        Validations.verifyNotNull( "Command execution params", commandExecutionParams );
        Validations.verifyNotNull( "Bucket", bucket );
        
        if ( INTERNAL_ACCESS_ONLY.isRequestInternal( commandExecutionParams.getRequest().getHttpRequest() ) )
        {
            return;
        }
        
        if ( null == bucket.getName() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST, 
                    "Bucket name cannot be " + bucket.getName() + "." );
        }
        if ( SystemBucketAccess.INTERNAL_ONLY == alwaysDenyForSystemBuckets 
                && bucket.getName().toLowerCase().startsWith( SYSTEM_BUCKET_NAME_PREFIX ) )
        {
            if ( null == commandExecutionParams.getRequest().getHttpRequest().getHeader( 
                    S3HeaderType.REPLICATION_SOURCE_IDENTIFIER ) )
            {
                throw new S3RestException( 
                        GenericFailure.FORBIDDEN, 
                        "Bucket names that start with " + SYSTEM_BUCKET_NAME_PREFIX 
                        + " are reserved for Spectra internal use only." );
            }
            LOG.info( "Since this is a replicated request, " 
                      + "will skip the internal only authorization requirement." );
        }
        
        if ( null != administratorOverride )
        {
            commandExecutionParams.getBucketAclAuthorizationService().verifyHasAccess(
                    new BucketAccessRequest( 
                            commandExecutionParams.getRequest().getAuthorization().getUser().getId(), 
                            bucket.getId(), 
                            permissionRequired ),
                            administratorOverride );
        }
    }
    
    
    public static Set< UUID > getBucketsUserHasAccessTo(
            final SystemBucketAccess alwaysDenyForSystemBuckets,
            final BucketAclPermission permissionRequired,
            final AdministratorOverride administratorOverride,
            final CommandExecutionParams commandExecutionParams )
    {
        final boolean internalRequest = INTERNAL_ACCESS_ONLY.isRequestInternal( 
                commandExecutionParams.getRequest().getHttpRequest() );
        if ( !internalRequest && null == commandExecutionParams.getRequest().getAuthorization().getUser() )
        {
            throw new S3RestException( 
                    GenericFailure.FORBIDDEN, 
                    "Operation cannot be performed via anonymous logon." );
        }
        
        final Set< UUID > retval = new HashSet<>();
        try
        {
	        commandExecutionParams.getServiceManager().reserveConnections( 0, 2 );
	        try ( final EnhancedIterable< Bucket > iterable =
	                commandExecutionParams.getServiceManager().getRetriever( Bucket.class ).retrieveAll( 
	                        Require.nothing() ).toIterable() )
	        {
	            for ( final Bucket bucket : iterable )
	            {
	                if ( SystemBucketAccess.INTERNAL_ONLY == alwaysDenyForSystemBuckets 
	                        && bucket.getName().startsWith( SYSTEM_BUCKET_NAME_PREFIX ) )
	                {
	                    continue;
	                }
	                
	                if ( internalRequest 
	                     || commandExecutionParams.getBucketAclAuthorizationService().hasAccess(
	                        new BucketAccessRequest( 
	                                commandExecutionParams.getRequest().getAuthorization().getUser().getId(),
	                                bucket.getId(),
	                                permissionRequired ), 
	                        administratorOverride ) )
	                {
	                    retval.add( bucket.getId() );
	                }
	            }
	            return retval;
	        }
        }
        finally
        {
            commandExecutionParams.getServiceManager().releaseReservedConnections();
        }
    }
    
    
    private final static InternalAccessOnlyAuthenticationStrategy INTERNAL_ACCESS_ONLY = 
            new InternalAccessOnlyAuthenticationStrategy();
    public final static String SYSTEM_BUCKET_NAME_PREFIX = "spectra-";
    private final static Logger LOG = Logger.getLogger( BucketAuthorization.class );
}
