/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.service.target.PublicCloudBucketNameService;
import com.spectralogic.s3.common.rpc.target.AzureConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

public final class PublicCloudBucketSupportImpl implements PublicCloudBucketSupport
{
    public PublicCloudBucketSupportImpl( 
            final Class< ? extends PublicCloudBucketNameService< ? > > bucketNameServiceType,
            final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        m_bucketNameServiceType = bucketNameServiceType;
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        Validations.verifyNotNull( "Service manager", m_serviceManager );
    }
    
    
    public PublicCloudBucketInformation verifyBucketToImport( 
            final PublicCloudConnection connection, 
            final String cloudBucketName )
    {
        return verifyBucketInternal(
                connection, 
                null,
                cloudBucketName, 
                false,
                true);
    }

    public PublicCloudBucketInformation verifyBucketForWrite(
            final PublicCloudConnection connection,
            final UUID bucketId,
            final UUID targetId)
    {
        final String cloudBucketName = attainCloudBucketName( bucketId, targetId );
        return verifyBucketInternal(
                connection,
                m_serviceManager.getRetriever( Bucket.class ).attain( bucketId ).getName(),
                cloudBucketName,
                true,
                //Special-cased until such time as we decide to allow existing bucket/container takeover in Azure
                connection instanceof AzureConnection);
    }


    public PublicCloudBucketInformation verifyBucket( 
            final PublicCloudConnection connection, 
            final UUID bucketId,
            final UUID targetId )
    {
        final String cloudBucketName = attainCloudBucketName( bucketId, targetId );
        return verifyBucketInternal( 
                connection,
                m_serviceManager.getRetriever( Bucket.class ).attain( bucketId ).getName(),
                cloudBucketName, 
                true,
                true);
    }
    
    
    private PublicCloudBucketInformation verifyBucketInternal(
            final PublicCloudConnection connection, 
            final String localBucketName,
            final String cloudBucketName,
            final boolean verifyOwnershipMatches,
            final boolean throwIfNonBpBucketAlreadyExists)
    {
        final PublicCloudBucketInformation retval = 
                connection.getExistingBucketInformation( cloudBucketName );
        if ( null == retval )
        {
            return null;
        }

        if ( null == retval.getOwnerId() )
        {
            if (throwIfNonBpBucketAlreadyExists) {
                throw new FailureTypeObservableException(
                        GenericFailure.CONFLICT,
                        "Cloud bucket already exists and is not formatted for use by this appliance.");
            } else {
                //Treat this the same as no bucket found. We can take over the existing one.
                return null;
            }
        }
        if ( null != localBucketName && !localBucketName.equals( retval.getLocalBucketName() ) )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONFLICT, 
                    "Expected the cloud bucket to be for " + localBucketName + ", but it was for " 
                    + retval.getLocalBucketName() + ".  Please ensure " + localBucketName 
                    + " is being mapped to the correct cloud bucket name." );
        }
        if ( retval.getVersion() > CLOUD_VERSION )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONFLICT, 
                    "The cloud structure version is " + retval.getVersion() 
                    + ", but this version of software only supports versions up to " + CLOUD_VERSION + "." );
        }
        if ( verifyOwnershipMatches )
        {
            if ( !m_serviceManager.getRetriever( DataPathBackend.class ).attain( Require.nothing() )
                    .getInstanceId().equals( retval.getOwnerId() ) )
            {
                throw new FailureTypeObservableException( 
                        GenericFailure.CONFLICT, 
                        "The cloud bucket is owned by another appliance with identifier "
                        + retval.getOwnerId() + "." );
            }
        }
        
        return retval;
    }
    
    
    public String attainCloudBucketName( final UUID bucketId, final UUID targetId )
    {
        return m_serviceManager.getService( m_bucketNameServiceType ).attainTargetBucketName(
                bucketId, targetId );
    }
    
    
    private final BeansServiceManager m_serviceManager;
    private final Class< ? extends PublicCloudBucketNameService< ? > > m_bucketNameServiceType;
}
