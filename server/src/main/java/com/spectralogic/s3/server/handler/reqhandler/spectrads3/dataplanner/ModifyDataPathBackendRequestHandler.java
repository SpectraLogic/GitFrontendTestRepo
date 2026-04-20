/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.dataplanner;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;

import static com.spectralogic.util.lang.iterate.EnhancedIterable.MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED;

public final class ModifyDataPathBackendRequestHandler 
    extends BaseModifyBeanRequestHandler< DataPathBackend >
{
    public ModifyDataPathBackendRequestHandler()
    {
        super( DataPathBackend.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DATA_PATH_BACKEND );
        
        registerOptionalBeanProperties(
                DataPathBackend.ACTIVATED,
                DataPathBackend.ALLOW_NEW_JOB_REQUESTS,
                DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS,
                DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT,
                DataPathBackend.DEFAULT_VERIFY_DATA_AFTER_IMPORT,
                DataPathBackend.AUTO_INSPECT,
                DataPathBackend.UNAVAILABLE_TAPE_PARTITION_MAX_JOB_RETRY_IN_MINS,
                DataPathBackend.UNAVAILABLE_POOL_MAX_JOB_RETRY_IN_MINS,
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY,
                DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES,
                DataPathBackend.CACHE_AVAILABLE_RETRY_AFTER_IN_SECONDS,
                DataPathBackend.IOM_ENABLED,
                DataPathBackend.IOM_CACHE_LIMITATION_PERCENT,
                DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK,
                DataPathBackend.VERIFY_CHECKPOINT_BEFORE_READ,
                DataPathBackend.POOL_SAFETY_ENABLED,
                DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS,
                DataPathBackend.ALWAYS_ROLLBACK);
    }

    
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final DataPathBackend bean,
            final Set< String > modifiedProperties )
    {
        if ( modifiedProperties.contains( DataPathBackend.ACTIVATED ) && !bean.isActivated() )
        {
            throw new S3RestException( 
                    GenericFailure.CONFLICT,
                    "The data path backend can be activated, but cannot be deactivated.  " 
                    + "You may however quiesce any backend data store type as needed." );
        }
        if ( null != bean.getAutoActivateTimeoutInMins()
                && bean.getAutoActivateTimeoutInMins().intValue() < 0 )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST, 
                    DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS 
                    + " is '" + bean.getAutoActivateTimeoutInMins() 
                    + "' but it has to be greater or equal to zero if specified." );
        }
        if ( null != bean.getDefaultVerifyDataAfterImport() && bean.isDefaultVerifyDataPriorToImport() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST,
                    "Either request data verification prior to import or after import, but not both." );
        }
        if ( modifiedProperties.contains( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES ) 
                && null != bean.getPartiallyVerifyLastPercentOfTapes() )
        {
            if ( 100 == bean.getPartiallyVerifyLastPercentOfTapes().intValue() )
            {
                bean.setPartiallyVerifyLastPercentOfTapes( null );
            }
            else
            {
                if ( 0 >= bean.getPartiallyVerifyLastPercentOfTapes().intValue() )
                {
                    throw new S3RestException( 
                            GenericFailure.BAD_REQUEST, 
                            DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES 
                            + " is '" + bean.getPartiallyVerifyLastPercentOfTapes() 
                            + "' but it has to be greater than zero if specified." );
                }
                if ( 99 < bean.getPartiallyVerifyLastPercentOfTapes().intValue() )
                {
                    throw new S3RestException( 
                            GenericFailure.BAD_REQUEST, 
                            DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES 
                            + " is '" + bean.getPartiallyVerifyLastPercentOfTapes()
                            + "' but it cannot exceed 99 if specified." );
                }
            }
        }
        if ( modifiedProperties.contains( DataPathBackend.CACHE_AVAILABLE_RETRY_AFTER_IN_SECONDS ) )
        {
            if ( 0 >= bean.getCacheAvailableRetryAfterInSeconds() )
            {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        DataPathBackend.CACHE_AVAILABLE_RETRY_AFTER_IN_SECONDS
                        + " is '" + bean.getCacheAvailableRetryAfterInSeconds()
                        + "' but it has to be greater than zero if specified." );
            }
        }

        if ( modifiedProperties.contains( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT ) ) {
            double iomCacheLimitationPercent = bean.getIomCacheLimitationPercent();
            if (iomCacheLimitationPercent <= 0.0 || iomCacheLimitationPercent > 1.0) {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        DataPathBackend.IOM_CACHE_LIMITATION_PERCENT
                                + " is '" + iomCacheLimitationPercent
                                + "' but must be a percentage value greater than 0 and less than or equal to 1.0"
                );
            }
        }

        if ( modifiedProperties.contains( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK ) ) {
            int maxAggregatedBlobsPerChunk = bean.getMaxAggregatedBlobsPerChunk();
            if (maxAggregatedBlobsPerChunk < 0 || maxAggregatedBlobsPerChunk > MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED) {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK
                                + " is '" + maxAggregatedBlobsPerChunk
                                + "' but must be a value 0 to 500,000"
                );
            }
        }
        if ( modifiedProperties.contains( DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS ) ) {
            int maxNumberOfConcurrentJobs = bean.getMaxNumberOfConcurrentJobs();
            if ( maxNumberOfConcurrentJobs < MIN_ACTIVE_JOB_LIMIT || maxNumberOfConcurrentJobs > MAX_ACTIVE_JOB_LIMIT ) {
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS
                                + " is '" + maxNumberOfConcurrentJobs
                                + "' but must be a value " + MIN_ACTIVE_JOB_LIMIT + " to " + MAX_ACTIVE_JOB_LIMIT
                );
            }
        }
    }

    /**
     * The maximum number of active jobs the system can be configured to allow.
     */
    static int MAX_ACTIVE_JOB_LIMIT = MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED;

    /**
     * The minimum value that can be set for limiting the maximum active jobs in the system. The system may have fewer
     * jobs at any time, but it cannot enforce a smaller number.
     */
    static int MIN_ACTIVE_JOB_LIMIT = 10;
}
