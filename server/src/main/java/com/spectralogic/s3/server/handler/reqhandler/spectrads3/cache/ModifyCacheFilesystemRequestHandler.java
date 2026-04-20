/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;

public final class ModifyCacheFilesystemRequestHandler
    extends BaseModifyBeanRequestHandler< CacheFilesystem >
{
    public ModifyCacheFilesystemRequestHandler()
    {
        super( CacheFilesystem.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.CACHE_FILESYSTEM );
        
        registerOptionalBeanProperties(
               CacheFilesystem.MAX_CAPACITY_IN_BYTES,
               CacheFilesystem.AUTO_RECLAIM_INITIATE_THRESHOLD,
               CacheFilesystem.AUTO_RECLAIM_TERMINATE_THRESHOLD,
               CacheFilesystem.BURST_THRESHOLD,
               CacheFilesystem.CACHE_SAFETY_ENABLED,
               CacheFilesystem.NEEDS_RECONCILE );
    }

    
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final CacheFilesystem cf,
            final Set< String > modifiedProperties )
    {
        if ( cf.getAutoReclaimInitiateThreshold() < 0 )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    "Auto reclaim initiate threshold cannot be less than zero." );
        }
        if ( cf.getAutoReclaimInitiateThreshold() > 1 )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    "Auto reclaim initiate threshold cannot be greater than than one." );
        }
        if ( cf.getAutoReclaimTerminateThreshold() < 0 )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    "Auto reclaim terminate threshold cannot be less than zero." );
        }
        if ( null != cf.getMaxCapacityInBytes()
                && cf.getMaxCapacityInBytes().longValue() < MIN_CACHE_CAPACITY )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    "Max capacity in bytes cannot be less than " + MIN_CACHE_CAPACITY + "." );
        }
        if ( cf.getBurstThreshold() < 0 )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    "Burst threshold cannot be less than zero." );
        }
        if ( cf.getBurstThreshold() > 1 )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    "Burst threshold cannot be greater than one." );
        }
        if ( cf.getAutoReclaimInitiateThreshold() < cf.getAutoReclaimTerminateThreshold() )
        {
            throw new S3RestException(
                    GenericFailure.CONFLICT,
                    "Auto reclaim terminate threshold cannot be higher than initiate threshold." );
        }
    }
    
    
    private final static long MIN_CACHE_CAPACITY = 1024 * 1024L * 1024;
}
