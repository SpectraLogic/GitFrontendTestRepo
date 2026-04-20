/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.thread.CronRunnableExecutor;

public final class CreateStorageDomainRequestHandler
    extends BaseCreateBeanRequestHandler< StorageDomain >
{
    public CreateStorageDomainRequestHandler()
    {
        super( StorageDomain.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.STORAGE_DOMAIN );
        
        registerBeanProperties( 
                StorageDomain.MAX_TAPE_FRAGMENTATION_PERCENT,
                StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS,
                StorageDomain.MEDIA_EJECTION_ALLOWED,
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION,
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION,
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL,
                StorageDomain.AUTO_EJECT_UPON_CRON,
                StorageDomain.AUTO_EJECT_MEDIA_FULL_THRESHOLD,
                StorageDomain.VERIFY_PRIOR_TO_AUTO_EJECT,
                StorageDomain.WRITE_OPTIMIZATION,
                StorageDomain.LTFS_FILE_NAMING,
                NameObservable.NAME,
                StorageDomain.SECURE_MEDIA_ALLOCATION );
    }

    
    @Override
    protected UUID createBean( final CommandExecutionParams params, final StorageDomain storageDomain )
    {
        validate( storageDomain );
        final UUID retval = super.createBean( params, storageDomain );
        params.getTapeResource().refreshStorageDomainAutoEjectCronTriggers().get( Timeout.DEFAULT );
        return retval;
    }


    static void validate( final StorageDomain storageDomain )
    {
        if ( null != storageDomain.getAutoEjectUponCron() )
        {
            CronRunnableExecutor.verify( storageDomain.getAutoEjectUponCron() );
        }
        if ( 10 > storageDomain.getMaxTapeFragmentationPercent()
                || 100 < storageDomain.getMaxTapeFragmentationPercent() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST, 
                    "The " + StorageDomain.MAX_TAPE_FRAGMENTATION_PERCENT 
                    + " must be between 10% and 100%." );
        }
        
        if ( storageDomain.isMediaEjectionAllowed() )
        {
            return;
        }
        if ( null == storageDomain.getAutoEjectUponCron() 
                &&  null == storageDomain.getAutoEjectMediaFullThreshold() 
                && !storageDomain.isAutoEjectUponJobCompletion() 
                && !storageDomain.isAutoEjectUponJobCancellation()
                && !storageDomain.isAutoEjectUponMediaFull() )
        {
            return;
        }
        
        throw new S3RestException( 
                GenericFailure.CONFLICT, 
                "Storage domains that don't allow media export cannot define auto-export triggers." );
    }
}
