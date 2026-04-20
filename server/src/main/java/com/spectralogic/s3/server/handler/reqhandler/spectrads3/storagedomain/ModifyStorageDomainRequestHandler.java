/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyStorageDomainRequestHandler extends BaseModifyBeanRequestHandler< StorageDomain >
{
    public ModifyStorageDomainRequestHandler()
    {
        super( StorageDomain.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.STORAGE_DOMAIN );
        
        registerOptionalBeanProperties( 
                NameObservable.NAME,
                StorageDomain.MAX_TAPE_FRAGMENTATION_PERCENT,
                StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS,
                StorageDomain.MEDIA_EJECTION_ALLOWED,
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION,
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION,
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL,
                StorageDomain.AUTO_EJECT_UPON_CRON,
                StorageDomain.AUTO_EJECT_MEDIA_FULL_THRESHOLD,
                StorageDomain.VERIFY_PRIOR_TO_AUTO_EJECT,
                StorageDomain.LTFS_FILE_NAMING,
                StorageDomain.WRITE_OPTIMIZATION,
                StorageDomain.SECURE_MEDIA_ALLOCATION );
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final StorageDomain storageDomain,
            final Set< String > modifiedProperties )
    {
        CreateStorageDomainRequestHandler.validate( storageDomain );
        params.getDataPolicyResource().modifyStorageDomain( 
                storageDomain,
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.LONG );
        params.getTapeResource().refreshStorageDomainAutoEjectCronTriggers().get( Timeout.DEFAULT );
    }
}
