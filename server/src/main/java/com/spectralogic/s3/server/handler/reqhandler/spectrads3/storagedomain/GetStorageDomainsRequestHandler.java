/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetStorageDomainsRequestHandler extends BaseGetBeansRequestHandler< StorageDomain >
{
    public GetStorageDomainsRequestHandler()
    {
        super( StorageDomain.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.STORAGE_DOMAIN );
        
        registerOptionalBeanProperties(
                NameObservable.NAME,
                StorageDomain.WRITE_OPTIMIZATION,
                StorageDomain.MEDIA_EJECTION_ALLOWED,
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION,
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION,
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL,
                StorageDomain.AUTO_EJECT_UPON_CRON,
                StorageDomain.SECURE_MEDIA_ALLOCATION );
    }
}
