/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CreateDataPolicyRequestHandler extends BaseCreateBeanRequestHandler< DataPolicy >
{
    public CreateDataPolicyRequestHandler()
    {
        super( DataPolicy.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DATA_POLICY );
        
        registerBeanProperties( 
                NameObservable.NAME,
                DataPolicy.BLOBBING_ENABLED,
                DataPolicy.CHECKSUM_TYPE,
                DataPolicy.DEFAULT_BLOB_SIZE,
                DataPolicy.DEFAULT_GET_JOB_PRIORITY,
                DataPolicy.DEFAULT_PUT_JOB_PRIORITY,
                DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY,
                DataPolicy.DEFAULT_VERIFY_AFTER_WRITE,
                DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.REBUILD_PRIORITY,
                DataPolicy.VERSIONING,
                DataPolicy.MAX_VERSIONS_TO_KEEP,
                DataPolicy.ALWAYS_FORCE_PUT_JOB_CREATION,
                DataPolicy.ALWAYS_MINIMIZE_SPANNING_ACROSS_MEDIA );
    }
}
