/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetS3DataReplicationRuleRequestHandler
    extends BaseGetBeanRequestHandler< S3DataReplicationRule >
{
    public GetS3DataReplicationRuleRequestHandler()
    {
        super( S3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.S3_DATA_REPLICATION_RULE );
    }
}
