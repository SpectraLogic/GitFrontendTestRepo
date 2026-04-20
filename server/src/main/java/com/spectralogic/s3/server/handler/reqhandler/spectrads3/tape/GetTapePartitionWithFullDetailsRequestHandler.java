/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.server.domain.DetailedTapePartition;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetTapePartitionWithFullDetailsRequestHandler 
    extends BaseGetBeanRequestHandler< TapePartition >
{
    public GetTapePartitionWithFullDetailsRequestHandler()
    {
        super( TapePartition.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.TAPE_PARTITION );
        
        registerRequiredRequestParameters( RequestParameterType.FULL_DETAILS );
    }
    

    @Override
    protected TapePartition performCustomPopulationWork(
            final DS3Request request,
            final CommandExecutionParams params,
            final TapePartition partition )
    {
        return GetTapePartitionRequestHandler.getTapePartition(
                DetailedTapePartition.class, params, partition );
    }
}
