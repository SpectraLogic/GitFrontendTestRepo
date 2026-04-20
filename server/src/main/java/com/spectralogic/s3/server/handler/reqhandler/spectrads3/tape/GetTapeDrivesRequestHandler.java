/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetTapeDrivesRequestHandler extends BaseGetBeansRequestHandler< TapeDrive >
{
    public GetTapeDrivesRequestHandler()
    {
        super( TapeDrive.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.TAPE_DRIVE );
        
        registerOptionalBeanProperties(
                TapeDrive.PARTITION_ID,
                TapeDrive.STATE,
                TapeDrive.TYPE,
                TapeDrive.RESERVED_TASK_TYPE,
                TapeDrive.MINIMUM_TASK_PRIORITY,
                SerialNumberObservable.SERIAL_NUMBER );
    }
}
