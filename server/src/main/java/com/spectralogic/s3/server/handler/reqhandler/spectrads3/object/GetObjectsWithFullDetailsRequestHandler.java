/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import com.spectralogic.s3.server.request.api.RequestParameterType;

public final class GetObjectsWithFullDetailsRequestHandler extends BaseGetObjectsRequestHandler
{
    public GetObjectsWithFullDetailsRequestHandler()
    {
        registerRequiredRequestParameters(
                RequestParameterType.FULL_DETAILS );
        registerOptionalRequestParameters( 
                RequestParameterType.INCLUDE_PHYSICAL_PLACEMENT );
    }
}
