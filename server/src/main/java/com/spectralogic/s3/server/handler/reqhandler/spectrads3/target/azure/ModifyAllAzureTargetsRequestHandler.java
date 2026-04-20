/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseModifyAllTargetsRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class ModifyAllAzureTargetsRequestHandler
    extends BaseModifyAllTargetsRequestHandler< AzureTarget >
{
    public ModifyAllAzureTargetsRequestHandler()
    {
        super( AzureTarget.class, RestDomainType.AZURE_TARGET );
    }
}
