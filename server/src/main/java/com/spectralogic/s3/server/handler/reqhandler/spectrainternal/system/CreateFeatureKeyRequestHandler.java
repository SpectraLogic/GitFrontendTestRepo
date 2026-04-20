/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.FeatureKey;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.service.ds3.FeatureKeyService;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class CreateFeatureKeyRequestHandler extends BaseCreateBeanRequestHandler< FeatureKey >
{
    public CreateFeatureKeyRequestHandler()
    {
        super( FeatureKey.class,
               new InternalAccessOnlyAuthenticationStrategy(),
               RestDomainType.FEATURE_KEY );
        
        registerBeanProperties( 
                FeatureKey.KEY, 
                FeatureKey.EXPIRATION_DATE, 
                FeatureKey.LIMIT_VALUE,
                ErrorMessageObservable.ERROR_MESSAGE );
    }

    
    @Override
    protected UUID createBean( final CommandExecutionParams params, final FeatureKey bean )
    {
        final BeansServiceManager transaction = params.getServiceManager().startTransaction();
        try
        {
            transaction.getService( FeatureKeyService.class ).create( bean );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        return bean.getId();
    }
}
