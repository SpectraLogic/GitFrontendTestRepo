/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapeDensityDirective;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;

public final class CreateTapeDensityDirectiveRequestHandler 
    extends BaseCreateBeanRequestHandler< TapeDensityDirective >
{
    public CreateTapeDensityDirectiveRequestHandler()
    {
        super( TapeDensityDirective.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ),
               RestDomainType.TAPE_DENSITY_DIRECTIVE );
        
        registerBeanProperties( 
                TapeDensityDirective.DENSITY,
                TapeDensityDirective.PARTITION_ID,
                TapeDensityDirective.TAPE_TYPE );
    }

    
    @Override
    protected void validateBeanForCreation(
            final CommandExecutionParams params, 
            final TapeDensityDirective directive )
    {
        if ( !directive.getDensity().toString().startsWith( "TS" ) )
        {
            fail( TapeDensityDirective.DENSITY + " invalid: " + directive.getDensity() );
        }
        if ( !directive.getTapeType().toString().startsWith( "TS" ) )
        {
            fail( TapeDensityDirective.TAPE_TYPE + " invalid: " + directive.getTapeType() );
        }
        if ( !directive.getTapeType().canContainData() )
        {
            fail( TapeDensityDirective.TAPE_TYPE + " invalid: " + directive.getTapeType() );
        }
    }
    
    
    private void fail( final String cause )
    {
        throw new S3RestException( GenericFailure.BAD_REQUEST, cause );
    }
}
