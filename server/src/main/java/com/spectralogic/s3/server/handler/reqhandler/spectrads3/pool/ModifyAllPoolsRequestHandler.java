/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import java.util.HashSet;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;

public final class ModifyAllPoolsRequestHandler extends BaseDaoTypedRequestHandler< Pool >
{
    public ModifyAllPoolsRequestHandler()
    {
        super( Pool.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_MODIFY,
                       RestDomainType.POOL ) );
        
        registerRequiredBeanProperties( Pool.QUIESCED );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final PoolService poolService = 
                params.getServiceManager().getService( PoolService.class );
        final Set< Pool > allPools = poolService.retrieveAll().toSet();
        final Quiesced setState = getBeanSpecifiedViaQueryParameters(
                params,
                AutoPopulatePropertiesWithDefaults.NO ).getQuiesced();
        final Set< String > validateFailures = new HashSet<>();
                
        for ( final Pool pool : allPools )
        {
            try
            {
                pool.setQuiesced( setState );
                ModifyPoolRequestHandler.validateQuiescedValueChange( 
                            poolService, pool );
                poolService.update( pool, TapePartition.QUIESCED );
            }
            catch ( RuntimeException e )
            {
                validateFailures.add( "Failed to validate " + pool.getName() + ": " + e.getMessage() );
                LOG.info( "Failed to validate " + pool + ".", e );
            }
        }
        
        if ( !validateFailures.isEmpty() )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONFLICT,
                    "Attempted to modify " + allPools.size() + " pools, but encountered " + 
                    validateFailures.size() + " failures: " + validateFailures + "." );
        }
        
        return BeanServlet.serviceModify( params, null );
    }
}
