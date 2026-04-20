/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.HashSet;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
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

public final class ModifyAllTapePartitionsRequestHandler extends BaseDaoTypedRequestHandler< TapePartition >
{
    public ModifyAllTapePartitionsRequestHandler()
    {
        super( TapePartition.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_MODIFY,
                       RestDomainType.TAPE_PARTITION ) );
        
        registerRequiredBeanProperties( TapePartition.QUIESCED );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final TapePartitionService tpService = 
                params.getServiceManager().getService( TapePartitionService.class );
        final Set< TapePartition > allPartitions = tpService.retrieveAll().toSet();
        final Quiesced setState = getBeanSpecifiedViaQueryParameters(
                params,
                AutoPopulatePropertiesWithDefaults.NO ).getQuiesced();
        final Set< String > validateFailures = new HashSet<>();
                
        for ( final TapePartition partition : allPartitions )
        {
            try
            {
                partition.setQuiesced( setState );
                ModifyTapePartitionRequestHandler.validateQuiescedValueChange( 
                            tpService, partition );
                tpService.update( partition, TapePartition.QUIESCED );
            }
            catch ( RuntimeException e )
            {
                validateFailures.add( "Failed to validate " + partition.getName() + ": " + e.getMessage() );
                LOG.info( "Failed to validate " + partition + ".", e );
            }
        }

        if ( setState == Quiesced.NO ) {
            //We just unquiesced partitions, flag environment for refresh
            params.getTapeResource().flagEnvironmentForRefresh();
        }
        
        if ( !validateFailures.isEmpty() )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONFLICT,
                    "Attempted to modify " + allPartitions.size() + " tape partitions, but encountered " + 
                    validateFailures.size() + " failures: " + validateFailures + "." );
        }
        
        return BeanServlet.serviceModify( params, null );
    }
}
