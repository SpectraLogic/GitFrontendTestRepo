/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared;

import java.util.HashSet;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
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
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;

public abstract class BaseModifyAllTargetsRequestHandler
    < T extends Identifiable & ReplicationTarget< T > >
    extends BaseDaoTypedRequestHandler< T >
{
    protected BaseModifyAllTargetsRequestHandler( final Class< T > clazz, final RestDomainType domainType )
    {
        super( clazz,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_MODIFY,
                       domainType ) );
        
        registerRequiredBeanProperties( ReplicationTarget.QUIESCED );
    }
    
    
    public static < T extends Identifiable & ReplicationTarget< T > > void validateQuiescedValueChange( 
            final BeansRetriever< T > retriever, 
            final T bean ) 
    {
        final T target = retriever.attain( bean.getId() );
        if ( target.getQuiesced().equals( bean.getQuiesced() )
                || Quiesced.NO == bean.getQuiesced()
                || Quiesced.PENDING == bean.getQuiesced() )
        {
            return;
        }
        
        throw new FailureTypeObservableException( 
                GenericFailure.BAD_REQUEST,
                "It is illegal to transist the quiesced state from " + target.getQuiesced()
                + " to " + bean.getQuiesced() + "." );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansRetriever< T > retriever = params.getServiceManager().getRetriever( m_daoType );
        final BeanUpdater< T > updater = params.getServiceManager().getUpdater( m_daoType );
        final Set< T > allTargets = retriever.retrieveAll().toSet();
        final Quiesced setState = getBeanSpecifiedViaQueryParameters(
                params,
                AutoPopulatePropertiesWithDefaults.NO ).getQuiesced();
        final Set< String > validateFailures = new HashSet<>();
                
        for ( final T target : allTargets )
        {
            try
            {
                target.setQuiesced( setState );
                validateQuiescedValueChange( retriever, target );
                updater.update( target, ReplicationTarget.QUIESCED );
            }
            catch ( final RuntimeException e )
            {
                validateFailures.add( "Failed to validate " + target.getName() + ": " + e.getMessage() );
                LOG.info( "Failed to validate " + target + ".", e );
            }
        }
        
        if ( !validateFailures.isEmpty() )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONFLICT,
                    "Attempted to modify " + allTargets.size() + " replication targets, but encountered " + 
                    validateFailures.size() + " failures: " + validateFailures + "." );
        }
        
        return BeanServlet.serviceModify( params, null );
    }
}
