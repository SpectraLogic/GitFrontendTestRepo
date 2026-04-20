/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import com.spectralogic.s3.server.handler.auth.AuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

/**
 * A request handler that will delete the a bean of the type specified.
 */
public abstract class BaseDeleteBeanRequestHandler< T extends SimpleBeanSafeToProxy & Identifiable >
    extends BaseDaoTypedRequestHandler< T >
{
    protected BaseDeleteBeanRequestHandler( 
            final Class< T > daoType,
            final AuthenticationStrategy authenticationStrategy, 
            final RestDomainType restDomainType )
    {
        super( daoType,
               authenticationStrategy,
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.DELETE,
                       restDomainType ) );
    }
    

    @Override
    final protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansRetriever< T > service = params.getServiceManager().getRetriever( m_daoType );
        final T bean = request.getRestRequest().getBean( service );
        validateDeleteRequest( bean );
        deleteBean( params, bean );
        
        return BeanServlet.serviceDelete( params, null );
    }
    
    
    protected void validateDeleteRequest( @SuppressWarnings( "unused" ) final T bean )
    {
        // do nothing
    }
    
    
    protected void deleteBean( final CommandExecutionParams params, final T bean )
    {
        final BeanDeleter deleter = params.getServiceManager().getDeleter( m_daoType );
        deleter.delete( bean.getId() );
    }
}
