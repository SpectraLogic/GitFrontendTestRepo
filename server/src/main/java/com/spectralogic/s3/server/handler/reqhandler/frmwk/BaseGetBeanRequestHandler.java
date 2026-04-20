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
import com.spectralogic.s3.server.request.rest.RestResourceType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

/**
 * A request handler that will return the bean requested of a particular type, however the beans retriever for
 * that type populates it.
 */
public abstract class BaseGetBeanRequestHandler< T extends SimpleBeanSafeToProxy > 
    extends BaseDaoTypedRequestHandler< T >
{
    protected BaseGetBeanRequestHandler( 
            final Class< T > daoType,
            final AuthenticationStrategy authenticationStrategy,
            final RestDomainType restDomain )
    {
        super( daoType,
               authenticationStrategy, 
               new RestfulCanHandleRequestDeterminer(
                       ( RestResourceType.SINGLETON == restDomain.getResourceType() ) ? 
                               RestActionType.LIST : RestActionType.SHOW, 
                       restDomain ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        T retval = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( m_daoType ) );
        retval = performCustomPopulationWork( request, params, retval );
        return BeanServlet.serviceGet( params, retval );
    }
    
    
    protected T performCustomPopulationWork( 
            @SuppressWarnings( "unused" ) final DS3Request request,
            @SuppressWarnings( "unused" ) final CommandExecutionParams params,
            final T bean )
    {
        return bean;
    }
}
