/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.AuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestResourceType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.CollectionFactory;

/**
 * A request handler that will modify a bean of the type specified.  <br><br>
 * 
 * Any bean properties registered that are indeed bean properties of the dao type this request handler is for
 * will be used to modify the targeted bean.  For example, if property Pet.type has a value of 'DOG', then
 * the bean targeted / identified in the request will have its type property modified to 'DOG'.
 */
public abstract class BaseModifyBeanRequestHandler< T extends SimpleBeanSafeToProxy >
    extends BaseDaoTypedRequestHandler< T >
{
    protected BaseModifyBeanRequestHandler(
            final Class< T > daoType,
            final AuthenticationStrategy authenticationStrategy,
            final RestDomainType restDomain )
    {
        super(
                daoType,
                authenticationStrategy, 
                new RestfulCanHandleRequestDeterminer(
                        ( RestResourceType.SINGLETON == restDomain.getResourceType() ) ? 
                                RestActionType.BULK_MODIFY
                                : RestActionType.MODIFY, 
                        restDomain ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansRetriever< T > service = params.getServiceManager().getRetriever( m_daoType );
        final T specifiedBean = 
                getBeanSpecifiedViaQueryParameters( params, AutoPopulatePropertiesWithDefaults.NO );
        final T bean = request.getRestRequest().getBean( service );
        
        final Set< String > unmodifiedProperties = new HashSet<>();
        final Set< String > modifiedProperties = 
                new HashSet<>( request.getBeanPropertyValueMapFromRequestParameters().keySet() );
        for ( final String prop : new HashSet<>( modifiedProperties ) )
        {
            try
            {
                final Method reader = BeanUtils.getReader( m_daoType, prop );
                final Object originalValue = reader.invoke( bean );
                final Object value = reader.invoke( specifiedBean );
                if ( ( null == originalValue ) == ( null == value )
                        && ( null == originalValue || originalValue.equals( value ) ) )
                {
                    unmodifiedProperties.add( prop );
                    modifiedProperties.remove( prop );
                }
                else
                {
                    final Method writer = BeanUtils.getWriter( m_daoType, prop );
                    if ( null == reader.getAnnotation( Optional.class ) && null == value ) 
                    {
                        throw new S3RestException(
                                AWSFailure.INVALID_ARGUMENT,
                                "Query parameter '" + m_daoType.getSimpleName() + "." + prop 
                                + "' cannot be null when modifying it.");
                    }
                    writer.invoke( bean, value );
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to handle property " + prop + ".", ex );
            }
        }
        if ( !unmodifiedProperties.isEmpty() )
        {
            LOG.info( unmodifiedProperties.size() + " properties specified to modify were not modified: "
                      + unmodifiedProperties );
        }
        
        validateBeanToCommit( params, bean, modifiedProperties );
        if ( modifiedProperties.isEmpty() )
        {
            return BeanServlet.serviceModify( params, bean );
        }
        modifyBean( params, bean, modifiedProperties );

        return BeanServlet.serviceModify( params, bean );
    }
    
    
    protected void modifyBean(
            final CommandExecutionParams params,
            final T bean, 
            final Set< String > modifiedProperties )
    {
        final BeanUpdater< T > updater = params.getServiceManager().getUpdater( m_daoType );
        updater.update( bean, CollectionFactory.toArray( String.class, modifiedProperties ) );
    }
    
    
    protected void validateBeanToCommit(
            @SuppressWarnings( "unused" ) final CommandExecutionParams params,
            @SuppressWarnings( "unused" ) final T bean,
            @SuppressWarnings( "unused" ) final Set< String > modifiedProperties )
    {
        // do nothing by default
    }
}
