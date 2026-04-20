/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import com.spectralogic.s3.server.handler.auth.AuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.CanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.Validations;

public abstract class BaseDaoTypedRequestHandler< T extends SimpleBeanSafeToProxy >
    extends BaseRequestHandler
{
    protected BaseDaoTypedRequestHandler(
            final Class< T > daoType,
            final AuthenticationStrategy authenticationStrategy,
            final CanHandleRequestDeterminer canHandleRequestDeterminer )
    {
        super( authenticationStrategy, canHandleRequestDeterminer );
        m_daoType = daoType;
        Validations.verifyNotNull( "Dao type", m_daoType );
        getCanHandleRequestDeterminer().getQueryStringRequirement().registerDaoType( m_daoType );
    }
    
    
    final protected void registerRequiredBeanProperties(
            final String ... beanPropertyNames )
    {
        getCanHandleRequestDeterminer().getQueryStringRequirement().registerRequiredBeanProperties(
                beanPropertyNames );
    }
    
    
    final protected void registerOptionalBeanProperties( 
            final String ... beanPropertyNames )
    {
        getCanHandleRequestDeterminer().getQueryStringRequirement().registerOptionalBeanProperties(
                beanPropertyNames );
    }
    

    final protected T getBeanSpecifiedViaQueryParameters(
            final CommandExecutionParams params,
            final AutoPopulatePropertiesWithDefaults autoPopulation )
    {
        /*
         * This is safe since we registered the dao type as type T
         */
        @SuppressWarnings( "unchecked" )
        final T retval = (T)
             getCanHandleRequestDeterminer().getQueryStringRequirement().getBeanSpecifiedViaQueryParameters(
                     params, autoPopulation );
        return retval;
    }
    
    
    protected final Class< T > m_daoType;
}
