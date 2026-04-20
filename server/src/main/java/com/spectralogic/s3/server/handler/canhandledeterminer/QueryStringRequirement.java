/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import java.util.List;

import com.spectralogic.s3.server.handler.canhandledeterminer.RequestHandlerRequestContract.RequestHandlerParamContract;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface QueryStringRequirement extends Requirement
{
    QueryStringRequirement registerOptionalRequestParameters(
            final RequestParameterType ... requestParameters );
    
    
    QueryStringRequirement registerOptionalBeanProperties( 
            final String ... beanPropertyNames );
    
    
    QueryStringRequirement registerRequiredRequestParameters( 
            final RequestParameterType ... requestParameters );
    
    
    QueryStringRequirement registerRequiredBeanProperties( 
            final String ... beanPropertyNames );
    
    
    QueryStringRequirement registerDaoType( final Class< ? extends SimpleBeanSafeToProxy > daoType );
    
    
    List< RequestHandlerParamContract > getParamsContract( final boolean required );
    
    
    public enum AutoPopulatePropertiesWithDefaults
    {
        YES,
        NO
    }
    

    Object getBeanSpecifiedViaQueryParameters(
            final CommandExecutionParams params,
            final AutoPopulatePropertiesWithDefaults autoPopulation );
}
