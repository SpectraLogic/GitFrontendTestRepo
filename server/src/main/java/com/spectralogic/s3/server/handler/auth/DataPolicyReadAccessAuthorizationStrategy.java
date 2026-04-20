/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.auth;

import java.util.Map;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;

/**
 * An {@link AuthenticationStrategy} for {@link DataPolicy}s where the user is required to have read access
 * to use or view the data policy.
 */
public final class DataPolicyReadAccessAuthorizationStrategy implements AuthenticationStrategy
{
    public void authenticate( final CommandExecutionParams commandExecutionParams )
    {
        final Map< String, String > beanProperties =
                commandExecutionParams.getRequest().getBeanPropertyValueMapFromRequestParameters();
        final DataPolicy dataPolicy;
        if ( beanProperties.containsKey( Bucket.DATA_POLICY_ID ) )
        {
            dataPolicy = commandExecutionParams.getServiceManager().getRetriever( DataPolicy.class ).discover(
                    beanProperties.get( Bucket.DATA_POLICY_ID ) );
        }
        else
        {
            dataPolicy = commandExecutionParams.getRequest().getRestRequest().getBean(
                    commandExecutionParams.getServiceManager().getRetriever( DataPolicy.class ) );
        }
        
        DataPolicyAuthorization.verify(
                commandExecutionParams,
                dataPolicy.getId() );
    }
}
