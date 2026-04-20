/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.auth;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.platform.security.DataPolicyAccessRequest;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

public final class DataPolicyAuthorization
{
    public static void verify( 
            final CommandExecutionParams commandExecutionParams, 
            final UUID dataPolicyId )
    {
        Validations.verifyNotNull( "Command execution params", commandExecutionParams );
        Validations.verifyNotNull( "Data policy", dataPolicyId );
        
        if ( INTERNAL_ACCESS_ONLY.isRequestInternal( commandExecutionParams.getRequest().getHttpRequest() ) )
        {
            return;
        }
        
        commandExecutionParams.getDataPolicyAclAuthorizationService().verifyHasAccess(
                new DataPolicyAccessRequest( 
                        commandExecutionParams.getRequest().getAuthorization().getUser().getId(), 
                        dataPolicyId ) );
    }
    
    
    public static Set< UUID > getDataPoliciesUserHasAccessTo(
            final CommandExecutionParams commandExecutionParams )
    {
        final boolean internalRequest = 
           ( INTERNAL_ACCESS_ONLY.isRequestInternal( commandExecutionParams.getRequest().getHttpRequest() ) );
        
        final Set< UUID > retval = new HashSet<>();
        try
        {
	        commandExecutionParams.getServiceManager().reserveConnections( 0, 2 );
	        try ( final EnhancedIterable< DataPolicy > iterable =
	                commandExecutionParams.getServiceManager().getRetriever( DataPolicy.class ).retrieveAll( 
	                        Require.nothing() ).toIterable() )
	        {
	            for ( final DataPolicy dataPolicy : iterable )
	            {
	                if ( internalRequest )
	                {
	                    retval.add( dataPolicy.getId() );
	                }
	                else if ( commandExecutionParams.getDataPolicyAclAuthorizationService().hasAccess(
	                        new DataPolicyAccessRequest( 
	                                commandExecutionParams.getRequest().getAuthorization().getUser().getId(), 
	                                dataPolicy.getId() ) ) )
	                {
	                    retval.add( dataPolicy.getId() );
	                }
	            }
	        }
        }
        finally
        {
        	commandExecutionParams.getServiceManager().releaseReservedConnections();
        }
        
        return retval;
    }
    
    
    private final static InternalAccessOnlyAuthenticationStrategy INTERNAL_ACCESS_ONLY = 
            new InternalAccessOnlyAuthenticationStrategy();
}
