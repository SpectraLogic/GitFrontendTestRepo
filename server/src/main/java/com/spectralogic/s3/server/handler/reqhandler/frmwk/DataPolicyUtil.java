/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.security.DataPolicyAccessRequest;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DataPolicyAuthorization;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.exception.GenericFailure;

public final class DataPolicyUtil
{
    private DataPolicyUtil()
    {
        // singleton
    }

    
    public static UUID getDataPolicy( final CommandExecutionParams params, final UUID specifiedDataPolicyId )
    {
        if ( null != specifiedDataPolicyId )
        {
            return verifyDataPolicyAccess( params, specifiedDataPolicyId );
        }

        final User user = params.getRequest().getAuthorization().getUser();
        return getDataPolicy( params, user );
    }
    
    
    public static UUID getDataPolicy( final CommandExecutionParams params, final User user )
    {
        final UUID defaultId = user.getDefaultDataPolicyId();
        if ( null != defaultId )
        {
            return verifyDataPolicyAccess( params, defaultId );
        }
        
        final Set< UUID > dataPolicies = BeanUtils.extractPropertyValues( 
                params.getServiceManager().getRetriever( DataPolicy.class ).retrieveAll().toSet(),
                Identifiable.ID );
        final int numDataPolicies = dataPolicies.size();
        for ( final UUID id : new HashSet<>( dataPolicies ) )
        {
            if ( !params.getDataPolicyAclAuthorizationService().hasAccess( 
                    new DataPolicyAccessRequest( user.getId(), id ) ) )
            {
                dataPolicies.remove( id );
            }
        }
        if ( 1 == dataPolicies.size() )
        {
            return verifyDataPolicyAccess( params, dataPolicies.iterator().next() );
        }
        
        if ( dataPolicies.isEmpty() )
        {
            throw new S3RestException( 
                    GenericFailure.CONFLICT,
                    "User " + user.getName() + " does not have access to use any data policies (there are " 
                    + numDataPolicies + " data policies currently defined)." );
        }
        throw new S3RestException( 
                GenericFailure.BAD_REQUEST,
                "User " + user.getName() + " has access to use " + dataPolicies.size() 
                + " data policies.  Since the desired data policy was not specified and " + user.getName() 
                + " does not have a default data policy configured, the data policy cannot be inferred." );
    }
    
    
    private static UUID verifyDataPolicyAccess( final CommandExecutionParams params, final UUID dataPolicyId )
    {
        DataPolicyAuthorization.verify( params, dataPolicyId );
        return dataPolicyId;
    }
}
