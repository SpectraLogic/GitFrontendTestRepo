/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.frmwrk;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.NamingConventionType;

public final class RpcLogger
{
    private RpcLogger()
    {
        // do not instantiate
    }
    
    
    public static String getDescriptionForRequest( 
            final long requestId, 
            final String resourceTypeName, 
            final String instanceName,
            final String resourceMethodName )
    {
        return "RPC " 
               + getResourceName( resourceTypeName, instanceName )
               + "."
               + NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( resourceMethodName )
               + "<" + requestId + ">";
    }
    
    
    public static String getResourceName(
            final String resourceTypeName,
            final String instanceName )
    {
        final String instancePart = ( null == instanceName || instanceName.isEmpty() ) ? 
                "" 
                : "$" + instanceName;
        return NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( resourceTypeName ) 
               + instancePart;
    }
    
    
    public static Level getLogLevelForHttpResponseCode( final int httpResponseCode )
    {
        if ( GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT.getHttpResponseCode() == httpResponseCode
             || GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT.getHttpResponseCode() == httpResponseCode
             || GenericFailure.NOT_FOUND.getHttpResponseCode() == httpResponseCode )
        //NOTE: NOT_FOUND errors are normal when waiting for a resource to come online
        {
            return Level.INFO;
        }
        return Level.WARN;
    }
    

    public final static Logger CLIENT_LOG = Logger.getLogger( RpcLogger.class.getName() + ".Client" );
    public final static Logger SERVER_LOG = Logger.getLogger( RpcLogger.class.getName() + ".Server" );
}
