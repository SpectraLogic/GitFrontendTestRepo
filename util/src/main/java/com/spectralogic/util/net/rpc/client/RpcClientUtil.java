/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.net.rpc.frmwrk.RpcLogger;

final class RpcClientUtil
{
    private RpcClientUtil()
    {
        // singleton
    }
    
    
    static void logSuccess( 
            final Logger logger,
            final ClientRpcFuture future,
            String response )
    {
        String msg = "[" + future.getRpcInvokerThreadName() + "] " + future.getRequestDescription() 
                + " completed successfully after " + future.getDuration();
        if ( null == response )
        {
            response = "null";
        }
        msg += ( ( 150 >= response.length() ) ? " with response: " + response : "." );
        
        RpcLogger.CLIENT_LOG.info( msg );
        logger.info( msg );
    }
    
    
    static void logFailure(
            final Logger logger,
            final ClientRpcFuture future,
            final String longMessage,
            final String code,
            final int httpResponseCode )
    {
        final Level failureLogLevel = RpcLogger.getLogLevelForHttpResponseCode( httpResponseCode );
        final String shortMessage = ( null == longMessage || 120 > longMessage.length() ) ? 
                longMessage
                : longMessage.substring( 0, 115 ) + "...";
        final String messagePrefix =
                "[" + future.getRpcInvokerThreadName() + "] "
                + future.getRequestDescription()
                + " FAILED after " + future.getDuration() + ": " 
                + Platform.NEWLINE + code + " / HTTP " + httpResponseCode
                + Platform.NEWLINE;
        RpcLogger.CLIENT_LOG.log( failureLogLevel, messagePrefix + LogUtil.getShortVersion( longMessage ) );
        logger.log( failureLogLevel, messagePrefix + shortMessage );
    }
}
