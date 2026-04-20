/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.frmwrk;

public final class NetworkConnectionClosedException extends Exception
{
    public NetworkConnectionClosedException( final String msg, final Throwable cause )
    {
        super( msg, cause );
    }
}
