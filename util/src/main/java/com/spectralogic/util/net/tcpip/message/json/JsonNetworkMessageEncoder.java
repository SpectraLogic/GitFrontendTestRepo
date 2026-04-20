/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.json;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageEncoder;

public final class JsonNetworkMessageEncoder implements NetworkMessageEncoder< JsonNetworkMessage >
{
    public byte[] encode( final JsonNetworkMessage message )
    {
        Validations.verifyNotNull( "Message", message );
        if ( message.getJson().indexOf( JsonNetworkMessage.END ) != -1 )
        {
            throw new UnsupportedOperationException( "Json message cannot contain the end of message flag." );
        }
        return ( message.getJson() + JsonNetworkMessage.END ).getBytes();
    }
}
