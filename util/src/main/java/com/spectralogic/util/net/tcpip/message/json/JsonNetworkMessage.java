/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.json;

import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessage;

public final class JsonNetworkMessage implements NetworkMessage
{
    final static char END = '\0';
    
    
    public JsonNetworkMessage( final String json )
    {
        m_json = json;
    }
    
    
    public String getJson()
    {
        return m_json;
    }
    
    
    private final String m_json;
}
