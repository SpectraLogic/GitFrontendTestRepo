/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.frmwrk;

public interface NetworkMessageHandler< M extends NetworkMessage >
{
    void handle( final M message, final NetworkMessageSender networkMessageSender );
}
