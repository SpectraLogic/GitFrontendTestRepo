/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.frmwrk;

public interface NetworkMessageEncoder< M extends NetworkMessage >
{
    byte [] encode( final M message );
}
