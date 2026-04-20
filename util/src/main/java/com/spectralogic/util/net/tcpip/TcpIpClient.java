/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkConnectionClosedException;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessage;
import com.spectralogic.util.shutdown.Shutdownable;

public interface TcpIpClient< M extends NetworkMessage > extends Shutdownable, Runnable
{
    void send( final M message ) throws NetworkConnectionClosedException;
}
