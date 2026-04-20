/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import java.util.concurrent.atomic.AtomicInteger;

public interface TcpIpServerTestPort
{
    AtomicInteger NEXT_PORT = new AtomicInteger( 2999 );
}
