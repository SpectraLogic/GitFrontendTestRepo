/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.frmwrk;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.List;

public interface NetworkMessageDecoder< M extends NetworkMessage >
{
    List< M > decode( final Channel channel, final ByteBuf in );
}
