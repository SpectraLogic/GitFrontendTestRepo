/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.management.ThreadInfo;
import java.util.Set;

public interface DeadlockListener
{
    void deadlockOccurred( final Set< ThreadInfo > deadlockedThreads );
}
