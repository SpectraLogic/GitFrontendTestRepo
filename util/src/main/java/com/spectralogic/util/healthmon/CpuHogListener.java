/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.management.ThreadInfo;
import java.util.Map;

public interface CpuHogListener
{
    /**
     * @param cpuHoggingThreads - {@code Map <thread, milliseconds of CPU time>}
     */
    void cpuHogOccurred( final Map< ThreadInfo, Integer > cpuHoggingThreads );
}
