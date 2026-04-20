/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum AutoInspectMode
{
    /**
     * Tapes will never have inspections scheduled automatically for them.  <br><br>
     * 
     * In this mode, it is possible that tapes could have been removed from the library, modified, and re-
     * inserted into the library without this application's knowledge or ability to eventually detect it.  
     * Furthermore, tapes added to the library will not be inspected and become usable automatically.<br><br>
     * 
     * This mode should only be used in system restore workflows where the media with the database backup to
     * restore needs to be inspected, imported, and the backup retrieved so that it can be restored.
     */
    NEVER,
    
    
    /**
     * Tapes will only have inspections scheduled for them if an inspection is necessary.  Specifically, an
     * inspection shall be scheduled if any of the conditions below are true:
     * <ol>
     * <li>The tape has never been inspected
     * <li>The tape had left the library (its state was ejected or lost) and re-appeared later in a storage
     * slot
     * <li>The tape was onlined (it was ejected or lost, re-appeared in import/export, and was onlined using 
     * the online tape command)
     * </ol>
     * 
     * In this mode, it is possible that tapes could have been removed from the library, modified, and re-
     * inserted into the library without this application's knowledge or ability to eventually detect it if,
     * for example, this appliance is shut down, tapes removed from the library and modified, those tapes
     * returned to the library, and this appliance powered back on.
     */
    MINIMAL,
    
    
    /**
     * Tapes will have inspections scheduled for them if an inspection is necessary given the tape's current
     * state, as well as every time the data path starts up.  <br><br>
     * 
     * This mode should be avoided in systems with tons of tapes, since we'll go to inspect them all whenever
     * the data path starts up, and inspecting that many tapes can be problematic (we may induce a failure
     * by performing so many moves, tape mounts, etc.) as this is tape.
     */
    FULL
}
