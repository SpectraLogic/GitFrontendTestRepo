/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.util.bean.lang.Optional;

/**
 * Information about a tape loaded in a tape drive that can be obtained quickly, and regardless as to
 * whetherexp the tape is formatted or not.
 */
public interface LoadedTapeInformation 
    extends BasicTapeInformation, SerialNumberObservable< LoadedTapeInformation >
{
    String TAPE_ID = "tapeId";
    
    /**
     * @return the tape id last written to the tape
     * 
     * Note: If the tape id matches the tape id in our database, that means that we own that tape.  If it does
     * not match, that means that some other appliance took ownership over the tape and the tape is foreign
     * and would have to be imported.  If the tape id is null, no appliance has taken ownership over the tape.
     */
    @Optional
    UUID getTapeId();
    
    void setTapeId( final UUID value );
    
    
    String READ_ONLY = "readOnly";
    
    boolean isReadOnly();
    
    void setReadOnly( final boolean value );


    String CHARACTERIZATION_VER = "characterizationVer";

    String getCharacterizationVer();

    LoadedTapeInformation setCharacterizationVer( final String value );
}
