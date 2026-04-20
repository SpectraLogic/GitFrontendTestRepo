/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.s3.common.dao.domain.shared.ElementAddressObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

/**
 * Note that the tape serial number is the only immutable, reliable identifier we have on a tape.  The tape
 * serial number can only be determined while the tape is in a tape drive, so it will not be provided in the
 * basic tape information payload.  The serial number reported must be a concatenation of the vendor id and
 * manufacturer serial number to guarantee uniqueness across all vendors / manufacturers.  Both of these
 * attributes are burned in and immutable at the time that the tape is manufactured.  <br><br>
 * 
 * I/O operations performed against the tape library work with element addresses.  The bar code of a tape is 
 * NOT guaranteed to be unique in a partition.  The bar code, element address, and tape type are the only 
 * attributes that are known and will be reported on an inventory without having to load the tape into a 
 * drive. <br><br>
 * 
 * Warning: If the bar code is not correct or is customer-supplied since the customer bought their own tapes,
 * then the tape type we determine may not be correct.
 */
public interface BasicTapeInformation 
    extends SimpleBeanSafeToProxy, ElementAddressObservable< BasicTapeInformation >
{
    String BAR_CODE = "barCode";
    
    /**
     * Always to be populated, unless the tape is in a tape drive and we don't know anything about that tape 
     * being moved into that tape drive, or if the tape does not have a bar code.
     */
    @Optional
    String getBarCode();
    
    void setBarCode( final String value );
    
    
    String TYPE = "type";

    /**
     * When returned as part of a {@link BasicTapeInformation} where the tape is not necessarily in a tape 
     * drive, the type is determined by the bar code.  If the tape does not have a bar code, 
     * {@link TapeType#FORBIDDEN} shall be returned.  If the tape has a bar code, but it's not a Spectra one, 
     * {@link TapeType#UNKNOWN} shall be returned.  If the tape has a Spectra bar code, the 
     * appropriate {@link TapeType} enum shall be returned denoting what type of tape it is.  <br><br>
     * 
     * When returned as part of a subclass of {@link BasicTapeInformation} where the tape is in a tape drive,
     * the type is determined by inspecting the tape.  If the type cannot be determined by inspecting the
     * tape, {@link TapeType#FORBIDDEN} shall be returned.  Otherwise, the appropriate {@link TapeType} enum 
     * shall be returned denoting what type of tape it is.
     */
    TapeType getType();
    
    void setType( final TapeType value );
}
