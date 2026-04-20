/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.domain;

import com.spectralogic.s3.common.dao.domain.shared.ElementAddressObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.bean.lang.DefaultStringValue;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface SimTape 
    extends SimpleBeanSafeToProxy, SerialNumberObservable< SimTape >, ElementAddressObservable< SimTape >,
            OnlineObservable< SimTape >
{
    String HARDWARE_SERIAL_NUMBER = "hardwareSerialNumber";
    
    /**
     * The serial number property will only be populated while the tape is loaded in a tape drive.  While in
     * a tape drive, it will be populated with the immutable hardware serial number.
     */
    String getHardwareSerialNumber();
    
    SimTape setHardwareSerialNumber( final String value );
    
    
    String PARTITION_SERIAL_NUMBER = "partitionSerialNumber";
    
    String getPartitionSerialNumber();
    
    SimTape setPartitionSerialNumber( final String value );
    
    
    String BAR_CODE = "barCode";
    
    String getBarCode();
    
    SimTape setBarCode( final String value );
    
    
    String TYPE = "type";
    
    TapeType getType();
    
    SimTape setType( final TapeType value );
    
    
    String TOTAL_RAW_CAPACITY = "totalRawCapacity";
    
    long getTotalRawCapacity();
    
    SimTape setTotalRawCapacity( final long value );
    
    
    String AVAILABLE_RAW_CAPACITY = "availableRawCapacity";

    long getAvailableRawCapacity();
    
    SimTape setAvailableRawCapacity( final long value );
    
    
    String CONTAINS_FOREIGN_DATA = "containsForeignData";
    
    boolean isContainsForeignData();
    
    SimTape setContainsForeignData( final boolean value );
    

    String QUIESCE_REQUIRED = "quiesceRequired";
    
    boolean isQuiesceRequired();
    
    SimTape setQuiesceRequired( final boolean value );
    

    String REMOVAL_PREPARED = "removalPrepared";
    
    boolean isRemovalPrepared();
    
    SimTape setRemovalPrepared( final boolean value );
    
    
    String READ_ONLY = "readOnly";
    
    boolean isReadOnly();
    
    SimTape setReadOnly( final boolean value );


    String CHARACTERIZATION_VER = "characterizationVer";

    @DefaultStringValue("NotSupported")
    String getCharacterizationVer();

    SimTape setCharacterizationVer(final String value );
}
