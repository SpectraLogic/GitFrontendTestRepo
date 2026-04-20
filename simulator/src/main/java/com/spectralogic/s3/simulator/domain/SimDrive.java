/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.domain;

import com.spectralogic.s3.common.dao.domain.shared.ElementAddressObservable;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.util.bean.lang.DefaultStringValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface SimDrive 
  extends SimpleBeanSafeToProxy, ErrorMessageObservable< SimDrive >, SerialNumberObservable< SimDrive >,
          ElementAddressObservable< SimDrive >, OnlineObservable< SimDrive >
{
    String PARTITION_SERIAL_NUMBER = "partitionSerialNumber";
    
    String getPartitionSerialNumber();
    
    SimDrive setPartitionSerialNumber( final String value );
    
    
    String TAPE_SERIAL_NUMBER = "tapeSerialNumber";
    
    String getTapeSerialNumber();
    
    SimDrive setTapeSerialNumber( final String value );
    
    
    String TYPE = "type";
    
    TapeDriveType getType();
    
    SimDrive setType( final TapeDriveType value );
    
    
    String MFG_SERIAL_NUMBER = "mfgSerialNumber";
    
    @Optional
    String getMfgSerialNumber();
    
    SimDrive setMfgSerialNumber( final String value );


    String CHARACTERIZATION_VER = "characterizationVer";

    @DefaultStringValue("NotSupported")
    String getCharacterizationVer();

    SimDrive setCharacterizationVer(final String value );
}
