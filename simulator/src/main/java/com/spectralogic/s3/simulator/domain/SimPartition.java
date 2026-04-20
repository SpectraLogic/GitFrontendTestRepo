/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.domain;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface SimPartition 
  extends SimpleBeanSafeToProxy, SerialNumberObservable< SimPartition >, 
          ErrorMessageObservable< SimPartition >, OnlineObservable< SimPartition >
{
    String LIBRARY_SERIAL_NUMBER = "librarySerialNumber";
    
    String getLibrarySerialNumber();
    
    SimPartition setLibrarySerialNumber( final String value );

    String DRIVE_TYPE = "driveType";
    TapeDriveType getDriveType();

    SimPartition setDriveType(final TapeDriveType value);
}
