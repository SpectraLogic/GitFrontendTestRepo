/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface TapeLibraryInformation 
    extends SimpleBeanSafeToProxy, SerialNumberObservable< TapeLibraryInformation >, 
            NameObservable< TapeLibraryInformation >
{
    String PARTITIONS = "partitions";
    
    TapePartitionInformation [] getPartitions();
    
    TapeLibraryInformation setPartitions( final TapePartitionInformation [] value );
    
    
    String MANAGEMENT_URL = "managementUrl";
    
    String getManagementUrl();
    
    TapeLibraryInformation setManagementUrl( final String value );
    
    
    String getName();
}
