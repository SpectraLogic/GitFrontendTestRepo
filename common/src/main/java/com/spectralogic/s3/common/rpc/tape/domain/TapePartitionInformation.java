/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.ImportExportConfiguration;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface TapePartitionInformation 
  extends SimpleBeanSafeToProxy, SerialNumberObservable< TapePartitionInformation >, 
          ErrorMessageObservable< TapePartitionInformation >, NameObservable< TapePartitionInformation >
{
    String TAPES = "tapes";
    
    @Optional
    BasicTapeInformation [] getTapes();
    
    void setTapes( final BasicTapeInformation [] value );
    
    
    String DRIVES = "drives";
    
    @Optional
    TapeDriveInformation [] getDrives();
    
    void setDrives( final TapeDriveInformation [] value );
    
    
    String ELEMENT_ADDRESS_BLOCKS = "elementAddressBlocks";
    
    /**
     * There will be one or more element address blocks for each element address type.  Only usable element 
     * addresses are to be reported. <br><br>
     * 
     * Note: While there will always be exactly one address block for each element type and that address block
     * will be contiguous, there could be many unusable addresses in that block, which is why one or more
     * element address blocks can be reported for a given element address type.
     */
    ElementAddressBlockInformation [] getElementAddressBlocks();
    
    void setElementAddressBlocks( final ElementAddressBlockInformation [] value );
    
    
    String IMPORT_EXPORT_CONFIGURATION = "importExportConfiguration";
    
    ImportExportConfiguration getImportExportConfiguration();
    
    void setImportExportConfiguration( final ImportExportConfiguration value );
}
