/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface ElementAddressBlockInformation extends SimpleBeanSafeToProxy
{
    String TYPE = "type";
    
    ElementAddressType getType();
    
    void setType( final ElementAddressType value );
    
    
    String START_ADDRESS = "startAddress";
    
    int getStartAddress();
    
    void setStartAddress( final int value );
    
    
    String END_ADDRESS = "endAddress";
    
    int getEndAddress();
    
    void setEndAddress( final int value );
}
