/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.security.ChecksumType;

public interface BlobOnMedia extends SimpleBeanSafeToProxy, ChecksumObservable< BlobOnMedia >, Identifiable
{
    String OFFSET = "offset";
    
    long getOffset();
    
    void setOffset( final long value );
    
    
    String LENGTH = "length";
    
    long getLength();
    
    void setLength( final long value );
    
    
    String getChecksum();
    
    ChecksumType getChecksumType();
}
