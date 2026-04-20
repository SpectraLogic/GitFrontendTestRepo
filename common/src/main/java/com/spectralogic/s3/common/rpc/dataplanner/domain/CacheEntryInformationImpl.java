/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.util.marshal.BaseMarshalable;

final class CacheEntryInformationImpl extends BaseMarshalable implements CacheEntryInformation
{
    public Blob getBlob()
    {
        return m_blob;
    }
    
    public void setBlob( final Blob value )
    {
        m_blob = value;
    }
    
    
    public CacheEntryState getState()
    {
        return m_state;
    }
    
    public void setState( final CacheEntryState value )
    {
        m_state = value;
    }
    
    
    private volatile Blob m_blob;
    private volatile CacheEntryState m_state;
}
