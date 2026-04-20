/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.spectrads3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.security.ChecksumType;

final class BlobPersistenceImpl extends BaseMarshalable implements BlobPersistence
{
    public UUID getId()
    {
        return m_id;
    }

    
    public Identifiable setId( final UUID value )
    {
        m_id = value;
        return this;
    }

    
    public String getChecksum()
    {
        return m_checksum;
    }

    
    public BlobPersistence setChecksum( final String value )
    {
        m_checksum = value;
        return this;
    }

    
    public ChecksumType getChecksumType()
    {
        return m_checksumType;
    }

    
    public BlobPersistence setChecksumType( final ChecksumType value )
    {
        m_checksumType = value;
        return this;
    }

    
    public boolean isAvailableOnPoolNow()
    {
        return m_availableOnPoolNow;
    }

    
    public void setAvailableOnPoolNow( final boolean value )
    {
        m_availableOnPoolNow = value;
    }

    
    public boolean isAvailableOnTapeNow()
    {
        return m_availableOnTapeNow;
    }

    
    public void setAvailableOnTapeNow( final boolean value )
    {
        m_availableOnTapeNow = value;
    }
    
    
    private volatile UUID m_id;
    private volatile String m_checksum;
    private volatile ChecksumType m_checksumType;
    private volatile boolean m_availableOnPoolNow;
    private volatile boolean m_availableOnTapeNow;
}
