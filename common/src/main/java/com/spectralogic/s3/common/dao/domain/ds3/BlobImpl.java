/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.security.ChecksumType;

final class BlobImpl extends BaseMarshalable implements Blob
{
    public UUID getId()
    {
        return m_id;
    }

    
    public DatabasePersistable setId( final UUID id )
    {
        m_id = id;
        return this;
    }
    
    
    public UUID getObjectId()
    {
        return m_objectId;
    }

    
    public Blob setObjectId( final UUID value )
    {
        m_objectId = value;
        return this;
    }

    
    public long getByteOffset()
    {
        return m_byteOffset;
    }

    
    public Blob setByteOffset( final long value )
    {
        m_byteOffset = value;
        return this;
    }

    
    public long getLength()
    {
        return m_length;
    }

    
    public Blob setLength( final long value )
    {
        m_length = value;
        return this;
    }

    
    public String getChecksum()
    {
        return m_checksum;
    }

    
    public Blob setChecksum( final String value )
    {
        m_checksum = value;
        return this;
    }
    
    
    public ChecksumType getChecksumType()
    {
        return m_checksumType;
    }
    
    
    public Blob setChecksumType( final ChecksumType value )
    {
        m_checksumType = value;
        return this;
    }
    
    
    public Date getCreationDate()
    {
        return m_creationDate;
    }
    
    
    public Blob setCreationDate( final Date value )
    {
        m_creationDate = value;
        return this;
    }


    private volatile UUID m_id;
    private volatile UUID m_objectId;
    private volatile long m_byteOffset;
    private volatile long m_length;
    private volatile String m_checksum;
    private volatile ChecksumType m_checksumType;
    private volatile Date m_creationDate;
}
