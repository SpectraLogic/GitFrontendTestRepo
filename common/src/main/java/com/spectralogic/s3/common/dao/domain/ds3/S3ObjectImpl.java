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

final class S3ObjectImpl extends BaseMarshalable implements S3Object
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

    
    public String getName()
    {
        return m_name;
    }

    
    public S3Object setName( final String value )
    {
        m_name = value;
        return this;
    }

    
    public S3ObjectType getType()
    {
        return m_type;
    }

    
    public S3Object setType( final S3ObjectType type )
    {
        m_type = type;
        return this;
    }
    
    
    public boolean isLatest()
    {
        return m_latest;
    }
    
    
    public S3Object setLatest( final boolean value )
    {
        m_latest = value;
        return this;
    }

    
    public UUID getBucketId()
    {
        return m_bucketId;
    }

    
    public S3Object setBucketId( final UUID bucketId )
    {
        m_bucketId = bucketId;
        return this;
    }

    
    public Date getCreationDate()
    {
        return m_creationDate;
    }

    
    public S3Object setCreationDate( final Date value )
    {
        m_creationDate = value;
        return this;
    }
    
    
    private volatile UUID m_id;
    private volatile String m_name;
    private volatile S3ObjectType m_type;
    private volatile boolean m_latest;
    private volatile UUID m_bucketId;
    private volatile Date m_creationDate;
}
