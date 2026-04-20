/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.PhysicalPlacementApiBean;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

public class BlobApiBean extends BaseMarshalable implements Identifiable
{
    @MarshalXmlAsAttribute
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    public UUID getId()
    {
        return m_id;
    }
    
    public BlobApiBean setId( final UUID value )
    {
        m_id = value;
        return this;
    }
    
    
    public final static String BUCKET = "bucket";

    @SortBy( 1 )
    @MarshalXmlAsAttribute
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    String getBucket()
    {
        return m_bucket;
    }
    
    public BlobApiBean setBucket( final String value )
    {
        m_bucket = value;
        return this;
    }
    
    
    public final static String NAME = "name";
    
    @SortBy( 2 )
    @MarshalXmlAsAttribute
    public String getName()
    {
        return m_name;
    }
    
    public BlobApiBean setName( final String value )
    {
        m_name = value;
        return this;
    }
    
    
    public final static String VERSION_ID = "versionId";
    
    @SortBy( 3 )
    @MarshalXmlAsAttribute
    public UUID getVersionId()
    {
        return m_objectId;
    }
    
    public BlobApiBean setVersionId( final UUID value )
    {
        m_objectId = value;
        return this;
    }
    
    
    public final static String LATEST = "latest";
    
    @MarshalXmlAsAttribute
    public boolean isLatest()
    {
        return m_latest;
    }
    
    public BlobApiBean setLatest( final boolean value )
    {
        m_latest = value;
        return this;
    }
    

    public final static String OFFSET = "offset";
    
    @SortBy( 4 )
    @MarshalXmlAsAttribute
    public long getOffset()
    {
        return m_offset;
    }
    
    public BlobApiBean setOffset( final long value )
    {
        m_offset = value;
        return this;
    }
    

    public final static String LENGTH = "length";
    
    @MarshalXmlAsAttribute
    public long getLength()
    {
        return m_length;
    }
    
    public BlobApiBean setLength( final long value )
    {
        m_length = value;
        return this;
    }
    

    public final static String IN_CACHE = "inCache";
    
    @MarshalXmlAsAttribute
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    public Boolean getInCache()
    {
        return m_inCache;
    }
    
    public BlobApiBean setInCache( final Boolean value )
    {
        m_inCache = value;
        return this;
    }
    

    public final static String PHYSICAL_PLACEMENT = "physicalPlacement";
    
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    public PhysicalPlacementApiBean getPhysicalPlacement()
    {
        return m_physicalPlacement;
    }
    
    public BlobApiBean setPhysicalPlacement( final PhysicalPlacementApiBean physicalPlacement )
    {
        m_physicalPlacement = physicalPlacement;
        return this;
    }
    
    
    private volatile UUID m_id;
    private volatile String m_name;
    private volatile String m_bucket;
    private volatile UUID m_objectId;
    private volatile boolean m_latest;
    private volatile long m_length;
    private volatile long m_offset;
    private volatile Boolean m_inCache;
    private volatile PhysicalPlacementApiBean m_physicalPlacement;
}
