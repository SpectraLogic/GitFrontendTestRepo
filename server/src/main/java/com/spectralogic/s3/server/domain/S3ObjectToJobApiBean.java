package com.spectralogic.s3.server.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

import java.util.UUID;

public class S3ObjectToJobApiBean extends BaseMarshalable implements SimpleBeanSafeToProxy
{
    public final static String NAME = "name";
    
    /**
     * All jobs will specify a name attribute
     */
    @MarshalXmlAsAttribute
    public String getName()
    {
        return m_name;
    }
    
    public S3ObjectToJobApiBean setName( final String value )
    {
        m_name = value;
        return this;
    }
    
    
    public final static String SIZE = "size";
    
    /**
     * PUT jobs will specify a size attribute
     */
    @MarshalXmlAsAttribute
    public Long getSize()
    {
        return m_length;
    }

    public S3ObjectToJobApiBean setSize( final Long value )
    {
        m_length = value;
        return this;
    }
    

    public final static String LENGTH = "length";
    
    /**
     * GET jobs may optionally specify a length attribute
     */
    @MarshalXmlAsAttribute
    public Long getLength()
    {
        return m_length;
    }
    
    public S3ObjectToJobApiBean setLength( final Long value )
    {
        m_length = value;
        return this;
    }
    

    public final static String OFFSET = "offset";
    
    /**
     * GET jobs may optionally specify an offset attribute
     */
    @MarshalXmlAsAttribute
    public Long getOffset()
    {
        return m_offset;
    }
    
    public S3ObjectToJobApiBean setOffset( final Long value )
    {
        m_offset = value;
        return this;
    }
    
    
    public final static String VERSION_ID = "versionId";
    
    /**
     * GET jobs may optionally specify a versionId attribute
     */
    @MarshalXmlAsAttribute
    public UUID getVersionId()
    {
        return m_versionId ;
    }
    
    public S3ObjectToJobApiBean setVersionId( final UUID value )
    {
        m_versionId = value;
        return this;
    }
    
    private volatile String m_name;
    private volatile long m_length;
    private volatile long m_offset;
    private volatile UUID m_versionId;
}
