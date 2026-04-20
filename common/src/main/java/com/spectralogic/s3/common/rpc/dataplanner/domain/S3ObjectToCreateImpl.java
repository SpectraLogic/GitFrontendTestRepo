/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.util.marshal.BaseMarshalable;

final class S3ObjectToCreateImpl extends BaseMarshalable implements S3ObjectToCreate
{
    public String getName()
    {
        return m_name;
    }

    
    public S3ObjectToCreate setName( final String value )
    {
        m_name = value;
        return this;
    }

    
    public long getSizeInBytes()
    {
        return m_sizeInBytes;
    }

    
    public S3ObjectToCreate setSizeInBytes( final long value )
    {
        m_sizeInBytes = value;
        return this;
    }

    
    private volatile String m_name;
    private volatile long m_sizeInBytes;
}
