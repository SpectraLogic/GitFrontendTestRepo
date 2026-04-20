/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import java.util.UUID;

import com.spectralogic.util.lang.Validations;

public final class DataPolicyAccessRequest
{
    public DataPolicyAccessRequest( final UUID userId, final UUID dataPolicyId )
    {
        m_userId = userId;
        m_dataPolicyId = dataPolicyId;
        Validations.verifyNotNull( "User id", m_userId );
        Validations.verifyNotNull( "Bucket id", m_dataPolicyId );
    }
    
    
    public UUID getUserId()
    {
        return m_userId;
    }
    
    
    public UUID getDataPolicyId()
    {
        return m_dataPolicyId;
    }
    
    
    @Override
    public int hashCode()
    {
        return m_userId.hashCode() + m_dataPolicyId.hashCode();
    }
    
    
    @Override
    public boolean equals( final Object other )
    {
        if ( this == other )
        {
            return true;
        }
        if ( null == other )
        {
            return false;
        }
        if ( ! ( other instanceof DataPolicyAccessRequest ) )
        {
            return false;
        }
        
        final DataPolicyAccessRequest oth = (DataPolicyAccessRequest)other;
        return ( m_userId.equals( oth.m_userId ) 
                && m_dataPolicyId.equals( oth.m_dataPolicyId ) );
    }


    private final UUID m_userId;
    private final UUID m_dataPolicyId;
}
