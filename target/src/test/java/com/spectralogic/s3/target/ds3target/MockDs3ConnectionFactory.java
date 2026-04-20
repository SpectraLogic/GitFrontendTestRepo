/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.ds3target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.util.lang.Validations;

public final class MockDs3ConnectionFactory implements Ds3ConnectionFactory
{
    public MockDs3ConnectionFactory( final MockDs3Client mockClient )
    {
        m_mockClient = mockClient;
        Validations.verifyNotNull( "Mock client", m_mockClient );
    }
    
    
    public Ds3Connection discoverWithoutAdministratorAccess( final Ds3Target target )
    {
        m_mockClient.clearShutdown();
        return new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", m_mockClient.getClient() );
    }

    
    public Ds3Connection discover( final Ds3Target target )
    {
        m_mockClient.clearShutdown();
        return new Ds3ConnectionImpl( m_sourceInstanceId, target, "YQ==", m_mockClient.getClient() );
    }


    public Ds3Connection connect( final UUID userId, final Ds3Target target )
    {
        m_mockClient.clearShutdown();
        return new Ds3ConnectionImpl( m_sourceInstanceId, target, "YQ==", m_mockClient.getClient() );
    }
    
    
    public UUID getSourceInstanceId()
    {
        return m_sourceInstanceId;
    }

    
    private final MockDs3Client m_mockClient;
    private final UUID m_sourceInstanceId = UUID.randomUUID();
}
