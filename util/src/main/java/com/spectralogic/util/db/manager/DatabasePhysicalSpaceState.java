/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager;

public enum DatabasePhysicalSpaceState
{
    CRITICAL( 0.05 ),
    LOW( 0.1 ),
    NEAR_LOW( 0.2 ),
    NORMAL( 1 ),
    ;
    
    
    private DatabasePhysicalSpaceState( final double freeSpaceRatioToReachThreshold )
    {
        m_freeSpaceRatioToReachThreshold = freeSpaceRatioToReachThreshold;
    }
    
    
    public double getFreeSpaceRatioToReachThreshold()
    {
        return m_freeSpaceRatioToReachThreshold;
    }
    
    
    public static DatabasePhysicalSpaceState getFreeToTotalDiskSpaceRatioState( final double ratio )
    {
        for ( final DatabasePhysicalSpaceState state : values() )
        {
            if ( ratio <= state.getFreeSpaceRatioToReachThreshold() )
            {
                return state;
            }
        }
        
        throw new IllegalArgumentException( "Bad input: " + ratio );
    }
    
    
    private final double m_freeSpaceRatioToReachThreshold;
}
