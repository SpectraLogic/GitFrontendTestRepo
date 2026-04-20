/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.HashSet;
import java.util.Set;

import com.spectralogic.util.lang.CollectionFactory;

public enum TapeDriveType
{
    UNKNOWN( TapeType.LTO_CLEANING_TAPE,
             new HashSet< TapeType >(),
             new HashSet< TapeType >() ),
    LTO5( TapeType.LTO_CLEANING_TAPE,
          CollectionFactory.toSet( TapeType.LTO5 ),
          CollectionFactory.toSet( TapeType.LTO5 ) ),
    LTO6( TapeType.LTO_CLEANING_TAPE,
          CollectionFactory.toSet( TapeType.LTO5, TapeType.LTO6 ),
          CollectionFactory.toSet( TapeType.LTO5, TapeType.LTO6 ) ),
    LTO7( TapeType.LTO_CLEANING_TAPE,
          CollectionFactory.toSet( TapeType.LTO5, TapeType.LTO6, TapeType.LTO7 ),
          CollectionFactory.toSet( TapeType.LTO6, TapeType.LTO7 ) ),
    LTO8( TapeType.LTO_CLEANING_TAPE,
            CollectionFactory.toSet( TapeType.LTO7, TapeType.LTOM8, TapeType.LTO8 ),
            CollectionFactory.toSet( TapeType.LTO7, TapeType.LTOM8, TapeType.LTO8 ) ),
    LTO9( TapeType.LTO_CLEANING_TAPE,
          CollectionFactory.toSet( TapeType.LTO8, TapeType.LTO9 ),
          CollectionFactory.toSet( TapeType.LTO8, TapeType.LTO9 ) ),
    LTO10( TapeType.LTO_CLEANING_TAPE,
            CollectionFactory.toSet( TapeType.LTO10, TapeType.LTO10P ),
            CollectionFactory.toSet( TapeType.LTO10, TapeType.LTO10P ) ),
    TS1140( TapeType.TS_CLEANING_TAPE,
            CollectionFactory.toSet( TapeType.TS_JC, TapeType.TS_JK ),
            CollectionFactory.toSet( TapeType.TS_JC, TapeType.TS_JK ) ),
    TS1150( TapeType.TS_CLEANING_TAPE,
            CollectionFactory.toSet( TapeType.TS_JC, TapeType.TS_JD, TapeType.TS_JK, TapeType.TS_JL ),
            CollectionFactory.toSet( TapeType.TS_JC, TapeType.TS_JD, TapeType.TS_JK, TapeType.TS_JL ) ),
    TS1155( TapeType.TS_CLEANING_TAPE,
            CollectionFactory.toSet( TapeType.TS_JC, TapeType.TS_JD, TapeType.TS_JK, TapeType.TS_JL ),
            CollectionFactory.toSet( TapeType.TS_JC, TapeType.TS_JD, TapeType.TS_JK, TapeType.TS_JL ) ),
    TS1160( TapeType.TS_CLEANING_TAPE,
    		CollectionFactory.toSet( TapeType.TS_JC, TapeType.TS_JD, TapeType.TS_JK, TapeType.TS_JL, TapeType.TS_JM, TapeType.TS_JE ),
            CollectionFactory.toSet( TapeType.TS_JC, TapeType.TS_JD, TapeType.TS_JK, TapeType.TS_JL, TapeType.TS_JM, TapeType.TS_JE ) ),
    TS1170( TapeType.TS_CLEANING_TAPE,
            CollectionFactory.toSet( TapeType.TS_JF, TapeType.TS_JN),
            CollectionFactory.toSet( TapeType.TS_JF, TapeType.TS_JN) )
    ;
    
    
    private TapeDriveType( 
            final TapeType cleaningTape,
            final Set< TapeType > tapeTypesSupportedForReads,
            final Set< TapeType > tapeTypesSupportedForWrites )
    {
        tapeTypesSupportedForReads.add( TapeType.UNKNOWN );
        tapeTypesSupportedForReads.add( TapeType.FORBIDDEN );
        tapeTypesSupportedForReads.add( cleaningTape );
        m_cleaningTape = cleaningTape;
        m_tapeTypesSupportedForReads = tapeTypesSupportedForReads;
        m_tapeTypesSupportedForWrites = tapeTypesSupportedForWrites;
    }
    
    
    public boolean isReadSupported( final TapeType tapeType )
    {
        return m_tapeTypesSupportedForReads.contains( tapeType );
    }
    
    
    public boolean isWriteSupported( final TapeType tapeType )
    {
        return m_tapeTypesSupportedForWrites.contains( tapeType );
    }
    
    
    public Set< TapeType > getSupportedTapeTypes()
    {
        return new HashSet<>( m_tapeTypesSupportedForReads );
    }
    
    
    public TapeType getCleaningTapeType()
    {
        return m_cleaningTape;
    }
    
    
    private final TapeType m_cleaningTape;
    private final Set< TapeType > m_tapeTypesSupportedForReads;
    private final Set< TapeType > m_tapeTypesSupportedForWrites;
}
