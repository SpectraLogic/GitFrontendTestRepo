/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import com.spectralogic.util.bean.lang.ExcludeFromDocumentation;
import com.spectralogic.util.db.lang.shared.ExcludeFromDatabasePersistence;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;

import java.util.HashSet;
import java.util.Set;

public enum TapeType
{
    /**
     * A Spectra LTO5 data tape.
     */
    LTO5( true, 1500L * 1000 * 1000 * 1000 ), /*1.5 TB*/
    
    
    /**s
     * A Spectra LTO6 data tape.
     */
    LTO6( true, 2500L * 1000 * 1000 * 1000 ), /*2.5 TB*/
    
    
    /**
     * A Spectra LTO7 data tape.
     */
    LTO7( true, 6000L * 1000 * 1000 * 1000 ), /*6 TB*/

    
    /**
    * A Spectra LTOM8 data tape.
    */
    LTOM8( true, 9000L * 1000 * 1000 * 1000 ), /*9 TB*/

    
    /**
     * A Spectra LTO8 data tape.
     */
    LTO8( true, 12000L * 1000 * 1000 * 1000 ), /*12 TB*/


    /**
     * A Spectra LTO9 data tape.
     */
    LTO9( true, 18000L * 1000 * 1000 * 1000 ), /*18 TB*/

    /**
     * A Spectra LTO10 data tape.
     */
    LTO10( true, 30000L * 1000 * 1000 * 1000 ), /*30 TB*/


    /**
     * A Spectra LTO10 data tape.
     */
    LTO10P( true, 40000L * 1000 * 1000 * 1000 ), /*30 TB*/
    
    
    /**
     * A Spectra LTO cleaning tape (to clean tape drives).
     */
    LTO_CLEANING_TAPE( false, null ),

    
    /**
     * A Spectra TS tape.  The largest tape a TS1140 supports.  Can be formatted in a lower-density format
     * compatible with TS1140 and TS1150 drives or in a higher-density format compatible only with TS1150
     * drives.
     */
    TS_JC( true, 7000L * 1000 * 1000 * 1000 ), /*7 TB*/ 

    
    /**
     * Not supported.  The largest WORM tape a TS1140 supports.  Can be formatted in a lower-density format
     * compatible with TS1140 and TS1150 drives or in a higher-density format compatible only with TS1150
     * drives. 
     */
    TS_JY( false, 7000L * 1000 * 1000 * 1000 ), /*7 TB*/    

    
    /**
     * A Spectra TS tape.  The smaller tape a TS1140 supports.  Can be formatted in a lower-density format
     * compatible with TS1140 and TS1150 drives or in a higher-density format compatible only with TS1150
     * drives.
     */
    TS_JK( true, 900L * 1000 * 1000 * 1000 ), /*900 GB*/

    
    /**
     * A Spectra TS tape.  The largest tape a TS1150 supports.  Not supported by a TS1140 drive.
     */
    TS_JD( true, 15000L * 1000 * 1000 * 1000 ), /*15 TB*/

    
    /**
     * Not supported.  The WORM tape a TS1150 supports.  Not supported by a TS1140 drive.
     */
    TS_JZ( false, 15000L * 1000 * 1000 * 1000 ), /*15 TB*/

    
    /**
     * A Spectra TS tape.  The smaller tape a TS1150 supports.  Not supported by a TS1140 drive.
     */
    TS_JL( true, 3000L * 1000 * 1000 * 1000 ), /*3 TB*/
    
    
    /**
     * A Spectra TS tape.  The smaller tape a TS1160 supports.  Not supported by a TS1155 drive.
     */
    TS_JM( true, 5000L * 1000 * 1000 * 1000 ), /*5 TB*/
    
    /**
     * A Spectra TS tape.  The largest tape a TS1160 supports.  Not supported by a TS1155 drive.
     */
    TS_JE( true, 20000L * 1000 * 1000 * 1000 ), /*20 TB*/
    
    /**
     * Not supported.  The WORM tape a TS1160 supports.  Not supported by a TS1155 drive.
     */
    TS_JV( false, 20000L * 1000 * 1000 * 1000 ), /*20 TB*/


    /**
     * A Spectra TS tape. The largest tape a TS1170 supports.  Not supported by TS1160 drive.
     */
    TS_JF( true, 50000L * 1000 * 1000 * 1000 ), /*50 TB*/


    /**
     * A Spectra TS tape. The smaller tape a TS1170 supports.  Not supported by a TS1160 drive.
     */
    TS_JN( true, 10000L * 1000 * 1000 * 1000 ), /*10 TB*/


    /**
     * A Spectra TS cleaning tape (to clean tape drives).
     */
    TS_CLEANING_TAPE( false, null ),
    
    
    /**
     * An unknown tape that can contain data.  If the tape type cannot be determined, the type should be
     * reported as UNKNOWN.
     */
    UNKNOWN( true, null ),
    
    
    /**
     * An unsupported or otherwise disallowed tape that cannot contain data (and thus, is forbidden from 
     * being used).  If the tape type can be determined and is not a tape type defined in this enumeration,
     * the type is not supported and should be reported as FORBIDDEN.
     */
    FORBIDDEN( false, null ),
    ;
    
    
    private TapeType( final boolean canContainData, final Long maxCapacity )
    {
        m_canContainData = canContainData;
        m_maxCapacity = maxCapacity;
        m_threshold = 95;
        m_minimumThreshold = 10;
    }
    
    
    public boolean canContainData()
    {
        return m_canContainData;
    }
    
    
    public Integer getDefaultAutoCompactionThreshold()
    {
        return null == m_threshold ? null : m_threshold;
    }

    public Integer getMinimumAutoCompactionThreshold()
    {
        return null == m_minimumThreshold ? null : m_minimumThreshold;
    }

    public Long getAutoCompactionThresholdInBytesFromPercentage( Integer percent )
    {
        return null == percent ? null : ( percent * m_maxCapacity ) / 100;
    }

    public static Set< TapeType > getDataContainingTypes()
    {
        final Set< TapeType > retval = new HashSet<>();
        for ( final TapeType type : values() )
        {
            if ( type.canContainData() )
            {
                retval.add( type );
            }
        }
        
        return retval;
    }

    @ExcludeFromMarshaler(ExcludeFromMarshaler.When.ALWAYS)
    @ExcludeFromDatabasePersistence
    @ExcludeFromDocumentation
    public Long getMaxCapacity() {
        return m_maxCapacity;
    }

    private final boolean m_canContainData;
    private final Long m_maxCapacity;
    private final Integer m_threshold;
    private final Integer m_minimumThreshold;
}
