/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.shared.ReservedTaskType;
import com.spectralogic.s3.common.rpc.tape.domain.TapeDriveInformation;
import com.spectralogic.util.bean.lang.*;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique( SerialNumberObservable.SERIAL_NUMBER )
})
@Indexes( { @Index( TapeDrive.STATE ), @Index( TapeDrive.TYPE ) } )
public interface TapeDrive
    extends DatabasePersistable, SerialNumberObservable< TapeDrive >, ErrorMessageObservable< TapeDrive >
{
    String PARTITION_ID = "partitionId";
    
    @CascadeDelete
    @References( TapePartition.class )
    UUID getPartitionId();
    
    TapeDrive setPartitionId( final UUID value );
    
    
    String TAPE_ID = "tapeId";
    
    @Optional
    @References( Tape.class )
    UUID getTapeId();
    
    TapeDrive setTapeId( final UUID value );
    
    
    String TYPE = "type";
    
    @SortBy( 2 )
    TapeDriveType getType();
    
    TapeDrive setType( final TapeDriveType value );
    
    
    String STATE = "state";
    
    @DefaultEnumValue( "NORMAL" )
    @SortBy( 1 )
    TapeDriveState getState();
    
    TapeDrive setState( final TapeDriveState value );
    
    
    String QUIESCED = "quiesced";

    @DefaultEnumValue( "NO" )
    Quiesced getQuiesced();
    
    TapeDrive setQuiesced( final Quiesced value );
    
    
    String LAST_CLEANED = "lastCleaned";
    
    @Optional
    Date getLastCleaned();
    
    TapeDrive setLastCleaned( final Date value );
    
    
    String FORCE_TAPE_REMOVAL = "forceTapeRemoval";
    
    /**
     * @return TRUE if the tape drive requires that the tape in it be moved out at the earliest opportunity
     */
    @DefaultBooleanValue( false )
    boolean isForceTapeRemoval();
    
    TapeDrive setForceTapeRemoval( final boolean value );
    
    
    String CLEANING_REQUIRED = "cleaningRequired";
    
    /**
     * @return TRUE if the tape drive requires cleaning at the earliest opportunity
     */
    @DefaultBooleanValue( false )
    boolean getCleaningRequired();
    
    TapeDrive setCleaningRequired( final boolean value );


    String MFG_SERIAL_NUMBER = "mfgSerialNumber";
    
    @Optional
    String getMfgSerialNumber();
    
    TapeDrive setMfgSerialNumber( final String value );


    String RESERVED_TASK_TYPE = "reservedTaskType";

    @DefaultEnumValue( "ANY" )
    ReservedTaskType getReservedTaskType();

    TapeDrive setReservedTaskType( final ReservedTaskType value );


    String MINIMUM_TASK_PRIORITY = "minimumTaskPriority";

    //NOTE: this field must be nullable because users cannot specify the lowest level (BACKGROUND) via the API.
    @Optional
    BlobStoreTaskPriority getMinimumTaskPriority();

    TapeDrive setMinimumTaskPriority( final BlobStoreTaskPriority value );


    String MAX_FAILED_TAPES = "maxFailedTapes";

    @Optional
    @DefaultIntegerValue(3)
    Integer getMaxFailedTapes();

    TapeDrive setMaxFailedTapes( final Integer value );


    String CHARACTERIZATION_VER = "characterizationVer";

    @Optional
    String getCharacterizationVer();

    TapeDrive setCharacterizationVer(final String value );
}
