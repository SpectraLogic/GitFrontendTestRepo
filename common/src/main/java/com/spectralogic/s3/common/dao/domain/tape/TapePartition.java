/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
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
@Indexes( { @Index( NameObservable.NAME ), @Index( TapePartition.STATE ) } )
public interface TapePartition
  extends DatabasePersistable, SerialNumberObservable< TapePartition >, 
          NameObservable< TapePartition >, ErrorMessageObservable< TapePartition >
{
    String STATE = "state";

    /**
     * Any partition we can talk to is a partition we can use i.e. there is no way to exclude a partition
     * from this application.
     */
    @DefaultEnumValue( "ONLINE" )
    @SortBy( 1 )
    TapePartitionState getState();
    
    TapePartition setState( final TapePartitionState value );
    
    
    String QUIESCED = "quiesced";

    @DefaultEnumValue( "YES" )
    Quiesced getQuiesced();
    
    TapePartition setQuiesced( final Quiesced value );
    
    
    String LIBRARY_ID = "libraryId";
    
    @References( TapeLibrary.class )
    UUID getLibraryId();
    
    TapePartition setLibraryId( final UUID value );
    
    
    String IMPORT_EXPORT_CONFIGURATION = "importExportConfiguration";
    
    ImportExportConfiguration getImportExportConfiguration();
    
    TapePartition setImportExportConfiguration( final ImportExportConfiguration value );
    
    
    String DRIVE_TYPE = "driveType";
    
    /**
     * @return the highest generation / best tape drive's type that is online and working in the partition
     * <br><br>
     * 
     * Note: A single tape partition is only allowed to have a single type of drive in it.  If there are
     * multiple tape drive types within a partition, the older drives will be ignored and a tape partition
     * failure generated for misconfiguring the tape partition.
     */
    @Optional
    TapeDriveType getDriveType();
    
    TapePartition setDriveType( final TapeDriveType value );
    
    
    /*
     * The minimum number of drives the tape partition should be reserving for reads at any given time.
     */
    String MINIMUM_READ_RESERVED_DRIVES = "minimumReadReservedDrives";

    @DefaultIntegerValue(0)
    int getMinimumReadReservedDrives();

    TapePartition setMinimumReadReservedDrives( final int value );


    /*
     * The minimum number of drives the tape partition should be reserving for writes at any given time.
     */
    String MINIMUM_WRITE_RESERVED_DRIVES = "minimumWriteReservedDrives";

    @DefaultIntegerValue(0)
    int getMinimumWriteReservedDrives();

    TapePartition setMinimumWriteReservedDrives( final int value );
    
    
    String AUTO_COMPACTION_ENABLED = "autoCompactionEnabled";

    /**
     * If true, tape compaction will happen passively
     */
    @DefaultBooleanValue( false )
    boolean isAutoCompactionEnabled();
    
    TapePartition setAutoCompactionEnabled( final boolean value );


    String AUTO_QUIESCE_ENABLED = "autoQuiesceEnabled";

    @DefaultBooleanValue( true )
    boolean isAutoQuiesceEnabled();
    TapePartition setAutoQuiesceEnabled( final boolean value );


    String DRIVE_IDLE_TIMEOUT_IN_MINUTES = "driveIdleTimeoutInMinutes";

    @Optional
    @DefaultIntegerValue(15)
    Integer getDriveIdleTimeoutInMinutes();

    TapePartition setDriveIdleTimeoutInMinutes( final Integer value );
}
