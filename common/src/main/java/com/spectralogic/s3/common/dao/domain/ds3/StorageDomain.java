/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A domain of storage that provides isolation from other {@link StorageDomain}s.
 */
@UniqueIndexes(
{
    @Unique( NameObservable.NAME )
})
public interface StorageDomain extends NameObservable< StorageDomain >, DatabasePersistable
{
    String WRITE_OPTIMIZATION = "writeOptimization";
    
    @DefaultEnumValue( "CAPACITY" )
    WriteOptimization getWriteOptimization();
    
    StorageDomain setWriteOptimization( final WriteOptimization value );
    
    
    String SECURE_MEDIA_ALLOCATION = "secureMediaAllocation";
    
    /**
     * If secure media allocation is enabled, we ensure that no media that contained data past, present, or 
     * future, is ever re-used for another purpose, which may be required in high-security applications where 
     * physical media must be kept separate and possibly even physically destroyed once the data must be 
     * deleted.  <br><br>
     * 
     * For example, if a tape is allocated to a storage domain with secure media allocation enabled and all
     * data on that tape is deleted so that it can be reformatted and reclaimed, it must be re-allocated into
     * the storage domain.  Otherwise, it shall be released and made available to any storage domain.
     */
    @DefaultBooleanValue( false )
    boolean isSecureMediaAllocation();
    
    StorageDomain setSecureMediaAllocation( final boolean value );
    
    
    String MAX_TAPE_FRAGMENTATION_PERCENT = "maxTapeFragmentationPercent";
    
    /**
     * @return the max percent fragmentation on a full or nearly full tape before the non-deleted data on the
     * tape is copied elsewhere and the original, highly-fragmented tape is reclaimed
     */
    @DefaultIntegerValue( 65 )
    int getMaxTapeFragmentationPercent();
    
    StorageDomain setMaxTapeFragmentationPercent( final int value );
    
    
    String MEDIA_EJECTION_ALLOWED = "mediaEjectionAllowed";
    
    /**
     * If false, a failure will be generated for any media that goes away that wasn't supposed to, and eject
     * commands for media allocated to this storage domain will be rejected
     */
    @DefaultBooleanValue( true )
    boolean isMediaEjectionAllowed();
    
    StorageDomain setMediaEjectionAllowed( final boolean value );
    
    
    String AUTO_EJECT_UPON_JOB_COMPLETION = "autoEjectUponJobCompletion";

    /**
     * If true, all media will be automatically ejected in the storage domain upon the completion of any job
     */
    @DefaultBooleanValue( false )
    boolean isAutoEjectUponJobCompletion();
    
    StorageDomain setAutoEjectUponJobCompletion( final boolean value );
    
    
    String AUTO_EJECT_UPON_JOB_CANCELLATION = "autoEjectUponJobCancellation";

    /**
     * If true, all media will be automatically ejected in the storage domain upon the cancellation of any job
     */
    @DefaultBooleanValue( false )
    boolean isAutoEjectUponJobCancellation();
    
    StorageDomain setAutoEjectUponJobCancellation( final boolean value );
    
    
    String AUTO_EJECT_UPON_MEDIA_FULL = "autoEjectUponMediaFull";

    /**
     * If true, media will be automatically ejected in the storage domain as it becomes full (or close enough
     * to full to be considered full)
     */
    @DefaultBooleanValue( false )
    boolean isAutoEjectUponMediaFull();
    
    StorageDomain setAutoEjectUponMediaFull( final boolean value );
    
    
    String AUTO_EJECT_MEDIA_FULL_THRESHOLD = "autoEjectMediaFullThreshold";

    /**
     * If configured, this is the minimum available capacity (in bytes) at which media will not be considered
     * full and eligible for auto-eject.  If not configured, the auto-eject threshold will be dynamically and 
     * automatically computed based on the preferred chunk size.  <br><br>
     * 
     * This attribute is ignored and has no effect if {@link #isAutoEjectUponMediaFull} is false.
     */
    @Optional
    Long getAutoEjectMediaFullThreshold();
    
    StorageDomain setAutoEjectMediaFullThreshold( final Long value );
    
    
    String AUTO_EJECT_UPON_CRON = "autoEjectUponCron";

    /**
     * If non-null, all media will be automatically ejected in the storage domain according to the CRON
     * schedule specified
     */
    @Optional
    String getAutoEjectUponCron();
    
    StorageDomain setAutoEjectUponCron( final String value );
    
    
    String VERIFY_PRIOR_TO_AUTO_EJECT = "verifyPriorToAutoEject";
    
    /**
     * @return the priority for verifies if tapes being ejected automatically due to any of the auto-eject 
     * triggers must be verified prior to being ejected; or null if verification is not required prior to
     * ejection
     */
    @Optional
    BlobStoreTaskPriority getVerifyPriorToAutoEject();
    
    StorageDomain setVerifyPriorToAutoEject( final BlobStoreTaskPriority value );
    
    
    String MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS = "maximumAutoVerificationFrequencyInDays";
    
    /**
     * @return the minimum duration to wait to verify media after it has been written to <br><br>
     * 
     * Note that media becomes eligible for auto-verification if this number of days has passed since the
     * media was last modified, and the media has not been verified since it was last modified (for example,
     * if this number is 30 and a tape gets written to every day for 40 days, then the tape becomes eligible
     * for auto-verification at 70 days assuming it wasn't verified between 40 and 70 days.  Once verified, 
     * the tape doesn't become eligible for auto-verification again until it gets modified again, at which
     * time, it can't be verified until 30 days go by without additional modification).
     */
    @Optional
    Integer getMaximumAutoVerificationFrequencyInDays();
    
    StorageDomain setMaximumAutoVerificationFrequencyInDays( final Integer value );
    
    
    String LTFS_FILE_NAMING = "ltfsFileNaming";
    
    @DefaultEnumValue( "OBJECT_ID" )
    LtfsFileNamingMode getLtfsFileNaming();
    
    StorageDomain setLtfsFileNaming( final LtfsFileNamingMode value );
}
