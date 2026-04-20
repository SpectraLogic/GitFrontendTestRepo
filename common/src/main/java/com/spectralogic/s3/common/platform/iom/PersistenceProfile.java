package com.spectralogic.s3.common.platform.iom;

public enum PersistenceProfile
{
    ANY( true, true, true, true, true ),
    EVERYTHING_BUT_OBSOLETE( true, true, true, true, false ),
    DATA_INTEGRITY_OK( true, true, false, false, true ),
    DATA_INTEGRITY_OK_NOT_OBSOLETE( true, true, false, false, false ),
    /*Typically, passing NO_MOVING_OR_HEALING_REQUIRED should yield the same result as PERECT. If that's
     *not true, it means we have records that are marked obsolete but otherwise look fine. This might happen if
     *someone started excluding a storage domain member, then stopped partway through. There's no need to move such
     *a record anywhere. We'll delete it eventually, or un-obsolete it if something bad happened to our newer copy.*/
    NO_MOVING_OR_HEALING_REQUIRED( false, false, false, false, true ),
    PERFECT( false, false, false, false, false );
    
    private PersistenceProfile(
            final boolean includeMembersPendingExclusion,
            final boolean includeTapesPendingAutoCompaction,
            final boolean includeTapesInErrorState,
            final boolean includeSuspectBlobsRecords,
            final boolean includeObsoleteBlobRecords )
    {
        m_includeMembersPendingExclusion = includeMembersPendingExclusion;
        m_includeTapesPendingAutoCompaction = includeTapesPendingAutoCompaction;
        m_includeTapesInErrorState = includeTapesInErrorState;
        m_includeSuspectBlobsRecords = includeSuspectBlobsRecords;
        m_includeObsoleteBlobRecords = includeObsoleteBlobRecords;
    }
    
    
    public boolean isIncludeMembersPendingExclusion()
    {
        return m_includeMembersPendingExclusion;
    }
    
    
    public boolean isIncludeTapesPendingAutoCompaction()
    {
        return m_includeTapesPendingAutoCompaction;
    }
    
    
    public boolean isIncludeTapesInErrorState()
    {
        return m_includeTapesInErrorState;
    }
    
    
    public boolean isIncludeSuspectBlobsRecords()
    {
        return m_includeSuspectBlobsRecords;
    }
    
    
    public boolean isIncludeObsoleteBlobRecords()
    {
        return m_includeObsoleteBlobRecords;
    }


    private final boolean m_includeMembersPendingExclusion;
    private final boolean m_includeTapesPendingAutoCompaction;
    private final boolean m_includeTapesInErrorState;
    private final boolean m_includeSuspectBlobsRecords;
    private final boolean m_includeObsoleteBlobRecords;
}
