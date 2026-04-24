/*******************************************************************************
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.dataorder;

import com.google.common.collect.Iterables;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.orm.PoolRM;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.ds3.JobCreationFailedService;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.frontend.api.JobEntryGrouping;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Orders {@link Blob}s so that they are retrieved in chunks that can be serviced entirely by a single
 * {@link Tape} or {@link Pool}, ordering the contents therein to avoid shoe-shining tapes by retrieving
 * {@link Blob} non-sequentially.
 */
public final class GetByPhysicalPlacementDataOrderingStrategy
{
    public GetByPhysicalPlacementDataOrderingStrategy(
            final Set< UUID > blobsToRead,
            final BeansServiceManager serviceManager,
            final CacheManager cacheManager,
            final JobRequestType jobRequestType,
            final JobChunkClientProcessingOrderGuarantee orderGuarantee,
            final Ds3TargetBlobPhysicalPlacement ds3TargetBlobPhysicalPlacement,
            final boolean failIfBlobsMissing,
            final boolean generateNotifications,
            final String userId ) {
        Validations.verifyNotNull( "Blobs to read", blobsToRead );
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Cache manager", cacheManager );
        Validations.verifyNotNull( "Job request type", jobRequestType );
        Validations.verifyNotNull( "Order guarantee", orderGuarantee );

        m_readByBlobds = true;
        m_blobsToRead = new HashSet<>( blobsToRead );
        m_serviceManager = serviceManager;
        m_cacheManager = cacheManager;
        m_jobRequestType = jobRequestType;
        m_orderGuarantee = orderGuarantee;

        m_failIfBlobsMissing = failIfBlobsMissing;
        m_generateNotifications = generateNotifications;
        m_userName = userId;
        m_ds3TargetBlobPhysicalPlacement = ds3TargetBlobPhysicalPlacement;
        initialize( jobRequestType, blobsToRead );

    }
    public GetByPhysicalPlacementDataOrderingStrategy(
            final Map< UUID, JobEntry> entriesByBlobId,
            final BeansServiceManager serviceManager,
            final JobRequestType jobRequestType,
            final JobChunkClientProcessingOrderGuarantee orderGuarantee,
            final Ds3TargetBlobPhysicalPlacement ds3TargetBlobPhysicalPlacement,
            final CacheManager cacheManager,
            final String userName,
            final boolean iom)
    {
        Validations.verifyNotNull( "Blobs to read", entriesByBlobId );
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Job request type", jobRequestType );
        Validations.verifyNotNull( "Order guarantee", orderGuarantee );

        m_readByBlobds = false;
        m_entriesByBlobId = new HashMap<>(entriesByBlobId);
        m_serviceManager = serviceManager;
        m_jobRequestType = jobRequestType;
        m_orderGuarantee = orderGuarantee;
        m_cacheManager = cacheManager;
        m_userName = userName;
        m_iom = iom;
        m_ds3TargetBlobPhysicalPlacement = ds3TargetBlobPhysicalPlacement;
        initialize( jobRequestType, entriesByBlobId.keySet() );

    }

    private void initialize(final JobRequestType jobRequestType, Set<UUID> blobIds) {
        if (m_serviceManager.getRetriever(DataPathBackend.class).retrieve(Require.nothing()).getUnavailableMediaPolicy() == UnavailableMediaUsagePolicy.DISALLOW) {
            m_useUnavailableMedia = false;
        } else {
            m_useUnavailableMedia = true;
        }
        if ( JobRequestType.GET == jobRequestType )
        {

            m_azureSupport = new PublicCloudBlobSupport<>(
                    AzureTarget.class,
                    BlobAzureTarget.class,
                    SuspectBlobAzureTarget.class,
                    AzureTargetReadPreference.class,
                    blobIds,
                    m_serviceManager );
            m_s3Support = new PublicCloudBlobSupport<>(
                    S3Target.class,
                    BlobS3Target.class,
                    SuspectBlobS3Target.class,
                    S3TargetReadPreference.class,
                    blobIds,
                    m_serviceManager );
        }
        else
        {
            m_ds3TargetBlobPhysicalPlacement = null;
            m_azureSupport = null;
            m_s3Support = null;
        }
    }




    public Set< UUID > getUnavailableBlobs()
    {
        if ( !m_ordered )
        {
            throw new IllegalStateException( "Strategy must be ordered before missing blobs IDs are requested.");
        }
        if (m_readByBlobds) {
            return m_blobsToRead;
        }
        return m_entriesByBlobId.keySet();
    }

    public Set<PersistenceType> getReadOrdering()
    {
        m_types.clear();
        if ( JobRequestType.GET == m_jobRequestType )
        {
            addZeroLengthObjects();
            addObjectsInCache();
        }

        final UnavailableMediaUsagePolicy mediaPolicy =
                m_serviceManager.getRetriever(DataPathBackend.class).retrieve(Require.nothing()).getUnavailableMediaPolicy();

        switch ( mediaPolicy )
        {
            case DISALLOW:
                m_useUnavailableMedia = false;
                addObjectsOnMedia();
                break;
            case ALLOW:
                m_useUnavailableMedia = true;
                addObjectsOnMedia();
                break;
            case DISCOURAGED:
                m_useUnavailableMedia = false;
                addObjectsOnMedia();
                m_useUnavailableMedia = true;
                addObjectsOnMedia();
                break;
            default:
                throw new UnsupportedOperationException(
                        "No code to support: " + mediaPolicy );
        }

        m_ordered = true;
        if (m_readByBlobds) {
            if ( !m_blobsToRead.isEmpty() && m_failIfBlobsMissing)
            {
                throw createFailure( m_blobsToRead );
            }
        } else {
            if ( !m_entriesByBlobId.isEmpty() )
            {
                throw createFailure( m_entriesByBlobId.keySet() );
            }

        }
        return m_types;

    }

    public Set<PersistenceType> setReadSources() {
        return setReadSources( true );
    }

    public Set<PersistenceType> setReadSources(final boolean setChunkNumbers)
    {
        getReadOrdering();
        if (setChunkNumbers) {
            allocateChunkNumbers();
        }
        return m_types;
    }

    private void allocateChunkNumbers(  )
    {
        // First, collect all current chunk numbers
        List<Integer> existingChunkNumbers = m_groupings.stream()
                .flatMap(grouping -> grouping.getEntries().stream())
                .map(JobEntry::getChunkNumber)
                .sorted()
                .toList();
        int index = 0;
        for (JobEntryGrouping grouping : m_groupings) {
            for (JobEntry entry : grouping.getEntries()) {
                entry.setChunkNumber(existingChunkNumbers.get(index));
                index++;
            }
        }

    }
    private void addObjectsOnMedia()
    {
        addObjectsOnPool(PoolType.ONLINE);
        addObjectsOnDs3Target( TargetReadPreferenceType.AFTER_ONLINE_POOL );
        addObjectsOnPublicCloudTarget( TargetReadPreferenceType.AFTER_ONLINE_POOL );
        addObjectsOnPool(PoolType.NEARLINE);
        addObjectsOnDs3Target( TargetReadPreferenceType.AFTER_NEARLINE_POOL );
        addObjectsOnPublicCloudTarget( TargetReadPreferenceType.AFTER_NEARLINE_POOL );
        addObjectsOnTape( false );
        addObjectsOnDs3Target( TargetReadPreferenceType.AFTER_NON_EJECTABLE_TAPE );
        addObjectsOnPublicCloudTarget( TargetReadPreferenceType.AFTER_NON_EJECTABLE_TAPE );
        addObjectsOnTape( true );
        addObjectsOnDs3Target( TargetReadPreferenceType.LAST_RESORT );
        addObjectsOnPublicCloudTarget( TargetReadPreferenceType.LAST_RESORT );
    }


    private void addObjectsInCache()
    {
        if (m_readByBlobds) {
            addObjectsInCacheBlobs();

        } else  {
            addObjectsInCacheJobEntries();

        }

    }

    private void addObjectsInCacheBlobs() {
        Iterables.partition(m_blobsToRead, SINGLE_TRANSACTION_MAX).forEach( segment -> {
            final List<UUID> objectsIdsInCache = segment.stream().filter((b) -> m_cacheManager.isInCache(b)).collect(Collectors.toList());
            List<UUID> entriesForGrouping = new ArrayList<>();
            for (final UUID blobId : objectsIdsInCache) {
                entriesForGrouping.add(blobId);
                m_blobsToRead.remove(blobId);
            }

        });
    }

    private void addObjectsInCacheJobEntries() {
        Iterables.partition(m_entriesByBlobId.keySet(), SINGLE_TRANSACTION_MAX).forEach( segment -> {
            final List<UUID> objectsIdsInCache = segment.stream().filter((b) -> m_cacheManager.isInCache(b)).collect(Collectors.toList());
            List<JobEntry> entriesForGrouping = new ArrayList<>();
            for (final UUID blobId : objectsIdsInCache) {
                entriesForGrouping.add(m_entriesByBlobId.get(blobId));
                m_entriesByBlobId.remove(blobId);
            }
            if (!entriesForGrouping.isEmpty()){
                m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
            }
        });
    }


    private void addZeroLengthObjects()
    {
        if (m_readByBlobds) {
            addZeroLengthObjectsBlobs();
        } else {
            addZeroLengthObjectsEntries();
        }
    }

    private void addZeroLengthObjectsBlobs() {
        Iterables.partition(m_blobsToRead, SINGLE_TRANSACTION_MAX).forEach( segment -> {
            final List<Blob> zeroLengthObjects = m_serviceManager.getRetriever(Blob.class).retrieveAll(
                    Require.all(
                            Require.beanPropertyEqualsOneOf(Identifiable.ID, segment),
                            Require.beanPropertyEquals(Blob.LENGTH, 0L))).toList();
            List<JobEntry> entriesForGrouping = new ArrayList<>();
            for (final Blob blob : zeroLengthObjects) {
                m_blobsToRead.remove(blob.getId());
            }

        });
    }
    private void addZeroLengthObjectsEntries() {
        Iterables.partition(m_entriesByBlobId.keySet(), SINGLE_TRANSACTION_MAX).forEach( segment -> {
            final List<Blob> zeroLengthObjects = m_serviceManager.getRetriever(Blob.class).retrieveAll(
                    Require.all(
                            Require.beanPropertyEqualsOneOf(Identifiable.ID, segment),
                            Require.beanPropertyEquals(Blob.LENGTH, 0L))).toList();
            List<JobEntry> entriesForGrouping = new ArrayList<>();
            for (final Blob blob : zeroLengthObjects) {
                entriesForGrouping.add(m_entriesByBlobId.get(blob.getId()));
                m_entriesByBlobId.remove(blob.getId());
            }
            if (!entriesForGrouping.isEmpty()){
                m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
            }
        });
    }

    private void addObjectsOnPool(final PoolType type)
    {
        if (m_readByBlobds) {
            addObjectsOnPoolBlobs(type);
        } else {
            addObjectsOnPoolEntries(type);
        }

    }

    private void addObjectsOnPoolEntries(final PoolType type) {
        Iterables.partition(m_entriesByBlobId.keySet(), SINGLE_TRANSACTION_MAX).forEach( segment -> {
            final List< BlobPool > objectsOnPool = new ArrayList<>(findAvailableBlobPools(segment, type));
            List<JobEntry> entriesForGrouping = new ArrayList<>();
            UUID lastPoolId = null;
            for (final BlobPool blobPool : objectsOnPool) {
                if (lastPoolId != null && !lastPoolId.equals(blobPool.getPoolId())) {
                    m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
                    entriesForGrouping = new ArrayList<>();
                }
                lastPoolId = blobPool.getPoolId();
                if (m_entriesByBlobId.containsKey(blobPool.getBlobId())) {
                    m_entriesByBlobId.get(blobPool.getBlobId()).setReadFromPoolId(blobPool.getPoolId());
                    entriesForGrouping.add(m_entriesByBlobId.get(blobPool.getBlobId()));
                    m_entriesByBlobId.remove(blobPool.getBlobId());
                }
            }
            if (!entriesForGrouping.isEmpty()){
                m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
            }
            if (!objectsOnPool.isEmpty()) {
                m_types.add(PersistenceType.POOL);
            }
        });
    }

    private void addObjectsOnPoolBlobs(final PoolType type) {
        Iterables.partition(m_blobsToRead, SINGLE_TRANSACTION_MAX).forEach( segment -> {
            final List< BlobPool > objectsOnPool = new ArrayList<>(findAvailableBlobPools(segment, type));
            List<JobEntry> entriesForGrouping = new ArrayList<>();
            UUID lastPoolId = null;
            for (final BlobPool blobPool : objectsOnPool) {
                if (m_blobsToRead.contains(blobPool.getBlobId())) {
                    m_blobsToRead.remove(blobPool.getBlobId());
                }
            }
            if (!objectsOnPool.isEmpty()) {
                m_types.add(PersistenceType.POOL);
            }
        });
    }

    private void addObjectsOnTape( final boolean searchOnEjectableMedia ) {
        if (m_readByBlobds) {
            addObjectsOnTapeBlobs(searchOnEjectableMedia);
        } else {
            addObjectsOnTapeEntries(searchOnEjectableMedia);
        }
    }

    private void addObjectsOnTapeBlobs( final boolean searchOnEjectableMedia )
    {
        Iterables.partition(m_blobsToRead, SINGLE_TRANSACTION_MAX).forEach( segment -> {
            final List< BlobTape > objectsOnTape = new ArrayList<>(findAvailableBlobTapes(segment, searchOnEjectableMedia));
            Collections.sort(
                    objectsOnTape,
                    new BeanComparator<>(
                            BlobTape.class,
                            new BeanComparator.BeanPropertyComparisonSpecifiction(
                                    BlobTape.TAPE_ID, SortBy.Direction.ASCENDING, null ),
                            new BeanComparator.BeanPropertyComparisonSpecifiction(
                                    BlobTape.ORDER_INDEX, SortBy.Direction.ASCENDING, null ) ) );


            List<JobEntry> entriesForGrouping = new ArrayList<>();
            UUID lastTapeId = null;
            for (final BlobTape blobTape : objectsOnTape) {
                if (m_blobsToRead.contains(blobTape.getBlobId())) {
                    m_blobsToRead.remove(blobTape.getBlobId());
                }
            }
            if (!objectsOnTape.isEmpty()) {
                m_types.add(PersistenceType.TAPE);
            }
        });
    }

    private void addObjectsOnTapeEntries( final boolean searchOnEjectableMedia )
    {
        Iterables.partition(m_entriesByBlobId.keySet(), SINGLE_TRANSACTION_MAX).forEach( segment -> {
            final List< BlobTape > objectsOnTape = new ArrayList<>(findAvailableBlobTapes(segment, searchOnEjectableMedia));
            Collections.sort(
                    objectsOnTape,
                    new BeanComparator<>(
                            BlobTape.class,
                            new BeanComparator.BeanPropertyComparisonSpecifiction(
                                    BlobTape.TAPE_ID, SortBy.Direction.ASCENDING, null ),
                            new BeanComparator.BeanPropertyComparisonSpecifiction(
                                    BlobTape.ORDER_INDEX, SortBy.Direction.ASCENDING, null ) ) );


            List<JobEntry> entriesForGrouping = new ArrayList<>();
            UUID lastTapeId = null;
            for (final BlobTape blobTape : objectsOnTape) {
                if (lastTapeId != null && !lastTapeId.equals(blobTape.getTapeId())) {
                    m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
                    entriesForGrouping = new ArrayList<>();
                }
                lastTapeId = blobTape.getTapeId();
                if (m_entriesByBlobId.containsKey(blobTape.getBlobId())) {
                    m_entriesByBlobId.get(blobTape.getBlobId()).setReadFromTapeId(blobTape.getTapeId());
                    entriesForGrouping.add(m_entriesByBlobId.get(blobTape.getBlobId()));
                    m_entriesByBlobId.remove(blobTape.getBlobId());
                }
            }
            if (!entriesForGrouping.isEmpty()){
                m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
            }
            if (!objectsOnTape.isEmpty()) {
                m_types.add(PersistenceType.TAPE);
            }
        });
    }

    private void addObjectsOnDs3Target( final TargetReadPreferenceType requiredReadPreference ) {
        if ( null == m_ds3TargetBlobPhysicalPlacement )
        {
            return;
        }
        for ( final UUID targetId : m_ds3TargetBlobPhysicalPlacement.getCandidateTargets() )
        {
            final Set< UUID > blobIds = new HashSet<>();
            final TargetReadPreferenceType targetReadPreference =
                    m_ds3TargetBlobPhysicalPlacement.getReadPreference( targetId );
            if ( requiredReadPreference == targetReadPreference )
            {
                blobIds.addAll( m_ds3TargetBlobPhysicalPlacement.getBlobsOnPool( targetId ) );
                blobIds.addAll( m_ds3TargetBlobPhysicalPlacement.getBlobsOnTape( targetId ) );
            }
            if ( TargetReadPreferenceType.MINIMUM_LATENCY == targetReadPreference )
            {
                if ( TargetReadPreferenceType.AFTER_NEARLINE_POOL == requiredReadPreference )
                {
                    blobIds.addAll( m_ds3TargetBlobPhysicalPlacement.getBlobsOnPool( targetId ) );
                }
                if ( TargetReadPreferenceType.LAST_RESORT == requiredReadPreference )
                {
                    blobIds.addAll( m_ds3TargetBlobPhysicalPlacement.getBlobsOnTape( targetId ) );
                }
            }
            if (m_readByBlobds) {
                for ( final UUID blobId : blobIds )
                {
                    if (m_blobsToRead.contains(blobId)) {
                        m_blobsToRead.remove(blobId);
                    }
                }
                if (!blobIds.isEmpty()) {
                    m_types.add(PersistenceType.DS3);
                }

            } else {
                addObjectsOnDs3Target( targetId, blobIds );
            }

        }

    }

    private void addObjectsOnDs3Target( final UUID targetId, final Set< UUID > blobIds )
    {
        blobIds.retainAll(m_entriesByBlobId.keySet());
        if ( blobIds.isEmpty() )
        {
            return;
        }

        List<JobEntry> entriesForGrouping = new ArrayList<>();
        UUID lastTargetId = null;
        for ( final UUID blobId : blobIds )
        {
            if (lastTargetId != null && !lastTargetId.equals(targetId)) {
                m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
                entriesForGrouping = new ArrayList<>();
            }
            if (m_entriesByBlobId.containsKey(blobId)) {
                m_entriesByBlobId.get(blobId).setReadFromDs3TargetId(targetId);
                entriesForGrouping.add(m_entriesByBlobId.get(blobId));
                m_entriesByBlobId.remove(blobId);
            }
        }
        if (!entriesForGrouping.isEmpty()){
            m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
        }
        m_types.add(PersistenceType.DS3);
    }


    private void addObjectsOnPublicCloudTarget( TargetReadPreferenceType requiredReadPreference )
    {
        if ( JobRequestType.GET != m_jobRequestType )
        {
            return;
        }
        if ( TargetReadPreferenceType.AFTER_NEARLINE_POOL == requiredReadPreference )
        {
            addObjectsOnPublicCloudTarget( TargetReadPreferenceType.MINIMUM_LATENCY );
        }

        final Map< UUID, Set< UUID > > azureBlobs = m_azureSupport.getBlobs( requiredReadPreference );
        for ( final Map.Entry< UUID, Set< UUID > > e : azureBlobs.entrySet() )
        {
            if (m_readByBlobds) {
                for ( final UUID blobId : e.getValue() )
                {
                    if (m_blobsToRead.contains(blobId)) {
                        m_blobsToRead.remove(blobId);
                    }
                }
                if (! e.getValue().isEmpty()) {
                    m_types.add(PersistenceType.AZURE);
                }
            } else if (!m_entriesByBlobId.isEmpty()) {
                addObjectsOnAzureTarget( e.getKey(), e.getValue() );
            }

        }

        final Map< UUID, Set< UUID > > s3Blobs = m_s3Support.getBlobs( requiredReadPreference );
        for ( final Map.Entry< UUID, Set< UUID > > e : s3Blobs.entrySet() )
        {
            if (m_readByBlobds) {
                for ( final UUID blobId : e.getValue() )
                {
                    if (m_blobsToRead.contains(blobId)) {
                        m_blobsToRead.remove(blobId);
                    }
                }
                if (! e.getValue().isEmpty()) {
                    m_types.add(PersistenceType.S3);
                }
            } else if (!m_entriesByBlobId.isEmpty()) {
                addObjectsOnS3Target( e.getKey(), e.getValue() );
            }

        }
    }


    private void addObjectsOnAzureTarget( final UUID targetId, final Set< UUID > blobIds )
    {
        blobIds.retainAll(m_entriesByBlobId.keySet());
        if ( blobIds.isEmpty() )
        {
            return;
        }

        List<JobEntry> entriesForGrouping = new ArrayList<>();
        UUID lastTargetId = null;
        for ( final UUID blobId : blobIds )
        {
            if (lastTargetId != null && !lastTargetId.equals(targetId)) {
                m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
                entriesForGrouping = new ArrayList<>();
            }
            if (m_entriesByBlobId.containsKey(blobId)) {
                m_entriesByBlobId.get(blobId).setReadFromAzureTargetId(targetId);
                entriesForGrouping.add(m_entriesByBlobId.get(blobId));
                m_entriesByBlobId.remove(blobId);
            }
        }
        if (!entriesForGrouping.isEmpty()){
            m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
        }
        m_types.add(PersistenceType.AZURE);
    }


    private void addObjectsOnS3Target( final UUID targetId, final Set< UUID > blobIds )
    {
        blobIds.retainAll(m_entriesByBlobId.keySet());
        if ( blobIds.isEmpty() )
        {
            return;
        }


        List<JobEntry> entriesForGrouping = new ArrayList<>();
        UUID lastTargetId = null;
        for ( final UUID blobId : blobIds )
        {
            if (lastTargetId != null && !lastTargetId.equals(targetId)) {
                m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
                entriesForGrouping = new ArrayList<>();
            }
            if (m_entriesByBlobId.containsKey(blobId)) {
                m_entriesByBlobId.get(blobId).setReadFromS3TargetId(targetId);
                entriesForGrouping.add(m_entriesByBlobId.get(blobId));
                m_entriesByBlobId.remove(blobId);
            }
        }
        if (!entriesForGrouping.isEmpty()){
            m_groupings.add(BeanFactory.newBean(JobEntryGrouping.class).setEntries(entriesForGrouping));
        }
        m_types.add(PersistenceType.S3);
    }


    private FailureTypeObservableException createFailure( final Set< UUID > missingBlobIds )
    {
        final int totalBlobsMissing = missingBlobIds.size();
        final Set< BlobTape > unavailableBlobTapes =
                m_serviceManager.getRetriever( BlobTape.class ).retrieveAll(
                        Require.beanPropertyEqualsOneOf(
                                BlobObservable.BLOB_ID, missingBlobIds ) ).toSet();
        final Set< UUID > unavailableTapeBlobIds = new HashSet<>(
                BeanUtils.< UUID >extractPropertyValues( unavailableBlobTapes, BlobObservable.BLOB_ID ) );
        final Set< ? extends BlobTape > suspectBlobTapes =
                m_serviceManager.getRetriever( SuspectBlobTape.class ).retrieveAll(
                        BeanUtils.toMap( unavailableBlobTapes ).keySet() ).toSet();
        final Set< UUID > suspectBlobTapeIds = new HashSet<>(
                BeanUtils.< UUID >extractPropertyValues( suspectBlobTapes, BlobObservable.BLOB_ID ) );

        final Set< BlobPool > unavailableBlobPools = m_serviceManager
                .getRetriever( BlobPool.class )
                .retrieveAll(
                        Require.all( Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID,
                                missingBlobIds ) ) ).toSet();
        final Set< UUID > unavailablePoolBlobIds = new HashSet<>(
                BeanUtils.< UUID >extractPropertyValues( unavailableBlobPools, BlobObservable.BLOB_ID ) );
        final Set< ? extends BlobPool > suspectBlobPools =
                m_serviceManager.getRetriever( SuspectBlobPool.class ).retrieveAll(
                        BeanUtils.toMap( unavailableBlobPools ).keySet() ).toSet();
        final Set< UUID > suspectBlobPoolIds = new HashSet<>(
                BeanUtils.< UUID >extractPropertyValues( suspectBlobPools, BlobObservable.BLOB_ID ) );

        final Set< UUID > unavailableBlobIds = new HashSet<>();
        unavailableBlobIds.addAll( unavailableTapeBlobIds );
        unavailableBlobIds.addAll( unavailablePoolBlobIds );
        missingBlobIds.removeAll( unavailableBlobIds );
        FailureTypeObservableException failure = null;
        if ( !missingBlobIds.isEmpty() )
        {
            if ( missingBlobIds.size() == m_serviceManager.getRetriever( Blob.class ).getCount(
                    Require.all(
                            Require.beanPropertyEqualsOneOf( Identifiable.ID, missingBlobIds ),
                            Require.exists(
                                    Blob.OBJECT_ID,
                                    Require.beanPropertyEquals( S3Object.CREATION_DATE, null ) ) ) ) )
            {
                failure = new FailureTypeObservableException(
                        GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                        "Some of the blobs requested have not been fully uploaded yet: "
                                + LogUtil.getShortVersion( missingBlobIds.toString() ) );
            } else {
                failure = new FailureTypeObservableException(
                        GenericFailure.CONFLICT,
                        "The following blobs cannot be found anywhere: "
                                + LogUtil.getShortVersion(missingBlobIds.toString()));
            }
        } else if ( !suspectBlobTapeIds.isEmpty() )
        {
            failure = new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "All available tape copies of the following blobs are suspected of being degraded: "
                            + LogUtil.getShortVersion( suspectBlobTapeIds.toString() ) );
        } else if ( !suspectBlobPoolIds.isEmpty() )
        {
            failure = new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "All available pool copies of the following blobs are suspected of being degraded: "
                            + LogUtil.getShortVersion( suspectBlobPoolIds.toString() ) );
        }
        if (failure != null) {
            if (m_iom) { //NOTE: We only generate failures for this case if this is for an IOM job
                m_serviceManager.getService(JobCreationFailedService.class).create(
                        m_userName,
                        JobCreationFailedType.DATA_UNAVAILABLE,
                        new ArrayList<>(),
                        "IOM is unable to progress on work for " + totalBlobsMissing + " blobs because: "
                                + failure.getMessage(),
                        60
                );
            }
            return failure;
        }

        final Set< UUID > unavailableTapeIds = new HashSet<>(
                BeanUtils.< UUID >extractPropertyValues( unavailableBlobTapes, BlobTape.TAPE_ID ) );
        final Set< UUID > unavailablePoolIds = new HashSet<>(
                BeanUtils.< UUID >extractPropertyValues( unavailableBlobPools, BlobPool.POOL_ID ) );
        final Set< Tape > unavailableTapes = m_serviceManager.getRetriever( Tape.class )
                .retrieveAll( unavailableTapeIds ).toSet();
        final Set< Pool > unavailablePools = m_serviceManager.getRetriever( Pool.class )
                .retrieveAll( unavailablePoolIds ).toSet();
        final Set< UUID > tapeStorageDomains = new HashSet<>();
        for ( final Tape tape : unavailableTapes )
        {
            tapeStorageDomains.add(
                    new TapeRM( tape, m_serviceManager ).getStorageDomainMember().unwrap().getStorageDomainId() );
        }
        final Set< UUID > poolStorageDomains = new HashSet<>();
        for ( final Pool pool : unavailablePools)
        {
            poolStorageDomains.add(
                    new PoolRM( pool, m_serviceManager ).getStorageDomainMember().unwrap().getStorageDomainId() );
        }
        final List< List< String > > barCodeSets = new ArrayList<>();
        for ( final UUID storageDomainId : tapeStorageDomains )
        {
            if ( isBlobSetEntirelyContained( storageDomainId, unavailableBlobIds ) )
            {
                barCodeSets.add( getTapeBarCodes( storageDomainId, unavailableTapes ) );
            }
        }
        for ( final UUID storageDomainId : poolStorageDomains )
        {
            if ( isBlobSetEntirelyContained( storageDomainId, unavailableBlobIds ) )
            {
                barCodeSets.add( getPoolNames( storageDomainId, unavailablePools ) );
            }
        }

        // NOTE: if possible we will return a list of lists of barcodes. There is one list of barcodes per storage domain.
        // Each such list represents an optional set of barcodes to load in order to online ALL currently unavailable data.
        // If no such list can be made, we will return a list of all barcodes with missing data further down in this function.
        if ( !barCodeSets.isEmpty() )
        {
            m_serviceManager.getService(JobCreationFailedService.class).create(
                    m_userName,
                    JobCreationFailedType.TAPES_MUST_BE_ONLINED,
                    barCodeSets,
                    "Some required media is offline.",
                    60
            );
            if ( 1 == barCodeSets.size() )
            {
                return new FailureTypeObservableException(
                        AWSFailure.MEDIA_ONLINING_REQUIRED,
                        ONLINE_PT_MSG_PREFIX + "Online: " + barCodeSets.get( 0 ) );
            }
            final StringBuilder retval = new StringBuilder( ONLINE_PT_MSG_PREFIX + "Online " );
            for ( int i = 0; i < barCodeSets.size(); ++i )
            {
                if ( 0 < i )
                {
                    retval.append( "; OR " );
                }
                retval.append( barCodeSets.get( i ) );
            }
            retval.append( "." );
            return new FailureTypeObservableException(
                    AWSFailure.MEDIA_ONLINING_REQUIRED,
                    retval.toString() );
        }

        final List< String > tapeBarCodes = getTapeBarCodes( null, unavailableTapes );
        m_serviceManager.getService(JobCreationFailedService.class).create(
                m_userName,
                JobCreationFailedType.TAPES_MUST_BE_ONLINED,
                CollectionFactory.toList(tapeBarCodes),
                "Some required media is offline.",
                60
        );

        return new FailureTypeObservableException(
                AWSFailure.MEDIA_ONLINING_REQUIRED,
                ONLINE_PT_MSG_PREFIX + "Online: " + tapeBarCodes );
    }


    private boolean isBlobSetEntirelyContained( final UUID storageDomainId, final Set< UUID > blobIds )
    {
        final Set< BlobTape > blobTapes =
                m_serviceManager.getRetriever( BlobTape.class ).retrieveAll( Require.all(
                        Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ),
                        Require.exists(
                                BlobTape.TAPE_ID,
                                Require.exists(
                                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                        Require.beanPropertyEquals(
                                                StorageDomainMember.STORAGE_DOMAIN_ID,
                                                storageDomainId ) ) ) ) ).toSet();
        final Set< BlobPool > blobPools = m_serviceManager.getRetriever( BlobPool.class ).retrieveAll(
                Require.all(
                        Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ),
                        Require.exists(
                                BlobPool.POOL_ID,
                                Require.exists(
                                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                        Require.beanPropertyEquals(
                                                StorageDomainMember.STORAGE_DOMAIN_ID,
                                                storageDomainId ) ) ) ) ).toSet();

        final Set< UUID > containedBlobIds = new HashSet<>();
        containedBlobIds.addAll(
                BeanUtils.< UUID >extractPropertyValues( blobTapes, BlobObservable.BLOB_ID ) );
        containedBlobIds.addAll(
                BeanUtils.< UUID >extractPropertyValues( blobPools, BlobObservable.BLOB_ID ) );

        return containedBlobIds.equals( blobIds );
    }


    private List< String > getTapeBarCodes( final UUID storageDomainId, final Set< Tape > tapes )
    {
        final List< String > retval = new ArrayList<>();
        for ( final Tape tape : tapes )
        {
            if ( null == storageDomainId ||storageDomainId.equals(
                    new TapeRM( tape, m_serviceManager ).getStorageDomainMember().unwrap().getStorageDomainId() ) )
            {
                retval.add( tape.getBarCode() );
            }
        }

        Collections.sort( retval );
        return retval;
    }


    private List< String > getPoolNames( final UUID storageDomainId, final Set< Pool > pools )
    {
        final List< String > retval = new ArrayList<>();
        for ( final Pool pool : pools )
        {
            if ( null == storageDomainId ||storageDomainId.equals(
                    new PoolRM( pool, m_serviceManager ).getStorageDomainMember().unwrap().getStorageDomainId() ) )
            {
                retval.add( pool.getName() );
            }
        }

        Collections.sort( retval );
        return retval;
    }


    private Set< BlobTape > findAvailableBlobTapes(
            final Collection< UUID > segment,
            final boolean searchOnEjectableMedia )
    {
        return PersistenceTargetUtil.findBlobTapesAvailableNow(
                m_serviceManager.getRetriever( BlobTape.class ),
                segment,
                m_useUnavailableMedia,
                Boolean.valueOf( searchOnEjectableMedia ) );
    }


    private Set< BlobPool > findAvailableBlobPools( final Collection< UUID > segment, final PoolType poolType )
    {
        return PersistenceTargetUtil.findBlobPoolsAvailableNow(
                m_serviceManager.getRetriever( BlobPool.class ),
                segment,
                poolType,
                m_useUnavailableMedia );
    }



    private boolean m_useUnavailableMedia;
    private final static String ONLINE_PT_MSG_PREFIX =
            "Some of the data requested is offline (e.g. ejected, offline, quiesced)"
                    + " and must first be brought online.  ";


    private boolean m_ordered = false;
    private boolean m_iom;
    private List<JobEntryGrouping> m_groupings = new ArrayList<>();

    private Set<PersistenceType> m_types = new HashSet<>();
    private final String m_userName;
    private final BeansServiceManager m_serviceManager;
    private final JobRequestType m_jobRequestType;
    private final CacheManager m_cacheManager;
    private final JobChunkClientProcessingOrderGuarantee m_orderGuarantee;
    private  Map< UUID, JobEntry> m_entriesByBlobId = new HashMap<>();
    private  Ds3TargetBlobPhysicalPlacement m_ds3TargetBlobPhysicalPlacement;
    private  PublicCloudBlobSupport< ?, ?, ? > m_azureSupport;
    private  PublicCloudBlobSupport< ?, ?, ? > m_s3Support;
    private final static Logger LOG = Logger.getLogger( GetByPhysicalPlacementDataOrderingStrategy.class );
    private  boolean m_failIfBlobsMissing = false;
    private  boolean m_generateNotifications = false;
    private  Set< UUID > m_blobsToRead = new HashSet<>() ;
    private boolean m_readByBlobds = false;
    // For larger sets than this, beanPropertyEqualsOneOf can crash postgres
    private static final int SINGLE_TRANSACTION_MAX = 10000;

}
