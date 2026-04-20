/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainFailureService;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

public enum WritesStalledSupport {
    INSTANCE;
    
    /**
     * @param tapePartitionId if stalled trying to allocate tape; or null if trying to allocate pool
     */
    synchronized public void stalled( final UUID tapePartitionId, final UUID storageDomainId, final BeansServiceManager serviceManager )
    {
        updateStorageDomainsIfNecessary( storageDomainId, serviceManager );
        if ( !m_stalledStorageDomains.containsKey( storageDomainId ) )
        {
            m_stalledStorageDomains.put( storageDomainId, new HashSet< UUID >() );
        }
        if ( m_stalledStorageDomains.get( storageDomainId ).contains( tapePartitionId ) )
        {
            return;
        }
        if (m_storageDomains.containsKey(storageDomainId) && m_storageDomains.get(storageDomainId).contains(tapePartitionId)) {
            m_stalledStorageDomains.get( storageDomainId ).add( tapePartitionId );
            updateStallState( storageDomainId, serviceManager );
        }
    }
    

    /**
     * @param tapePartitionId if not stalled trying to allocate tape; or null if trying to allocate pool
     */
    synchronized public void notStalled( final UUID tapePartitionId, final UUID storageDomainId, final BeansServiceManager serviceManager )
    {
        updateStorageDomainsIfNecessary( storageDomainId, serviceManager );
        if ( !m_stalledStorageDomains.containsKey( storageDomainId )
                || !m_stalledStorageDomains.get( storageDomainId ).contains( tapePartitionId ) )
        {
            return;
        }
        m_stalledStorageDomains.get( storageDomainId ).remove( tapePartitionId );
        updateStallState( storageDomainId, serviceManager );
    }
    
    
    private void updateStorageDomainsIfNecessary( final UUID storageDomainId, final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Storage domain id", storageDomainId );
        if ( 0 == m_durationSinceStorageDomainsUpdated.getElapsedSeconds() 
                && m_storageDomains.containsKey( storageDomainId ) )
        {
            return;
        }
        
        m_storageDomains.clear();
        for ( final StorageDomain storageDomain 
                : serviceManager.getRetriever( StorageDomain.class ).retrieveAll().toSet() )
        {
            m_storageDomains.put( storageDomain.getId(), new HashSet< UUID >() );
        }
        for ( final StorageDomainMember member
                : serviceManager.getRetriever( StorageDomainMember.class ).retrieveAll().toSet() )
        {
            //NOTE: if the domain has any pool members, this line will add a "null" instead of a partition ID. this null
            //is used explicitly as an indication of pool member(s). See also null being used as the tape partition ID in
            //the stalled() and notStalled() methods, which likewise treat it as a blanket ID for all pool partitions in
            //a given storage domain.
            m_storageDomains.get( member.getStorageDomainId() ).add( member.getTapePartitionId() );
        }
        m_durationSinceStorageDomainsUpdated.reset();
        
        if ( !m_storageDomains.containsKey( storageDomainId ) )
        {
            throw new IllegalArgumentException( "Storage domain " + storageDomainId + " does not exist." );
        }
    }
    
    
    private void updateStallState( final UUID storageDomainId,  final BeansServiceManager serviceManager )
    {
        final boolean stalled = m_stalledStorageDomains.get( storageDomainId ).containsAll(
                m_storageDomains.get( storageDomainId ) );
        String msg = ( stalled ) ?
                "Writes STALLED"
                : "Writes are not stalled";
        msg += " for storage domain " + storageDomainId + " (" 
               + m_stalledStorageDomains.get( storageDomainId ).size() + " of "
               + m_storageDomains.get( storageDomainId ).size() + " members stalled).";

        final ActiveFailures activeFailures =
                serviceManager.getService( StorageDomainFailureService.class ).startActiveFailures(
                        storageDomainId,
                        StorageDomainFailureType.WRITES_STALLED_DUE_TO_NO_FREE_MEDIA_REMAINING );
        if ( stalled )
        {
            LOG.warn( msg );
            final List< String > mediasToAdd = new ArrayList<>();
            for ( final StorageDomainMember member 
                    : serviceManager.getRetriever( StorageDomainMember.class ).retrieveAll(
                            StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ).toSet() )
            {
                if ( null == member.getPoolPartitionId() )
                {
                    final TapePartition tp = serviceManager.getRetriever( TapePartition.class ).attain(
                            member.getTapePartitionId() );
                    if ( WritePreferenceLevel.NEVER_SELECT != member.getWritePreference() )
                    {
                        mediasToAdd.add( member.getTapeType() + " tapes in partition " + tp.getName() );
                    }
                }
                else
                {
                    final PoolPartition pp = serviceManager.getRetriever( PoolPartition.class ).attain(
                            member.getPoolPartitionId() );
                    mediasToAdd.add( pp.getType() + " pools in partition " + pp.getName() );
                }
            }
            Collections.sort( mediasToAdd );
            
            final String message = 
                    "No media is available to allocate to storage domain " 
                    + serviceManager.getRetriever( StorageDomain.class ).attain( storageDomainId ).getName()
                    + " at this time.  A later retry might succeed, but this generally means that additional "
                    + "media must be added.  Consider adding any of the following: " + mediasToAdd;
            activeFailures.add( message );
        }
        else
        {
            LOG.info( msg );
        }
        activeFailures.commit();
    }
    private final Duration m_durationSinceStorageDomainsUpdated = new Duration();
    private final Map< UUID, Set< UUID > > m_stalledStorageDomains = new HashMap<>(); //map of storage domain ID's to sets of tape partition ID's
    private final Map< UUID, Set< UUID > > m_storageDomains = new HashMap<>(); //map of storage domain ID's to sets of tape partition ID's
    private final static Logger LOG = Logger.getLogger( WritesStalledSupport.class );
}
