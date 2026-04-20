/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.lang.CollectionFactory;

final class TapeServiceImpl extends BaseService< Tape > implements TapeService
{
    TapeServiceImpl()
    {
        super( Tape.class );
    }
    
    
    public void transistState(
            final Tape tape, 
            final TapeState newState )
    {
        if ( tape.getState().equals( newState ) )
        {
            return;
        }
        
        TapeState previousState = null;
        if ( newState.isPreviousStateTracked() )
        {
            previousState = tape.getState();
            if ( null != previousState && !previousState.canBePreviousState() )
            {
                previousState = tape.getPreviousState();
            }
        }
        if ( previousState == newState )
        {
            previousState = null;
        }
        
        tape.setState( newState );
        tape.setPreviousState( previousState );
        
        super.update( tape, Tape.PREVIOUS_STATE, Tape.STATE );
        if ( TapeState.FOREIGN == tape.getState() )
        {
            super.update( 
                    tape.setLastCheckpoint( null ).setTakeOwnershipPending( false ),
                    Tape.LAST_CHECKPOINT, Tape.TAKE_OWNERSHIP_PENDING );
        }
        if ( TapeState.EJECTED == tape.getState() || TapeState.LOST == tape.getState() )
        {
            super.update( tape.setPartitionId( null ), Tape.PARTITION_ID );
        }
    }
    
    
    public void updatePreviousState( final Tape tape, final TapeState newPreviousState )
    {
        if ( null == newPreviousState )
        {
            if ( tape.getState().isCancellableToPreviousState() )
            {
                throw new IllegalStateException( 
                        "State must track a previous state: " + tape.getState() );
            }
        }
        else
        {
            if ( !tape.getState().isPreviousStateTracked() )
            {
                throw new IllegalStateException( 
                        "State does not track a previous state: " + tape.getState() );
            }
            if ( !newPreviousState.canBePreviousState() )
            {
                throw new IllegalStateException(
                        "State cannot be a previous state: " + newPreviousState );
            }
        }
        super.update( tape.setPreviousState( newPreviousState ), Tape.PREVIOUS_STATE );
    }
    
    
    public void rollbackLastStateTransition( final Tape tape )
    {
        if ( !tape.getState().isPreviousStateTracked() )
        {
            throw new IllegalStateException( "Cannot cancel state " + tape.getState() );
        }
        if ( null == tape.getPreviousState() )
        {
            throw new IllegalStateException( "Tape does not have a previous state." );
        }
        
        tape.setState( tape.getPreviousState() );
        tape.setPreviousState( null );
        super.update( tape, Tape.PREVIOUS_STATE, Tape.STATE );
    }
    
    
    @Override
    public void update( final Tape bean, final String... propertiesToUpdate )
    {
        if ( CollectionFactory.toSet( propertiesToUpdate ).removeAll( 
                PROPERTIES_THAT_CANNOT_BE_EXPLICITLY_UPDATED ) )
        {
            throw new RuntimeException( 
                    "It is illegal to update explicitly: " + PROPERTIES_THAT_CANNOT_BE_EXPLICITLY_UPDATED );
        }
        super.update( bean, propertiesToUpdate );
    }


    @Override
    public void update(final WhereClause whereClause, final Consumer<Tape> beanUpdater, final String... propertiesToUpdate)
    {
        if ( CollectionFactory.toSet( propertiesToUpdate ).removeAll(
                PROPERTIES_THAT_CANNOT_BE_EXPLICITLY_UPDATED ) )
        {
            throw new RuntimeException(
                    "It is illegal to update explicitly: " + PROPERTIES_THAT_CANNOT_BE_EXPLICITLY_UPDATED );
        }
        super.update(whereClause, beanUpdater, propertiesToUpdate);
    }


    public void deassociateFromPartition( final Set< UUID > tapeIdsToDeassociateFromTheirPartition )
    {
        getDataManager().updateBeans( 
                CollectionFactory.toSet( Tape.PARTITION_ID, Tape.PREVIOUS_STATE, Tape.STATE ), 
                BeanFactory.newBean( Tape.class ).setPartitionId( null )
                .setPreviousState( TapeState.PENDING_INSPECTION ).setState( TapeState.LOST ),
                Require.beanPropertyEqualsOneOf( Identifiable.ID, tapeIdsToDeassociateFromTheirPartition ) );
    }
    
    
    public long [] getAvailableSpacesForBucket( final UUID bucketId, final UUID storageDomainId )
    {
        final List< Tape > tapes = retrieveAll(
                PersistenceTargetUtil.filterForWritableTapes(
                        PersistenceTargetUtil.getIsolatedBucketId( 
                                bucketId, 
                                storageDomainId,
                                getServiceManager() ), 
                        storageDomainId ) ).toList();
        Collections.sort( tapes, new BeanComparator<>( Tape.class, Tape.AVAILABLE_RAW_CAPACITY ) );
        
        final long [] retval = new long[ tapes.size() ];
        for ( int i = 0; i < retval.length; ++i )
        {
            retval[ i ] = ( null == tapes.get( i ).getAvailableRawCapacity() ) ? 
                    0
                    : Math.max(0, tapes.get( i ).getAvailableRawCapacity().longValue() - PersistenceTargetUtil.RESERVED_SPACE_ON_TAPE);
        }
        return retval;
    }
    
    
    public void updateDates( final UUID tapeId, final TapeAccessType accessType )
    {
        final Date date = new Date();
        final Tape tape = BeanFactory.newBean( Tape.class )
                .setLastAccessed( date ).setLastModified( date ).setLastVerified( date );
        tape.setId( tapeId );
        
        switch ( accessType )
        {
            case ACCESSED:
                update( tape, PersistenceTarget.LAST_ACCESSED );
                break;
            case MODIFIED:
                tape.setLastVerified( null );
                update( tape, 
                        PersistenceTarget.LAST_ACCESSED,
                        PersistenceTarget.LAST_MODIFIED, 
                        PersistenceTarget.LAST_VERIFIED );
                break;
            case VERIFIED:
                update( tape.setVerifyPending( null ), 
                        PersistenceTarget.LAST_ACCESSED, 
                        PersistenceTarget.LAST_VERIFIED,
                        Tape.VERIFY_PENDING );
                break;
            default:
                throw new UnsupportedOperationException( "No code for: " + accessType );
        }
    }
    
    
    public void updateAssignment( final UUID tapeId )
    {
    	try( final NestableTransaction transaction = getServiceManager().startNestableTransaction() )
    	{
	    	final TapeRM tape = new TapeRM( tapeId, getServiceManager() ); 
	    	if ( tape.getBlobTapes().isEmpty()
	    			&& null != tape.unwrap().getStorageDomainMemberId()
	    			&& !tape.getStorageDomainMember().getStorageDomain().isSecureMediaAllocation() )
	    	{
	    		LOG.warn( "Tape " + tape.getId() + " (" + tape.getBarCode()
	    				+ ") is no longer referenced by any blob tape"
	    				+ " records and can be unassigned from storage domain member "
						+ tape.getStorageDomainMember() + ".");
	    		update( tape.unwrap().setStorageDomainMemberId( null ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
	    	}
    	}
    }
    
    
    private final static Set< String > PROPERTIES_THAT_CANNOT_BE_EXPLICITLY_UPDATED =
            CollectionFactory.toSet( Tape.STATE, Tape.PREVIOUS_STATE );
}
