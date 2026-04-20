/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.*;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.ReconcilingTapeEnvironmentManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.ImportExportConfiguration;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveState;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.orm.StorageDomainMemberRM;
import com.spectralogic.s3.common.dao.orm.TapePartitionRM;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeLibraryService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.tape.domain.BasicTapeInformation;
import com.spectralogic.s3.common.rpc.tape.domain.ElementAddressBlockInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeDriveInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeLibraryInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapePartitionInformation;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironmentManager;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

import static com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment.MISSING_BAR_CODE_PREFIX;

public final class TapeEnvironmentManagerImpl implements ReconcilingTapeEnvironmentManager
{
    public TapeEnvironmentManagerImpl(
            final BeansServiceManager serviceManager,
            final int minDriveCountPerPartition)
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        m_serviceManager = serviceManager;
        m_minDriveCountPerPartition = minDriveCountPerPartition;
    }


    @Override
    synchronized public void reconcileWith( final TapeEnvironmentInformation tapeEnvironment )
    {
        Validations.verifyNotNull( "Tape environment", tapeEnvironment );

        m_transaction = m_serviceManager.startTransaction();
        m_transactionDrivesRequiringCleaning = new HashSet<>();
        m_transactionImportExitSlots = new HashMap<>();
        m_transactionStorageSlots = new HashMap<>();
        m_transactionTapeElementAddresses = new HashMap<>();
        m_transactionTapesInOfflineDrives = new HashSet<>();
        m_transactionDriveElementAddresses = new HashMap<>();
        m_transactionPartitionDrives = new HashMap<>();
        m_transactionPartitionTapes = new HashMap<>();
        m_transactionQuiescedPartitions = new HashSet<>();
        final ActiveFailures failures = 
                m_serviceManager.getService( SystemFailureService.class ).startActiveFailures( 
                        SystemFailureType.RECONCILE_TAPE_ENVIRONMENT_FAILED );
        try
        {
            reconcileWithInternal( tapeEnvironment );
            m_transaction.getService( StorageDomainMemberService.class ).ensureWritePreferencesValid();

            final Set<UUID> tapesInQuiescedPartitions = BeanUtils.toMap(
                    m_transaction.getRetriever(Tape.class).retrieveAll(
                            Require.beanPropertyEqualsOneOf(
                                    Tape.PARTITION_ID,
                                    m_transactionQuiescedPartitions)).toSet()).keySet();
            final Set<UUID> drivesInQuiescedPartitions = BeanUtils.toMap(
                    m_transaction.getRetriever(TapeDrive.class).retrieveAll(
                            Require.beanPropertyEqualsOneOf(
                                    TapeDrive.PARTITION_ID,
                                    m_transactionQuiescedPartitions)).toSet()).keySet();
            LOG.info(tapesInQuiescedPartitions.size() + " tapes were in quiesced partitions.");

            m_transaction.commitTransaction();

            m_partitionTapes.keySet().retainAll(m_transactionQuiescedPartitions);
            m_partitionDrives.keySet().retainAll(m_transactionQuiescedPartitions);
            m_importExportSlots.keySet().retainAll(m_transactionQuiescedPartitions);
            m_storageSlots.keySet().retainAll(m_transactionQuiescedPartitions);

            m_tapeElementAddresses.keySet().retainAll(tapesInQuiescedPartitions);
            m_tapesInOfflineDrives.retainAll(tapesInQuiescedPartitions);

            m_driveElementAddresses.keySet().retainAll(drivesInQuiescedPartitions);
            m_drivesRequiringCleaning.retainAll(drivesInQuiescedPartitions);

            m_drivesRequiringCleaning.addAll( m_transactionDrivesRequiringCleaning );
            m_importExportSlots.putAll( m_transactionImportExitSlots );
            m_storageSlots.putAll( m_transactionStorageSlots );
            m_tapeElementAddresses.putAll( m_transactionTapeElementAddresses );
            m_tapesInOfflineDrives.addAll( m_transactionTapesInOfflineDrives );
            m_driveElementAddresses.putAll( m_transactionDriveElementAddresses );
            m_partitionDrives.putAll( m_transactionPartitionDrives );
            m_partitionTapes.putAll( m_transactionPartitionTapes );
        }
        catch ( final RuntimeException ex )
        {
            failures.add( ex );
            throw ex;
        }
        finally
        {
            m_transaction.closeTransaction();
            failures.commit();
        }
    }
    
    
    private void reconcileWithInternal( final TapeEnvironmentInformation tapeEnvironment )
    {
        final Duration duration = new Duration();
    
        final Map< String, TapeLibrary > knownLibraries = getKnownLibraries();
        for ( final TapeLibraryInformation library : tapeEnvironment.getLibraries() )
        {
            final String serialNumber = library.getSerialNumber();
            final TapeLibrary dbLibrary;
            if ( knownLibraries.containsKey( serialNumber ) )
            {
                dbLibrary = knownLibraries.get( serialNumber );
            }
            else
            {
                LOG.info( "New tape library found: " + serialNumber );
                dbLibrary = BeanFactory.newBean( TapeLibrary.class );
                BeanCopier.copy( dbLibrary, library );
                m_transaction.getService( TapeLibraryService.class ).create( dbLibrary );
            }
            
            knownLibraries.remove( serialNumber );
            reconcilePartitions( dbLibrary, CollectionFactory.toSet( library.getPartitions() ) );
        }
        
        int numberOfLibraries = 0;
        int numberOfPartitions = 0;
        int numberOfDrives = 0;
        int numberOfTapes = 0;
        for ( final TapeLibraryInformation library : tapeEnvironment.getLibraries() )
        {
            numberOfLibraries += 1;
            for ( final TapePartitionInformation partition : library.getPartitions() )
            {
                numberOfPartitions += 1;
                numberOfDrives += partition.getDrives().length;
                numberOfTapes += partition.getTapes().length;
            }
        }
        
        for ( final TapeLibrary library : knownLibraries.values() )
        {
            LOG.info( "Tape library offline: " + library.getName() );
            reconcilePartitions( library, new HashSet<>() );
        }
        
        LOG.info( "Reconciled tape environment in " + duration + ".  There were " 
                  + numberOfLibraries + " libraries, " + numberOfPartitions + " partitions, "
                  + numberOfDrives + " drives, and " + numberOfTapes + " tapes." );
    }
    
    
    private Map< String, TapeLibrary > getKnownLibraries()
    {
        final Map< String, TapeLibrary > retval = new HashMap<>();
        
        for ( final TapeLibrary library 
                : m_transaction.getRetriever( TapeLibrary.class ).retrieveAll().toSet() )
        {
            retval.put( library.getSerialNumber(), library );
        }
        
        return retval;
    }
    
    
    private static < T > Set< T > union( Set< T > setA, Set< T > setB )
    {
        Set< T > tmp = new TreeSet<>( setA );
        tmp.addAll( setB );
        return tmp;
    }
    
    
    private static < T > Set< T > intersection( Set< T > setA, Set< T > setB )
    {
        Set< T > tmp = new TreeSet<>();
        for ( T x : setA )
        {
            if ( setB.contains( x ) )
            {
                tmp.add( x );
            }
        }
        return tmp;
    }
    
    
    private static < T > Set< T > difference( Set< T > setA, Set< T > setB )
    {
        Set< T > tmp = new TreeSet<>( setA );
        tmp.removeAll( setB );
        return tmp;
    }
    
    
    private static < T > Set< T > symmetricDifference( Set< T > setA, Set< T > setB )
    {
        Set< T > tmpA;
        Set< T > tmpB;
        
        tmpA = union( setA, setB );
        tmpB = intersection( setA, setB );
        return difference( tmpA, tmpB );
    }
    
    
    private void reconcilePartitions( 
            final TapeLibrary library,
            final Set< TapePartitionInformation > partitions )
    {
        Map< String, TapePartition > knownPartitions = getKnownPartitions();

        matchMissingPartitions( partitions, knownPartitions );
        knownPartitions = getKnownPartitions();
        final Map< String, TapePartitionInformation > discoveredPartitions = partitions.stream()
                                                                                       .collect( Collectors.toMap(
                                                                                               TapePartitionInformation::getSerialNumber,
                                                                                               Function.identity() ) );
        final AutoInspectMode autoInspectMode = m_transaction.getService( DataPathBackendService.class )
                .attain( Require.nothing() ).getAutoInspect();

        for ( final TapePartitionInformation partition : partitions )
        {
            final String serialNumber = partition.getSerialNumber();
            if ( knownPartitions.containsKey( serialNumber ) )
            {
                final TapePartition existingPartition = knownPartitions.get( serialNumber );
                final boolean hitAutoQuiesceCriteria = partition.getErrorMessage() != null ||
                        (partition.getTapes().length == 0 && expectedTapes(existingPartition.getId()).findAny().isPresent() );
                if ( existingPartition.getQuiesced() != Quiesced.YES && existingPartition.isAutoQuiesceEnabled() && hitAutoQuiesceCriteria ) {
                    autoQuiescePartition(existingPartition, partition.getErrorMessage());
                } else if ( existingPartition.getQuiesced() != Quiesced.YES  || autoInspectMode == AutoInspectMode.FULL) {
                    //Do not reconcile known partition if quiesced. This allows maintenance without requiring inspects.
                    reconcilePartition( existingPartition, partition );
                } else {
                    m_transactionQuiescedPartitions.add( existingPartition.getId() );
                }
            }
            else
            {
                LOG.info( "New tape partition found: " + partition.getSerialNumber() );
                final TapePartition newPartition = BeanFactory.newBean( TapePartition.class )
                        .setLibraryId( library.getId() );
                BeanCopier.copy( newPartition, partition );
                m_transaction.getService( TapePartitionService.class ).create( newPartition );
                reconcilePartition(newPartition, partition);
            }
        }
    
        for ( final Map.Entry< String, TapePartition > knownPartitionEntry : knownPartitions.entrySet() )
        {
            final TapePartition knownPartition = knownPartitionEntry.getValue();
            if ( knownPartition.getQuiesced() != Quiesced.YES || autoInspectMode == AutoInspectMode.FULL) {
                if (discoveredPartitions.containsKey(knownPartitionEntry.getKey())) {
                    if (null == knownPartition.getErrorMessage()) {
                        m_transaction.getService(TapePartitionService.class).update(
                                knownPartitionEntry.getValue()
                                        .setState(TapePartitionState.ONLINE),
                                TapePartition.STATE);
                    } else {
                        partitionDowned(knownPartitionEntry.getValue(), TapePartitionState.ERROR);
                    }
                } else if ( knownPartition.getQuiesced() != Quiesced.YES && knownPartition.isAutoQuiesceEnabled() ) {
                    autoQuiescePartition(knownPartition, null);
                } else {
                    partitionDowned(knownPartitionEntry.getValue(), TapePartitionState.OFFLINE);
                }
            } else {
                m_transactionQuiescedPartitions.add( knownPartition.getId() );
            }
        }
    }

    private Stream<Tape> expectedTapes(final UUID tapePartitionId) {
        //count all tapes that we were expecting, but only those at risk of being marked lost
        //if we included tapes that were not in danger of being marked lost, we might auto-quiesce without needing to
        //for example - if a lot of blank tapes suddenly disappear, we will simply delete them - no inspects or quiesce needed
        return getKnownTapes(tapePartitionId).values()
                .stream().filter(tape -> (null != tape) &&
                        //Tapes that are physically present, or were pending eject to EE from storage are still at risk of being marked lost
                        (tape.getState().isPhysicallyPresent() || tape.getState() == TapeState.EJECT_TO_EE_IN_PROGRESS) &&
                        //Tapes that are marked barcode missing are not at risk of being marked lost
                        (TapeState.BAR_CODE_MISSING != tape.getState() ) &&
                        //Tapes with no managed data are not at risk of being marked lost
                        (tape.getStorageDomainMemberId() != null) );
    }

    private void autoQuiescePartition(final TapePartition knownPartition, final String errorMessage) {
        if (null == errorMessage) {
            LOG.warn("Auto-quiescing tape partition: " + knownPartition);
        } else {
            LOG.warn("Auto-quiescing tape partition: " + knownPartition + " due to error: " + errorMessage);
        }
        m_transaction.getService( TapePartitionService.class ).update(
                knownPartition.setQuiesced( Quiesced.YES ),
                TapePartition.QUIESCED );
        final ActiveFailures autoQuiesceAlerts =
                m_transaction.getService( TapePartitionFailureService.class ).startActiveFailures(
                        knownPartition.getId(),
                        TapePartitionFailureType.AUTO_QUIESCED );
        if (errorMessage == null) {
            autoQuiesceAlerts.add(
                    "Tape partition \"" + knownPartition.getName()
                            + "\" unexpectedly went offline and has been automatically quiesced. ");
        } else {
            autoQuiesceAlerts.add(
                    "Tape partition \"" + knownPartition.getName()
                            + "\" has been automatically quiesced due to the following error: "
                            + errorMessage);
        }
        autoQuiesceAlerts.commit();
        m_transactionQuiescedPartitions.add( knownPartition.getId() );
    }


    private void matchMissingPartitions( final Set< TapePartitionInformation > partitions,
            final Map< String, TapePartition > knownPartitions )
    {
        final Set< String > knownPartitionSerialNumbers = knownPartitions.keySet();
        final Map< String, TapePartitionInformation > discoveredPartitions = partitions.stream()
                                                                                       .collect( Collectors.toMap(
                                                                                               TapePartitionInformation::getSerialNumber,
                                                                                               Function.identity() ) );
        
        final Set< String > partitionsDifference =
                symmetricDifference( discoveredPartitions.keySet(), knownPartitionSerialNumbers );
        if ( ( 0 != knownPartitions.size() ) && ( 0 != partitionsDifference.size() ) )
        {
            final Set< String > discoveredSerialPartitionNumbers = discoveredPartitions.keySet();
            final Set< String > newPartitions = new HashSet<>( discoveredSerialPartitionNumbers );
            newPartitions.removeAll( knownPartitionSerialNumbers );
            final Set< String > missingPartitions = new HashSet<>( knownPartitionSerialNumbers );
            missingPartitions.removeAll( discoveredSerialPartitionNumbers );
            
            for ( final String knownPartitionSerialNumber : missingPartitions )
            {
                final TapePartition knownPartition = knownPartitions.get( knownPartitionSerialNumber );
                final Set< String > knownBarcodes = getKnownTapes( knownPartition.getId() ).values()
                                                                                           .stream()
                                                                                           .map( Tape::getBarCode )
                                                                                           .collect(
                                                                                                   Collectors.toSet() );
                if ( 0 == knownBarcodes.size() )
                {
                    continue;
                }
                
                String foundPartitionSerialNumber = null;
                for ( final String newPartitionSerialNumber : newPartitions )
                {
                    final TapePartitionInformation newPartition = discoveredPartitions.get( newPartitionSerialNumber );
                    final Set< String > newBarcodes = Arrays.stream( newPartition.getTapes() )
                                                            .map( BasicTapeInformation::getBarCode )
                                                            .collect( Collectors.toSet() );
                    final Set< String > barcodeDifference = symmetricDifference( knownBarcodes, newBarcodes );
                    if ( 1 >= barcodeDifference.size() )
                    {
                        LOG.warn( "Partition " + knownPartition.getName() + "'s serial number has changed from " +
                                knownPartition.getSerialNumber() + " to " + newPartitionSerialNumber + "." );
                        m_transaction.getService( TapePartitionService.class )
                                     .update( knownPartition.setSerialNumber( newPartitionSerialNumber ),
                                             SerialNumberObservable.SERIAL_NUMBER );
                        foundPartitionSerialNumber = newPartitionSerialNumber;
                        break;
                    }
                }
                if ( null == foundPartitionSerialNumber )
                {
                    LOG.warn( "Known partition " + knownPartition.getName() + "'s did not match any new partitions." );
                    
                }
                else
                {
                    newPartitions.remove( foundPartitionSerialNumber );
                }
            }
        }
    }
    
    
    private void reconcilePartition(
            final TapePartition dbPartition,
            final TapePartitionInformation partition)
    {
        if ( !dbPartition.getName().equals( partition.getName() ) )
        {
        	LOG.info( "Partition \"" + dbPartition.getName() + "\" has changed name to \""
        			+ partition.getName() + "\".");
        	m_transaction.getService( TapePartitionService.class ).update(
        			dbPartition.setName( partition.getName() ),
        			NameObservable.NAME );
        }
        m_transaction.getService( TapePartitionService.class ).update(
                dbPartition.setErrorMessage( partition.getErrorMessage() )
                           .setImportExportConfiguration( partition.getImportExportConfiguration() ),
                ErrorMessageObservable.ERROR_MESSAGE, TapePartition.IMPORT_EXPORT_CONFIGURATION );
        m_transactionImportExitSlots.put(
                dbPartition.getId(), 
                getElementAddresses(
                        ElementAddressType.IMPORT_EXPORT, partition.getElementAddressBlocks() ) );
        m_transactionStorageSlots.put(
                dbPartition.getId(), 
                getElementAddresses(
                        ElementAddressType.STORAGE, partition.getElementAddressBlocks() ) );
        final Set< Integer > driveElementAddresses = getElementAddresses( ElementAddressType.TAPE_DRIVE, partition.getElementAddressBlocks() );
    
        final Map< Integer, Tape > tapesByElementAddress =
                reconcileTapes( dbPartition, CollectionFactory.toSet( partition.getTapes() ), driveElementAddresses );
        final TapeDriveType tapeDriveType = reconcileTapeDrives(
                dbPartition, 
                CollectionFactory.toSet( partition.getDrives() ),
                tapesByElementAddress );
        if ( tapeDriveType != dbPartition.getDriveType() )
        {
            m_transaction.getService( TapePartitionService.class ).update(
                    dbPartition.setDriveType( tapeDriveType ), 
                    TapePartition.DRIVE_TYPE );
        }
        
        if ( null != tapeDriveType )
        {
            final ActiveFailures tapeMediaTypeIncompatibleFailures =
                    m_transaction.getService( TapePartitionFailureService.class ).startActiveFailures(
                            dbPartition.getId(),
                            TapePartitionFailureType.TAPE_MEDIA_TYPE_INCOMPATIBLE );
            final Set< Tape > incompatibleTapes = 
                    m_transaction.getRetriever( Tape.class ).retrieveAll( Require.all( 
                            Require.beanPropertyEquals( Tape.PARTITION_ID, dbPartition.getId() ),
                            Require.not( Require.beanPropertyEqualsOneOf( 
                                    Tape.STATE, TapeState.getStatesThatAreNotPhysicallyPresent() ) ),
                            Require.not( Require.beanPropertyEqualsOneOf(
                                    Tape.TYPE, tapeDriveType.getSupportedTapeTypes() ) ) ) ).toSet();
            if ( !incompatibleTapes.isEmpty() )
            {
                final List< TapeType > incompatibleTypes = new ArrayList<>( 
                        BeanUtils.extractPropertyValues( incompatibleTapes, Tape.TYPE ) );
                Collections.sort( incompatibleTypes );
                final List< String > incompatibleTapeBarCodes = new ArrayList<>(
                        BeanUtils.extractPropertyValues( incompatibleTapes, Tape.BAR_CODE ) );
                Collections.sort( incompatibleTapeBarCodes );
                tapeMediaTypeIncompatibleFailures.add( 
                        "A tape drive of type " + tapeDriveType 
                        + " cannot read tapes of type " 
                        + incompatibleTypes
                        + ".  It is illegal to have a tape in a partition where the tape drives "
                        + "are unable to read the tape.  Please remove tapes " 
                        + incompatibleTapeBarCodes + "." );
                for ( final Tape incompatibleTape : incompatibleTapes )
                {
                    m_transaction.getService( TapeService.class ).transistState( 
                            incompatibleTape, TapeState.INCOMPATIBLE );
                }
            }
            tapeMediaTypeIncompatibleFailures.commit();
        }
        if ( 0 == m_transaction.getRetriever( Tape.class ).getCount( Require.all( 
                Require.beanPropertyEquals( Tape.PARTITION_ID, dbPartition.getId() ),
                Require.beanPropertyEquals( Tape.STATE, TapeState.EJECT_FROM_EE_PENDING ) ) ) )
        {
            m_transaction.getService( TapePartitionFailureService.class ).deleteAll(
                    dbPartition.getId(), 
                    TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED );
        }
    }
    
    
    private Set< Integer > getElementAddresses(
            final ElementAddressType type,
            final ElementAddressBlockInformation [] blocks )
    {
        final Set< Integer > retval = new HashSet<>();
        for ( final ElementAddressBlockInformation block : blocks )
        {
            if ( type != block.getType() )
            {
                continue;
            }
            for ( int i = block.getStartAddress(); i <= block.getEndAddress(); ++i )
            {
                retval.add( i );
            }
        }
        
        return retval;
    }
    
    
    private void partitionDowned( final TapePartition partition, final TapePartitionState newState )
    {
        LOG.warn( "Tape partition " + newState + ": " + partition );
        m_transaction.getService( TapePartitionService.class ).update( 
                partition.setState( newState ), 
                TapePartition.STATE );
        for ( final TapeDrive drive 
                : m_transaction.getRetriever( TapeDrive.class ).retrieveAll(
                        TapeDrive.PARTITION_ID, partition.getId() ).toSet() )
        {
            m_transaction.getService( TapeDriveService.class ).update(
                    drive.setState( TapeDriveState.OFFLINE ), TapeDrive.STATE );
        }
        final TapeService tapeService = m_transaction.getService( TapeService.class );
        getKnownTapes( partition.getId() ).values()
                .stream()
                .filter( tape -> ( null != tape ) && ( TapeState.EJECTED != tape.getState() ) &&
                        ( TapeState.BAR_CODE_MISSING != tape.getState() ) )
                .forEach( tape -> tapeService.transistState( tape, TapeState.LOST ) );
    }
    
    
    private Map< String, TapePartition > getKnownPartitions()
    {
        final Map< String, TapePartition > retval = new HashMap<>();
        
        for ( final TapePartition partition 
                : m_transaction.getRetriever( TapePartition.class ).retrieveAll().toSet() )
        {
            retval.put( partition.getSerialNumber(), partition );
        }
        
        return retval;
    }
    
    
    private Map< Integer, Tape > reconcileTapes(
            final TapePartition partition, 
            final Set< BasicTapeInformation > tapes,
            final Set< Integer > driveElementAddresses )
    {
        for ( final BasicTapeInformation tape : tapes )
        {
            final boolean hasBarCode = ( null != tape.getBarCode() );
            if ( !hasBarCode )
            {
                LOG.warn( "Tape at element address " + tape.getElementAddress() + " reported no bar code." );
                tape.setBarCode( MISSING_BAR_CODE_PREFIX + UUID.randomUUID());
                tape.setType( TapeType.FORBIDDEN );
            }
        }
    
        final Map< Integer, Tape > tapesByElementAddress = new HashMap<>();
        final Set< String > duplicateBarCodes = new HashSet<>();
        final Set< String > discoveredTapes = new HashSet<>();
        final Map< String, Tape > knownTapes = getKnownTapes( partition.getId() );
        final ActiveFailures duplicateBarCodeFailures =
                m_transaction.getService( TapePartitionFailureService.class ).startActiveFailures(
                        partition.getId(),
                        TapePartitionFailureType.DUPLICATE_TAPE_BAR_CODES_DETECTED );
        final ActiveFailures invalidPartitionFailures = m_transaction.getService( TapePartitionFailureService.class )
                                                                     .startActiveFailures( partition.getId(),
                                                                             TapePartitionFailureType
                                                                                     .TAPE_IN_INVALID_PARTITION );
        
        for ( final BasicTapeInformation tape : tapes )
        {
            final boolean hasBarCode = !( tape.getBarCode().startsWith( MISSING_BAR_CODE_PREFIX ) );
            final String barCode = tape.getBarCode();
            if ( discoveredTapes.contains( barCode ) )
            {
                duplicateBarCodes.add( barCode );
            }
            discoveredTapes.add( barCode );
            
            Tape knownTape = knownTapes.get( barCode );
            final boolean pendingQueuedEject =
                    ImportExportConfiguration.NOT_SUPPORTED == partition.getImportExportConfiguration() &&
                    null != knownTape &&
                    TapeState.EJECT_FROM_EE_PENDING == knownTape.getState();
                    
            final boolean tapeOffline = pendingQueuedEject || 
                    m_transactionImportExitSlots.get( partition.getId() )
                                                .contains( tape.getElementAddress() );
            if ( null == knownTape )
            {
                // It might be known in a different partition
                knownTape = m_transaction.getRetriever( Tape.class ).retrieve( Tape.BAR_CODE, barCode );
                if ( null != knownTape )
                {
                    knownTapes.put( barCode, knownTape );
                }
            }
            if ( null == knownTape )
            {
                final Tape dbTape = BeanFactory.newBean( Tape.class ).setPartitionId( partition.getId() );
                populateProperties( dbTape, tape );
                if ( !dbTape.getType().canContainData() )
                {
                    dbTape.setState( TapeState.NORMAL );
                }
                if ( tapeOffline )
                {
                    dbTape.setPreviousState( ( dbTape.getType().canContainData() ) ? 
                            TapeState.PENDING_INSPECTION : TapeState.NORMAL );
                    dbTape.setState( TapeState.OFFLINE );
                }
                if ( !hasBarCode )
                {
                    dbTape.setState( TapeState.BAR_CODE_MISSING );
                }
                m_transaction.getService( TapeService.class ).create( dbTape );
                knownTapes.put( dbTape.getBarCode(), dbTape );
            }
            else if ( knownTape.getState() == TapeState.LOST 
                    || knownTape.getState() == TapeState.EJECTED
                    || knownTape.getState() == TapeState.EJECT_FROM_EE_PENDING )
            {
                if ( tapeOffline && knownTape.getState() == TapeState.EJECT_FROM_EE_PENDING )
                {
                    LOG.info( "Waiting for tape " + knownTape.getId() + " to be physically ejected." );
                }
                else
                {
                    m_transaction.getService( TapeService.class ).update(
                            knownTape.setWriteProtected( false ).setEjectDate( null ).setEjectPending( null ),
                            Tape.WRITE_PROTECTED, Tape.EJECT_DATE, Tape.EJECT_PENDING );
                    // EMPROD-982: skip inspection for tapes that cannot contain data, such as cleaning tapes
                    TapeState newState;
                    if (tapeOffline)
                    {
                    	newState = TapeState.OFFLINE;
                    }
                    else
                    {
                        newState = ( knownTape.getType().canContainData() ) ? TapeState.PENDING_INSPECTION : TapeState.NORMAL;
                    }
                    m_transaction.getService( TapeService.class ).transistState(
                            knownTape, 
                            newState );
                }
            }
            else if ( tapeOffline )
            {
                if (TapeState.OFFLINE != knownTape.getState()
                        && TapeState.ONLINE_PENDING != knownTape.getState()
                        && TapeState.ONLINE_IN_PROGRESS != knownTape.getState()) {
                    m_transaction.getService(TapeService.class).transistState(
                            knownTape, TapeState.OFFLINE);
                }
            }
            else
            {
                if (TapeState.OFFLINE == knownTape.getState() || TapeState.ONLINE_PENDING == knownTape.getState() ||
                        TapeState.ONLINE_IN_PROGRESS == knownTape.getState())
                {

                    // EMPROD-982: skip inspection for tapes that cannot contain data, such as cleaning tapes
                    final TapeState newState = (knownTape.getType().canContainData()) ? TapeState.PENDING_INSPECTION : TapeState.NORMAL;
                    m_transaction.getService(TapeService.class).transistState(
                            knownTape,
                            newState);
                }
            }
            if ( null != knownTape && !partition.getId().equals( knownTape.getPartitionId() ) )
            {
                LOG.warn( "Tape " + knownTape.getId() + "'s partition association has changed from " 
                          + knownTape.getPartitionId() + " to " + partition.getId() + "." );
    
                if ( null == knownTape.getStorageDomainMemberId() )
                {
                    m_transaction.getService( TapeService.class )
                                 .update( knownTape.setPartitionId( partition.getId() ), Tape.PARTITION_ID );
                }
                else
                {
                    final StorageDomainMemberRM storageDomainMemberRm = new StorageDomainMemberRM(
                            knownTape.getStorageDomainMemberId(), m_serviceManager );
                    final UUID storageDomainId = storageDomainMemberRm.unwrap().getStorageDomainId();

                    String oldPartitionName;
                    UUID oldPartitionId = null;
                    if ( null != knownTape.getPartitionId() ) {
                        final TapePartition oldPartition =
                                new TapePartitionRM( knownTape.getPartitionId(), m_serviceManager ).unwrap();
                        oldPartitionName = oldPartition.getName();
                        oldPartitionId = oldPartition.getId();
                    } else {
                        oldPartitionName = storageDomainMemberRm.getTapePartition().getName();
                    }

                    UUID storageDomainMemberId = null; 
                    
                    try {
                    	storageDomainMemberId  = m_serviceManager.getService( StorageDomainService.class )
                            .selectAppropriateStorageDomainMember(
                            		knownTape.setPartitionId( partition.getId() ),
                            		storageDomainId );
                    }
                    catch ( final RuntimeException e )
                    {
                        final String failure =
                                "Tape " + knownTape.getBarCode() + "'s partition association would have changed " +
                                        "from " + oldPartitionName + " to " + partition.getName() +
                                        " but there are no appropriate storage domains to assign it to.  Either put " +
                                        "it back in its original partition or create a valid storage domain in the " +
                                        "new partition.";
                        invalidPartitionFailures.add( failure );
                        LOG.warn( failure, e );                    	
                    }
                    
                    if ( null != storageDomainMemberId )
                    {
                        if ( storageDomainMemberId != knownTape.getStorageDomainMemberId() )
                        {
                            knownTape.setStorageDomainMemberId( storageDomainMemberId );
                        }
                        m_transaction.getService( TapeService.class )
                                     .update( knownTape.setStorageDomainMemberId( storageDomainMemberId ),
                                             PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, Tape.PARTITION_ID );
                        LOG.warn( "Tape " + knownTape.getId() + "'s partition association has changed from " 
                                + oldPartitionId + " to " + partition.getId() + "." );
                    }
                }
            }
    
            if ( null != knownTape && !knownTape.getType().equals( tape.getType() ) )
            {
                LOG.log( ( TapeType.UNKNOWN == tape.getType() ) ? Level.INFO : Level.WARN,
                         "Tape " + knownTape.getId() + " (" + knownTape.getBarCode() + ") is of type "
                          + knownTape.getType() 
                          + " (we think), but is being reported at this moment as type " + tape.getType() 
                          + ".  Will not change tape type at this time, "
                          + "since this reported type didn't come from a tape drive." );
            }
    
            tapesByElementAddress.put( tape.getElementAddress(), knownTapes.get( barCode ) );
        }
        invalidPartitionFailures.commit();
        
        // Fail tapes that have duplicate bar codes in the system
        for ( final String duplicateBarCode : duplicateBarCodes )
        {
            LOG.warn( "Duplicate tape bar code detected: " + duplicateBarCode );
            duplicateBarCodeFailures.add( "There are multiple tapes with bar code: " + duplicateBarCode );
            for ( final Tape tape : m_transaction.getRetriever( Tape.class ).retrieveAll( Require.all( 
                    Require.beanPropertyEquals( Tape.PARTITION_ID, partition.getId() ),
                    Require.beanPropertyEquals( Tape.BAR_CODE, duplicateBarCode ) ) ).toSet() )
            {
                LOG.warn( "Tape cannot be used until duplicate tape bar code conflict fixed: " + tape );
                tapeDowned( tape );
            }
        }
        duplicateBarCodeFailures.commit();
        
        // Update tapes in database with correct element addresses
        final Set< UUID > allTapeIds = new HashSet<>();
        for ( final BasicTapeInformation tape : tapes )
        {
            if ( duplicateBarCodes.contains( tape.getBarCode() ) )
            {
                continue;
            }
            
            final Tape dbTape = knownTapes.get( tape.getBarCode() );
            allTapeIds.add( dbTape.getId() );
            populateProperties( dbTape, tape );
            m_transactionTapeElementAddresses.put( 
                    dbTape.getId(), tape.getElementAddress() );
            //Add all tapes in drive elements to offline list. We will remove them as we discover them in drives.
        	if ( driveElementAddresses.contains( tape.getElementAddress() ) )
        	{
        		m_transactionTapesInOfflineDrives.add( dbTape.getId() );
        	}
        }
    
        m_transactionPartitionTapes.put( partition.getId(), allTapeIds );
        
        // Find tapes that went missing
        for ( final Map.Entry< String, Tape > knownTapeEntry : knownTapes.entrySet() )
        {
            if ( discoveredTapes.contains( knownTapeEntry.getKey() ) )
            {
                continue;
            }
            
            tapeDowned( knownTapeEntry.getValue() );
        }
        
        return tapesByElementAddress;
    }
    
    private void populateProperties( final Tape tape, final BasicTapeInformation response )
    {
        BeanCopier.copy( tape, response );
    }
    
    
    private Map< String, Tape > getKnownTapes( final UUID partitionId )
    {
        final Map< String, Tape > retval = new HashMap<>();
        
        final WhereClause filter = Require.all( 
                Require.beanPropertyEquals( Tape.PARTITION_ID, partitionId ) );
        for ( final Tape tape : m_transaction.getRetriever( Tape.class ).retrieveAll( filter ).toSet() )
        {
            retval.put( tape.getBarCode(), tape );
        }
        
        return retval;
    }
    
    
    private void tapeDowned( final Tape tape )
    {
        final boolean ejectPending = ( TapeState.EJECT_FROM_EE_PENDING == tape.getState() );
        final boolean fullLogging = ( tape.getState().isPhysicallyPresent() || ejectPending );
        if ( fullLogging )
        {
            if ( ejectPending )
            {
                LOG.info( "Tape ejected: " + tape );
            }
            else
            {
                final String barcodeMessage;
                if ( TapeState.BAR_CODE_MISSING == tape.getState() )
                {
                    barcodeMessage = tape.getId() + " (unknown barcode)";
                }
                else
                {
                    barcodeMessage = tape.getBarCode();
                }
                m_transaction.getService( TapePartitionFailureService.class ).create(
                        tape.getPartitionId(), TapePartitionFailureType.TAPE_REMOVAL_UNEXPECTED,
                        "Tape " + barcodeMessage + " removed unexpectedly",
                        null );
                LOG.warn( "Tape lost: " + tape );
            }
        }
        
        m_transactionTapeElementAddresses.remove( tape.getId() );
        final int blobTapeCount = m_transaction.getRetriever( BlobTape.class ).getCount(
                BlobTape.TAPE_ID, tape.getId() );
        if ( null == tape.getStorageDomainMemberId() && 0 == blobTapeCount )
        {
            if ( 0 == m_transaction.getRetriever( TapeDrive.class ).getCount( 
                     Require.beanPropertyEquals( TapeDrive.TAPE_ID, tape.getId() ) ) )
            {
                m_transaction.getService( TapeService.class ).delete( tape.getId() );
                return;
            }
            LOG.warn( "Cannot delete tape since it's in a tape drive." );
        }
        else if ( fullLogging )
        {
            LOG.info( "Cannot delete tape since it's assigned to storage domain member "
                      + tape.getStorageDomainMemberId() + " and has " + blobTapeCount + " "
                      + BlobTape.class.getSimpleName() + "s." );
        }
        if ( TapeState.LOST == tape.getState() 
                || TapeState.EJECTED == tape.getState()
                || TapeState.BAR_CODE_MISSING == tape.getState() )
        {
            return;
        }
        
        m_transaction.getService( TapeService.class ).transistState(
                tape,
                ( ejectPending ) ? TapeState.EJECTED : TapeState.LOST );
        m_transaction.getService( TapeService.class ).update(
                tape.setEjectPending( null ).setEjectDate( new Date() ),
                Tape.EJECT_PENDING, Tape.EJECT_DATE );
        if ( null != tape.getStorageDomainMemberId() )
        {
            final StorageDomain storageDomain =
                    new TapeRM( tape, m_serviceManager ).getStorageDomainMember().getStorageDomain().unwrap();
            if ( !storageDomain.isMediaEjectionAllowed() )
            {
                m_transaction.getService( StorageDomainFailureService.class ).create(
                        storageDomain.getId(),
                        StorageDomainFailureType.ILLEGAL_EJECTION_OCCURRED, 
                        "Tape " + tape.getId() + " (" + tape.getBarCode() 
                        + ") was illegally exported from tape partition " + tape.getPartitionId()
                        + ".  It was allocated to storage domain " 
                        + storageDomain.getName() + ", which does not permit media export.  "
                        + "Tape exports should never be performed at the physical tape library.",
                        null );
            }
        }
    }
    
    
    private TapeDriveType reconcileTapeDrives( final TapePartition partition,
            final Set< TapeDriveInformation > tapeDrives, final Map< Integer, Tape > tapesByElementAddress )
    {
        final Set< String > discoveredTapeDrives = new HashSet<>();
        final Map< String, TapeDrive > knownTapeDrives = getKnownTapeDrives( partition.getId() );
        TapeDriveType bestTapeDriveType = TapeDriveType.values()[ 0 ];
        final Set< TapeDriveType > knownDriveTypes = new HashSet<>();
        final Set< TapeDriveType > newDriveTypes = new HashSet<>();
        
        for ( final TapeDriveInformation tapeDrive : tapeDrives )
        {
            if ( tapeDrive.getType().ordinal() > bestTapeDriveType.ordinal() )
            {
                bestTapeDriveType = tapeDrive.getType();
            }
        }
        final ActiveFailures tapeDriveTypeMismatches =
                m_transaction.getService( TapePartitionFailureService.class ).startActiveFailures(
                        partition.getId(),
                        TapePartitionFailureType.TAPE_DRIVE_TYPE_MISMATCH );
        final ActiveFailures tapeDrivesInError =
                m_transaction.getService( TapePartitionFailureService.class ).startActiveFailures(
                        partition.getId(),
                        TapePartitionFailureType.TAPE_DRIVE_IN_ERROR );
        final ActiveFailures tapeDrivesMissing =
                m_transaction.getService( TapePartitionFailureService.class ).startActiveFailures(
                        partition.getId(),
                        TapePartitionFailureType.TAPE_DRIVE_MISSING );
        m_transactionPartitionDrives.put( partition.getId(), new HashSet<>() );
        for ( final TapeDriveInformation tapeDrive : tapeDrives )
        {
            if ( null == tapeDrive.getForceTapeRemoval() )
            {
                tapeDrive.setForceTapeRemoval( Boolean.FALSE );
            }
            if ( null == tapeDrive.getCleaningRequired() )
            {
                tapeDrive.setCleaningRequired( Boolean.FALSE );
            }
            
            final String serialNumber = tapeDrive.getSerialNumber();
            discoveredTapeDrives.add( serialNumber );
            TapeDrive knownTapeDrive = knownTapeDrives.get( serialNumber );
    
            if ( null == knownTapeDrive )
            {
                knownTapeDrive = m_transaction.getRetriever( TapeDrive.class )
                                              .retrieve( SerialNumberObservable.SERIAL_NUMBER, serialNumber );
        
                if ( null != knownTapeDrive )
                {
                    m_transaction.getService( TapeDriveService.class )
                                 .update( knownTapeDrive.setPartitionId( partition.getId() ), TapeDrive.TYPE );
                }
            }
    
            final TapeDrive dbTapeDrive;
            if ( null != knownTapeDrive )
            {
                dbTapeDrive = knownTapeDrive;
                if ( dbTapeDrive.getType()
                                .equals( tapeDrive.getType() ) )
                {
                    knownDriveTypes.add( tapeDrive.getType() );
                }
                else
                {
                    LOG.warn(
                            "Tape drive " + dbTapeDrive.getId() + "'s type has changed from " + dbTapeDrive.getType() +
                                    " to " + tapeDrive.getType() + "." );
                    m_transaction.getService( TapeDriveService.class )
                                 .update( dbTapeDrive.setType( tapeDrive.getType() ), TapeDrive.TYPE );
                    newDriveTypes.add( tapeDrive.getType() );
                }
                if ( !tapeDrive.getMfgSerialNumber().equals( dbTapeDrive.getMfgSerialNumber() ) )
                {
                    LOG.warn( "Tape drive " + dbTapeDrive.getId() + "'s mfg serial has changed from " +
                            dbTapeDrive.getMfgSerialNumber() + " to " + tapeDrive.getMfgSerialNumber() + "." );
                    m_transaction.getService( TapeDriveService.class ).update(
                            dbTapeDrive.setMfgSerialNumber( tapeDrive.getMfgSerialNumber() ),
                            TapeDrive.MFG_SERIAL_NUMBER );
                }
                m_transaction.getService( TapeDriveService.class )
                             .update( dbTapeDrive.setCleaningRequired( tapeDrive.getCleaningRequired() ),
                                     TapeDrive.CLEANING_REQUIRED );
            }
            else
            {
                dbTapeDrive = BeanFactory.newBean( TapeDrive.class )
                                         .setPartitionId( partition.getId() );
                BeanCopier.copy( dbTapeDrive, tapeDrive );
                m_transaction.getService( TapeDriveService.class )
                             .create( dbTapeDrive );
                newDriveTypes.add( tapeDrive.getType() );
            }
    
            m_transactionDriveElementAddresses.put( dbTapeDrive.getId(), tapeDrive.getElementAddress() );
            m_transactionPartitionDrives.get( partition.getId() ).add( dbTapeDrive.getId() );
            if ( Boolean.TRUE.equals( tapeDrive.getCleaningRequired() ) )
            {
                //Here we count drive cleans (either successful or failed) from the past 24 hours. If there are >= 3,
                //we won't clean the drive.
                if ( 3 > m_transaction.getService( TapeFailureService.class ).getCount( Require.all(
                        Require.beanPropertyGreaterThan(
                                Failure.DATE,
                                new Date( System.currentTimeMillis() - 3600 * 1000L * 24 ) ),
                        Require.beanPropertyEquals(
                                TapeFailure.TAPE_DRIVE_ID, dbTapeDrive.getId() ),
                        Require.beanPropertyEqualsOneOf(
                                Failure.TYPE,
                                TapeFailureType.DRIVE_CLEAN_FAILED, TapeFailureType.DRIVE_CLEANED ) ) ) )
                {
                    m_transactionDrivesRequiringCleaning.add( dbTapeDrive.getId() );
                }
                else
                {
                    LOG.warn( "Tape drive " + dbTapeDrive.getId()
                              + " has requested too many cleans recently." );
                    tapeDrive.setErrorMessage(
                            "Drive has requested too many cleans recently (something is wrong with it)." );
                }
            }
            
            UUID tapeId = null;
            if ( tapesByElementAddress.containsKey( tapeDrive.getElementAddress() ) )
            {
                final Tape tape = m_transaction.getService( TapeService.class ).attain(
                        tapesByElementAddress.get( tapeDrive.getElementAddress() )
                                             .getId() );
                tapeId = tape.getId();
                if ( tape.getBarCode()
                         .startsWith( MISSING_BAR_CODE_PREFIX ) && !tapeDrive.getForceTapeRemoval() )
                {
                    if ( null == tapeDrive.getErrorMessage() )
                    {
                        tapeDrive.setForceTapeRemoval( Boolean.TRUE );
                        LOG.warn( "Tape in " + dbTapeDrive + " has no bar code.  "
                                + "Will try to determine the bar code by forcing its removal to storage." );
                    }
                    else
                    {
                        LOG.warn( "Tape in " + dbTapeDrive + " has no bar code, but the drive has an error "
                                  + " reported on it, so cannot auto-set the force tape removal flag." );
                    }
                }
            }
    
            dbTapeDrive.setForceTapeRemoval( tapeDrive.getForceTapeRemoval() );
            dbTapeDrive.setErrorMessage(
                    ( dbTapeDrive.isForceTapeRemoval() ) ? null : tapeDrive.getErrorMessage() );
            m_transaction.getService( TapeDriveService.class ).update(
                    dbTapeDrive.setTapeId( tapeId )
                               .setState( ( bestTapeDriveType == dbTapeDrive.getType() ) ?
                                     ( null == dbTapeDrive.getErrorMessage() ) ?
                                             TapeDriveState.NORMAL
                                             : TapeDriveState.ERROR
                                     : TapeDriveState.NOT_COMPATIBLE_IN_PARTITION_DUE_TO_NEWER_TAPE_DRIVES ),
                    ErrorMessageObservable.ERROR_MESSAGE, TapeDrive.TAPE_ID, TapeDrive.STATE,
                    TapeDrive.FORCE_TAPE_REMOVAL );
            m_transactionTapesInOfflineDrives.remove( tapeId );
            if ( TapeDriveState.NOT_COMPATIBLE_IN_PARTITION_DUE_TO_NEWER_TAPE_DRIVES
                    == dbTapeDrive.getState() )
            {
                tapeDriveTypeMismatches.add(
                        "Tape drive " + dbTapeDrive.getId() + " (" + dbTapeDrive.getSerialNumber()
                        + ") is not of type " + bestTapeDriveType
                        + ".  Tape drive types cannot be mixed in a single partition." );
            }
            if ( null != dbTapeDrive.getErrorMessage() )
            {
                tapeDrivesInError.add(
                        "Tape drive " + dbTapeDrive + " (" + dbTapeDrive.getSerialNumber()
                        + ") is in error: " + dbTapeDrive.getErrorMessage() );
            }
        }
    
        if ( !knownDriveTypes.containsAll( newDriveTypes ) )
        {
            newDriveTypes.removeAll( knownDriveTypes );
            /*
             * The only way we can get INCOMPATIBLE tapes is if we try to format them but cannot because of drive
             * generational problems.  This stanza allows the tape to be re-inspected if a new drive type was added
             * that can write to the INCOMPATIBLE tape.
             */
            tapesByElementAddress.values()
                                 .stream()
                                 .filter( tape -> tape.getState() == TapeState.INCOMPATIBLE )
                                 .filter( tape -> newDriveTypes.stream()
                                                               .anyMatch(
                                                                       tapeDriveType -> tapeDriveType.isWriteSupported(
                                                                               tape.getType() ) ) )
                                 .forEach( tape -> m_transaction.getService( TapeService.class )
                                                                .transistState( tape, TapeState.PENDING_INSPECTION ) );
        }

        final ActiveFailures notEnoughDrivesError =
                m_transaction.getService( TapePartitionFailureService.class ).startActiveFailures(
                        partition.getId(),
                        TapePartitionFailureType.MINIMUM_DRIVE_COUNT_NOT_MET );
        if ( m_minDriveCountPerPartition > tapeDrives.size() )
        {
            notEnoughDrivesError.add(
                    "Every partition must have at least " + m_minDriveCountPerPartition + " tape drives." );
        }
        tapeDriveTypeMismatches.commit();
        tapeDrivesInError.commit();
        notEnoughDrivesError.commit();
        
        for ( final Map.Entry< String, TapeDrive > knownTapeDriveEntry : knownTapeDrives.entrySet() )
        {
            if ( discoveredTapeDrives.contains( knownTapeDriveEntry.getKey() ) )
            {
                continue;
            }
            
            final TapeDrive dbTapeDrive = knownTapeDriveEntry.getValue();
            tapeDriveDowned( dbTapeDrive );
            tapeDrivesMissing.add( "Tape drive " + dbTapeDrive.getSerialNumber() + " (MFG SN " + dbTapeDrive.getMfgSerialNumber() + ") is missing." );
        }
        tapeDrivesMissing.commit();
        
        return ( tapeDrives.isEmpty() ) ? null : bestTapeDriveType;
    }
    
    
    private Map< String, TapeDrive > getKnownTapeDrives( final UUID partitionId )
    {
        final Map< String, TapeDrive > retval = new HashMap<>();
        
        for ( final TapeDrive tapeDrive : m_transaction.getRetriever( TapeDrive.class ).retrieveAll(
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, partitionId ) ).toSet() )
        {
            retval.put( tapeDrive.getSerialNumber(), tapeDrive );
        }
        
        return retval;
    }
    
    
    private void tapeDriveDowned( final TapeDrive tapeDrive )
    {
        LOG.warn( "Tape drive down: " + tapeDrive );
        m_transactionDriveElementAddresses.remove( tapeDrive.getId() );
        m_transaction.getService( TapeDriveService.class ).update(
                tapeDrive.setState( TapeDriveState.OFFLINE ).setTapeId( null ), 
                TapeDrive.STATE, TapeDrive.TAPE_ID );
    }
    
    
    @Override
    synchronized public int getTapeElementAddress( final UUID tapeId )
    {
        if ( !m_tapeElementAddresses.containsKey( tapeId ) )
        {
            throw new IllegalStateException( "Tape " + tapeId + " is unknown." );
        }
        return m_tapeElementAddresses.get( tapeId );
    }
    
    
    @Override
    synchronized public Set< UUID > getTapesInOfflineDrives()
    {
        return m_tapesInOfflineDrives;
    }
    
    
    @Override
    synchronized public int getDriveElementAddress( final UUID driveId )
    {
        if ( !m_driveElementAddresses.containsKey( driveId ) )
        {
            throw new IllegalStateException( "Drive " + driveId + " is unknown." );
        }
        return m_driveElementAddresses.get( driveId );
    }
    
    
    @Override
    synchronized public boolean isSlotAvailable( final UUID partitionId, final ElementAddressType slotType )
    {
        try
        {
            getAvailableSlots( partitionId, slotType );
            return true;
        }
        catch ( final IllegalStateException ex )
        {
            LOG.debug( "No slots available.", ex );
            return false;
        }
    }
    
    
    @Override
    synchronized public int moveTapeSlotToSlot(
            final UUID tapeId, 
            final ElementAddressType destinationSlotType )
    {
        verifyTapeNotInDrive( tapeId );

        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        final TapePartition partition =
                m_serviceManager.getRetriever( TapePartition.class ).attain( tape.getPartitionId() );
        if ( getAllSlots( tape.getPartitionId(), destinationSlotType ).contains( getTapeElementAddress( tapeId ) ) )
        {
            throw new IllegalArgumentException(
                    "Tape already in slot type " + destinationSlotType + ": " + tape );
        }
        if ( ElementAddressType.STORAGE == destinationSlotType )
        {
            verifyNoOvercommittingOfStorageSlots( tape.getPartitionId(), 1 );
        }
        
        final Integer dest = getAvailableSlots( tape.getPartitionId(), destinationSlotType ).get( 0 );
        final Integer src = m_tapeElementAddresses.get( tapeId );
        if ( ImportExportConfiguration.NOT_SUPPORTED == partition.getImportExportConfiguration() &&
                ElementAddressType.IMPORT_EXPORT == destinationSlotType )
        {
            LOG.info( "Tape " + tapeId + "(" + tape.getBarCode() + ") will remain in slot "
                    + src + " due to queued eject.");
        }
        else
        {
            m_tapeElementAddresses.put( tapeId, dest );
            LOG.info( "Tape environment updated for slot-to-slot move of tape " + tapeId
                    + "(" + tape.getBarCode() + ") (" + src + " -> " + dest + ")." );
        }
        return dest;
    }
    
    
    @Override
    synchronized public void moveTapeSlotToSlot( final UUID tapeId, final int dest )
    {
        verifyTapeNotInDrive( tapeId );

        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        if ( !getAllAvailableSlotsForAllElementAddressTypes( tape.getPartitionId() ).contains( dest ) )
        {
            throw new IllegalArgumentException(
                    "Slot " + dest + " does not exist or is unavailable." );
        }
        if ( getAllSlots( tape.getPartitionId(), ElementAddressType.STORAGE ).contains( dest ) )
        {
            verifyNoOvercommittingOfStorageSlots( tape.getPartitionId(), 1 );
        }
        
        //NOTE: we do not address queued ejects here as we do in the other slotToSlot call, as this
        //call is only used by move rollbacks 
        final Integer src = m_tapeElementAddresses.get( tapeId );
        m_tapeElementAddresses.put( tapeId, dest );
        LOG.info( "Tape environment updated for slot-to-slot move of tape " + tapeId
                  + "  (" + src + " -> " + dest + ")." );
    }
    
    
    private void verifyNoOvercommittingOfStorageSlots(
            final UUID partitionId,
            final int numberOfAdditionalStorageSlotsNeeded )
    {
        final int numTapesInDrives = m_serviceManager.getRetriever( TapeDrive.class ).getCount( Require.all( 
                Require.beanPropertyEquals( TapeDrive.PARTITION_ID, partitionId ),
                Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ) );
        final int numSlotsAvailable = getAvailableSlots( partitionId, ElementAddressType.STORAGE ).size();
        if ( numSlotsAvailable - numTapesInDrives < numberOfAdditionalStorageSlotsNeeded )
        {
            throw new IllegalStateException(
                    "Insufficient storage slots in partition.  There are " + numSlotsAvailable 
                    + " storage slots available, " + numTapesInDrives
                    + " of which are needed for tapes loaded in drives, so can't allocate " 
                    + numberOfAdditionalStorageSlotsNeeded + " more storage slots." );
        }
    }
    
    
    @Override
    synchronized public void moveTapeToDrive( final UUID tapeId, final TapeDrive drive )
    {
        Validations.verifyNotNull( "Tape drive", drive );
        verifyTapeNotInDrive( tapeId );
        if ( null != drive.getTapeId() )
        {
            throw new IllegalArgumentException( 
                    "Cannot move a tape into a drive that already has a tape in it." );
        }
        
        verifyTapeAndDriveInSamePartition( drive, tapeId );
        final Integer src = m_tapeElementAddresses.get( tapeId );
        final Integer dest = m_driveElementAddresses.get( drive.getId() );
        m_tapeElementAddresses.put( tapeId, dest );
        m_serviceManager.getService( TapeDriveService.class ).update(
                drive.setTapeId( tapeId ),
                TapeDrive.TAPE_ID );
        LOG.info( "Tape environment updated for loading tape " + tapeId
                  + " into drive " + drive.getId() + " (" + src + " -> " + dest + ")." );
    }
    
    
    private void verifyTapeNotInDrive( final UUID tapeId )
    {
        Validations.verifyNotNull( "Tape id", tapeId );
        final TapeDrive drive = 
                m_serviceManager.getRetriever( TapeDrive.class ).retrieve( TapeDrive.TAPE_ID, tapeId );
        if ( null == drive )
        {
            return;
        }
        throw new IllegalStateException( "Tape " + tapeId + " is loaded in drive " + drive + "." );
    }
    
    
    @Override
    synchronized public int moveTapeFromDrive(
            final TapeDrive drive, 
            final ElementAddressType destinationSlotType )
    {
        Validations.verifyNotNull( "Tape drive", drive );
        Validations.verifyNotNull( "Tape id", drive.getTapeId() );
        
        final UUID tapeId = drive.getTapeId();
        final Integer dest = getAvailableSlots( drive.getPartitionId(), destinationSlotType ).get( 0 );
        m_tapeElementAddresses.put( tapeId, dest );
        m_serviceManager.getService( TapeDriveService.class ).update(
                drive.setTapeId( null ),
                TapeDrive.TAPE_ID );
        LOG.info( "Tape environment updated for unloading tape " + tapeId
                  + " from drive " + drive.getId() + " (" + getDriveElementAddress( drive.getId() ) + " -> "
                  + dest + ")." );
        return dest;
    }
    
    
    @Override
    synchronized public void moveTapeDriveToDrive( final TapeDrive src, final TapeDrive dest )
    {
        Validations.verifyNotNull( "Source tape drive", src );
        Validations.verifyNotNull( "Destination tape drive", dest );
        if ( null == src.getTapeId() )
        {
            throw new IllegalStateException( "Source drive cannot be empty." );
        }
        if ( null != dest.getTapeId() )
        {
            throw new IllegalStateException(
                    "Destination drive must be empty, but contained tape: "
                    + dest.getTapeId() );
        }
        
        final UUID tapeId = src.getTapeId();
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getService( TapeDriveService.class ).update(
                    src.setTapeId( null ),
                    TapeDrive.TAPE_ID );
            transaction.getService( TapeDriveService.class ).update(
                    dest.setTapeId( tapeId ),
                    TapeDrive.TAPE_ID );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        LOG.warn( "Tape environment updated for drive-to-drive move of tape " + tapeId
                  + " from drive " + src.getId() + " to drive " + dest.getId()
                  + " (" + getDriveElementAddress( src.getId() ) + " -> "
                  + getDriveElementAddress( dest.getId() ) + ")." );
    }
    
    
    private Set< Integer > getAllAvailableSlotsForAllElementAddressTypes( final UUID partitionId )
    {
        final Set< Integer > retval = new HashSet<>();
        try
        {
            retval.addAll( getAvailableSlots( partitionId, ElementAddressType.STORAGE ) );
        }
        catch ( final IllegalStateException ex )
        {
            LOG.debug( "No slots available of type.", ex );
        }
        try
        {
            retval.addAll( getAvailableSlots( partitionId, ElementAddressType.IMPORT_EXPORT ) );
        }
        catch ( final IllegalStateException ex )
        {
            LOG.debug( "No slots available of type.", ex );
        }
        
        return retval;
    }
    
    
    private List< Integer > getAvailableSlots( 
            final UUID partitionId, 
            final ElementAddressType slotType )
    {
        Validations.verifyNotNull( "Partition id", partitionId );
        final Set< Integer > slots = getAllSlots( partitionId, slotType );
        Validations.verifyNotNull( "Slots", slots );
        
        final List< Integer > retval = new ArrayList<>( slots );
        final Map< UUID, Tape > tapes = 
                BeanUtils.toMap( m_serviceManager.getRetriever( Tape.class ).retrieveAll( Require.all( 
                      Require.beanPropertyEquals( Tape.PARTITION_ID, partitionId ),
                      Require.beanPropertyEqualsOneOf( Identifiable.ID, m_tapeElementAddresses.keySet() ) ) )
                      .toSet() );
        for ( final UUID tapeId : tapes.keySet() )
        {
            retval.remove( m_tapeElementAddresses.get( tapeId ) );
        }
        
        Collections.sort( retval );
        if ( retval.isEmpty() )
        {
            if ( ElementAddressType.IMPORT_EXPORT == slotType
                    && 0 < m_serviceManager.getRetriever( TapePartitionFailure.class ).getCount( Require.all( 
                            Require.beanPropertyEquals( TapePartitionFailure.PARTITION_ID, partitionId ),
                            Require.beanPropertyEquals( 
                                    Failure.TYPE, 
                                    TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED ) ) ) )
            {
                throw new FailureTypeObservableException( 
                        GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                        "Tape export by operator required to continue." );
            }
            throw new IllegalStateException( 
                    "No slots of type " + slotType + " available in partition." );
        }
        
        return retval;
    }
    
    
    private Set< Integer > getAllSlots( final UUID partitionId, final ElementAddressType slotType )
    {
        Validations.verifyNotNull( "Slot type", slotType );
        
        switch ( slotType )
        {
            case STORAGE:
                return m_storageSlots.get( partitionId );
            case IMPORT_EXPORT:
                return m_importExportSlots.get( partitionId );
            default:
                throw new UnsupportedOperationException( "No code to support " + slotType + "." );
        }
    }
    
    
    private void verifyTapeAndDriveInSamePartition( final TapeDrive drive, final UUID tapeId )
    {
        final UUID partitionId = drive.getPartitionId();
        if ( !m_partitionDrives.get( partitionId ).contains( drive.getId() ) )
        {
            throw new IllegalStateException( 
                    "Tape drive " + drive + " is not a member of partition." );
        }
        if ( !m_partitionTapes.get( partitionId ).contains( tapeId ) )
        {
            throw new IllegalStateException( 
                    "Tape " + tapeId + " is not a member of partition." );
        }
    }
    
    
    @Override
    synchronized public Set< UUID > getTapesInPartition( final UUID partitionId )
    {
        if ( m_partitionTapes.containsKey( partitionId ) )
        {
            return new HashSet<>( m_partitionTapes.get( partitionId ) );
        }
        return new HashSet<>();
    }
    
    
    @Override
    synchronized public Set< UUID > getTapesNotInPartition( final UUID partitionId )
    {
        final Set< UUID > retval = new HashSet<>();
        for ( final Map.Entry< UUID, Set< UUID > > e : m_partitionTapes.entrySet() )
        {
            if ( e.getKey().equals( partitionId ) )
            {
                continue;
            }
            retval.addAll( e.getValue() );
        }
        return retval;
    }
    
    
    @Override
    synchronized public Set< UUID > getDrivesRequiringCleaning()
    {
        return m_drivesRequiringCleaning;
    }
    
    private volatile Set< UUID > m_transactionDrivesRequiringCleaning;
    private volatile Map< UUID, Set< Integer > > m_transactionImportExitSlots;
    private volatile Map< UUID, Set< Integer > > m_transactionStorageSlots;
    private volatile Map< UUID, Integer > m_transactionTapeElementAddresses;
    private volatile Set< UUID > m_transactionTapesInOfflineDrives;
    private volatile Map< UUID, Integer > m_transactionDriveElementAddresses;
    private volatile Map< UUID, Set< UUID > > m_transactionPartitionDrives;
    private volatile Map< UUID, Set< UUID > > m_transactionPartitionTapes;
    //NOTE: there is no permanent member variable corresponding to m_transactionQuiescedPartitions because we only use it during reconcile.
    private volatile Set< UUID > m_transactionQuiescedPartitions;

    private final Set< UUID > m_drivesRequiringCleaning = new HashSet<>();
    private final Map< UUID, Set< Integer > > m_importExportSlots = new HashMap<>();
    private final Map< UUID, Set< Integer > > m_storageSlots = new HashMap<>();
    private final Map< UUID, Integer > m_tapeElementAddresses = new HashMap<>();
    private final Set< UUID > m_tapesInOfflineDrives = new HashSet<>();
    private final Map< UUID, Integer > m_driveElementAddresses = new HashMap<>();
    private final Map< UUID, Set< UUID > > m_partitionDrives = new HashMap<>();
    private final Map< UUID, Set< UUID > > m_partitionTapes = new HashMap<>();
    
    private volatile BeansServiceManager m_transaction;
    private final BeansServiceManager m_serviceManager;
    private final int m_minDriveCountPerPartition;

    private final static Logger LOG = Logger.getLogger( TapeEnvironmentManager.class );
}
