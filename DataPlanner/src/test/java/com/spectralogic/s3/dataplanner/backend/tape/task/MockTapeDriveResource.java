/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.tape.DriveTestResult;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.FormattedTapeInformation;
import com.spectralogic.s3.common.rpc.tape.domain.LoadedTapeInformation;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsToVerify;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class MockTapeDriveResource extends BaseRpcResource implements TapeDriveResource
{
    @Override
    public boolean isServiceable()
    {
        return true;
    }

    public RpcFuture< LoadedTapeInformation > getLoadedTapeInformation()
    {
        if ( 0 == --m_failSubsequentGetLoadedTapeInformation )
        {
            m_failGetLoadedTapeInformation = true;
        }
        if ( m_failGetLoadedTapeInformation )
        {
            m_failGetLoadedTapeInformation = false;
            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
        }
        return new RpcResponse<>( getLoadedTapeInformationInternal() );
    }   
    
    
    public RpcFuture< String > getLoadedTapeSerialNumber()
    {
        return new RpcResponse<>( getLoadedTapeInformationInternal().getSerialNumber() );
    }
    
    
    private LoadedTapeInformation getLoadedTapeInformationInternal()
    {
        final LoadedTapeInformation retval = BeanFactory.newBean( LoadedTapeInformation.class );
        retval.setType( this.getTapeType() );
        retval.setTapeId( m_tapeId );
        retval.setReadOnly( m_loadedTapeReadOnly );
        retval.setSerialNumber( ( null == m_tapeSerialNumber ) ? 
                "tsn" + UUID.randomUUID().toString()
                : m_tapeSerialNumber );
        return retval;
    }

    
    public RpcFuture< FormattedTapeInformation > getFormattedTapeInformation()
    {
        if ( 0 == --m_failSubsequentGetFormattedTapeInformation )
        {
            m_failGetFormattedTapeInformation = true;
        }
        if ( m_failGetFormattedTapeInformation )
        {
            m_failGetFormattedTapeInformation = false;
            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
        }
        
        final FormattedTapeInformation retval = BeanFactory.newBean( FormattedTapeInformation.class );
        retval.setType( this.getTapeType() );
        retval.setTapeId( m_tapeId );
        retval.setReadOnly( m_loadedTapeReadOnly );
        retval.setSerialNumber( ( null == m_tapeSerialNumber ) ? 
                "tsn" + UUID.randomUUID().toString()
                : m_tapeSerialNumber );
        retval.setAvailableRawCapacity( m_availableRawCapacity );
        retval.setTotalRawCapacity( 1000 );
        return new RpcResponse<>( retval );
    }
    
    
    public void setGetFormattedTapeInformationAvailableRawCapacity( final long value )
    {
        m_availableRawCapacity = value;
    }
    
    
    public void setTapeSerialNumber( final String value )
    {
        m_tapeSerialNumber = value;
    }
    
    
    public void setTapeId( final UUID tapeId )
    {
        m_tapeId = tapeId;
    }
    
    
    public void setWriteDataResult( final BlobIoFailures value )
    {
        m_writeDataResult = value;
    }

    
    public RpcFuture< BlobIoFailures > writeData( 
            final LtfsFileNamingMode ltfsFileNamingMode,
            final S3ObjectsIoRequest objectsToWriteToTape )
    {
        final RpcFuture< BlobIoFailures > retval =
                m_invocationListener.writeData( ltfsFileNamingMode, objectsToWriteToTape );
        if ( null != retval )
        {
            return retval;
        }
        
        return new RpcResponse<>( m_writeDataResult );
    }
    
    
    public void setReadDataResult( final BlobIoFailures value )
    {
        m_readDataResult = value;
    }
    
    
    public RpcFuture< LtfsFileNamingMode > getLtfsFileNamingMode()
    {
        if ( null == m_ltfsFileNamingMode )
        {
            throw new RpcProxyException( "Oops.", BeanFactory.newBean( Failure.class ) );
        }
        return new RpcResponse<>( m_ltfsFileNamingMode );
    }
    
    
    public void setLtfsFileNamingMode( final LtfsFileNamingMode value )
    {
        m_ltfsFileNamingMode = value;
    }

    
    public RpcFuture< BlobIoFailures > readData( final S3ObjectsIoRequest objectsToReadIntoCache )
    {
        final RpcFuture< BlobIoFailures > retval = m_invocationListener.readData( objectsToReadIntoCache );
        if ( null != retval )
        {
            return retval;
        }
        
        return new RpcResponse<>( m_readDataResult );
    }
    
    
    public void setVerifyDataResult( final BlobIoFailures value )
    {
        m_verifyDataResult = value;
    }


    public void setVerifyDataException( final RuntimeException e )
    {
        m_verifyDataException = e;
    }
    
    
    public RpcFuture< BlobIoFailures > verifyData( final S3ObjectsToVerify objectsToVerify )
    {
        final RpcFuture< BlobIoFailures > retval = m_invocationListener.verifyData( objectsToVerify );
        if ( null != retval )
        {
            return retval;
        }
        if ( m_verifyDataException != null )
        {
            throw m_verifyDataException;
        }
        
        return new RpcResponse<>( m_verifyDataResult );
    }
    
    
    public RpcFuture< String > quiesce()
    {
        return new RpcResponse<>( UUID.randomUUID().toString() );
    }   
    
    
    public RpcFuture< String > verifyQuiescedToCheckpoint( final String checkpointIdentifier, final boolean allowRollback )
    {
        return verifyConsistent();
    }
    
    
    public RpcFuture< String > verifyConsistent()
    {
        if ( null != m_verifyQuiescedToCheckpointException )
        {
            throw m_verifyQuiescedToCheckpointException;
        }
        return new RpcResponse<>( m_verifyQuiescedToCheckpointResponse );
    }
    
    
    public void setVerifyQuiescedToCheckpointResponse( final String response )
    {
        m_verifyQuiescedToCheckpointResponse = response;
    }
    
    
    public void setVerifyQuiescedToCheckpointException( final RuntimeException ex )
    {
        m_verifyQuiescedToCheckpointException = ex;
    }
    
    
    public RpcFuture< Boolean > hasChangedSinceCheckpoint( final String checkpointIdentifier )
    {
        if ( null != m_hasChangedSinceCheckpointException )
        {
            throw m_hasChangedSinceCheckpointException;
        }
        return new RpcResponse<>( Boolean.valueOf( m_hasChangedSinceCheckpointResponse ) );
    }
    
    
    public void setHasChangedSinceCheckpointResponse( final boolean response )
    {
        m_hasChangedSinceCheckpointResponse = response;
    }
    
    
    public void setHasChangedSinceCheckpointException( final RuntimeException ex )
    {
        m_hasChangedSinceCheckpointException = ex;
    }

    
    public RpcFuture< ? > prepareForRemoval()
    {
        return null;
    }

    
    public RpcFuture< ? > format( final boolean characterize, final TapeDriveType tapeDriveTargetedForCompatibility )
    {
        m_formatTapeCalls.add( tapeDriveTargetedForCompatibility );
        
        if ( m_failFormat )
        {
            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
        }
        
        return new RpcResponse<>();
    }

    
    public RpcFuture< String > inspect()
    {
        if ( m_failInspect )
        {
            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
        }
        if (m_failPartition)
        {
            throw new RpcProxyException( "Medium contains only one partition", BeanFactory.newBean( Failure.class ) );
        }
        if ( null != m_returnNullOnInspect )
        {
            if ( m_returnNullOnInspect.booleanValue() )
            {
                return new RpcResponse<>( null );
            }
            return new RpcResponse<>( "nonnull" );
        }
        
        return new RpcResponse<>( ( null == m_tapeId ) ? null : m_tapeId.toString() );
    }
    
    
    public void setTakeOwnershipException( final RuntimeException ex )
    {
        m_takeOwnershipException = ex;
    }

    
    public RpcFuture< String > takeOwnershipOfTape( final UUID tapeId )
    {
        if ( null != m_takeOwnershipException )
        {
            throw m_takeOwnershipException;
        }
        
        m_tapeId = tapeId;
        return new RpcResponse<>( "blank" );
    }  
    
    
    public void setWaitForDriveCleaningToCompleteException( final RuntimeException ex )
    {
        m_waitForDriveCleaningToCompleteException = ex;
    }
    
    
    public RpcFuture< ? > waitForDriveCleaningToComplete()
    {
        if ( null != m_waitForDriveCleaningToCompleteException )
        {
            throw m_waitForDriveCleaningToCompleteException;
        }
        return new RpcResponse<>( null );
    }


    public RpcFuture<DriveTestResult> driveTestPostB()
    {
        if (m_failTest) {
            return new RpcResponse<>( DriveTestResult.FAILED );
        } else {
            return new RpcResponse<>( DriveTestResult.SUCCESS );
        }
    }

    @Override
    public RpcFuture<?> driveDump() {
        return null;
    }


    public void setDs3Contents( final String handle, final List< S3ObjectsOnMedia > responses )
    {
        m_applicationContentsHandle = handle;
        m_readApplicationContentsResponses = new ArrayList<>( responses );
    }


    public RpcFuture< String > openDs3Contents( 
            final boolean includeObjectMetadata, 
            final boolean recursive )
    {
        if ( !includeObjectMetadata || !recursive )
        {
            throw new RuntimeException( "Calls should always include metadata and require recursion." );
        }
        if ( null == m_applicationContentsHandle )
        {
            throw new FailureTypeObservableException( GenericFailure.CONFLICT, "Tape is not BP-formatted." );
        }
        return new RpcResponse<>( m_applicationContentsHandle );
    }


    public RpcFuture< String > openForeignContents( 
            final String bucketName,
            final String blobCountMetadataKey,
            final String creationDateMetadataKey,
            final long maxBlobSize,
            final long maxLtfsExtendedAttributeValueLengthInBytesToIncludeInObjectMetadata )
    {
        return openDs3Contents( true, true );
    }
    
    
    public RpcFuture< S3ObjectsOnMedia > readContents( 
            final String handle, 
            final int maxResults,
            final long maxLength )
    {
        if ( !handle.equals( m_applicationContentsHandle ) )
        {
            throw new FailureTypeObservableException( GenericFailure.NOT_FOUND, "Handle is unknown." );
        }
        if ( m_readApplicationContentsResponses.isEmpty() )
        {
            return new RpcResponse<>( null );
        }
        return new RpcResponse<>( m_readApplicationContentsResponses.remove( 0 ) );
    }
    

    public RpcFuture< ? > closeContents( final String handle )
    {
        if ( !handle.equals( m_applicationContentsHandle ) )
        {
            throw new FailureTypeObservableException( GenericFailure.NOT_FOUND, "Handle is unknown." );
        }
        m_applicationContentsHandle = null;
        return new RpcResponse<>( null );
    }
    
    
    public void setInvocationListener( final TapeDriveResource listener )
    {
        m_invocationListener = listener;
    }
    
    
    public boolean isFormatInvoked()
    {
        return !m_formatTapeCalls.isEmpty();
    }
    
    
    public List< TapeDriveType > getFormatCalls()
    {
        return new ArrayList<>( m_formatTapeCalls );
    }
    
    
    public void setFailFormat( final boolean value )
    {
        m_failFormat = value;
    }

    public void setFailTest( final boolean value )
    {
        m_failTest = value;
    }
    
    
    public void setFailInspect( final boolean value )
    {
        m_failInspect = value;
    }

    public void setFailPartition(final boolean value)
    {
        m_failPartition = value;
    }
    
    public void setFailGetLoadedTapeInformation( final boolean value )
    {
        m_failGetLoadedTapeInformation = value;
    }
    
    
    public void setFailSubsequentGetLoadedTapeInformation( final int callNumber )
    {
        m_failSubsequentGetLoadedTapeInformation = callNumber;
    }
    
    
    public void setFailGetFormattedTapeInformation( final boolean value )
    {
        m_failGetFormattedTapeInformation = value;
    }
    
    
    public void setFailSubsequentGetFormattedTapeInformation( final int callNumber )
    {
        m_failSubsequentGetFormattedTapeInformation = callNumber;
    }
    
    
    public void setReturnNullOnInspect( final Boolean value )
    {
        m_returnNullOnInspect = value;
    }
    
    
    public void setLoadedTapeReadOnly( final boolean value )
    {
        m_loadedTapeReadOnly = value;
    }

    public void setTapeType( final TapeType tapeType )
    {
        m_tapeType = tapeType;
    }

    private TapeType getTapeType()
    {
        if (m_tapeType == null)
        {
            return TapeType.LTO5;
        }
        return m_tapeType;
    }
    
    
    private volatile long m_availableRawCapacity = 1000;
    private volatile LtfsFileNamingMode m_ltfsFileNamingMode = LtfsFileNamingMode.OBJECT_ID;
    private volatile String m_applicationContentsHandle;
    private volatile List< S3ObjectsOnMedia > m_readApplicationContentsResponses = new ArrayList<>();
    private volatile String m_tapeSerialNumber = "tsn" + UUID.randomUUID();
    private volatile boolean m_loadedTapeReadOnly;
    private volatile Boolean m_returnNullOnInspect;
    private volatile boolean m_failInspect;
    private volatile boolean m_failPartition;
    private volatile boolean m_failFormat;
    private volatile boolean m_failTest;
    private volatile boolean m_failGetLoadedTapeInformation;
    private volatile int m_failSubsequentGetLoadedTapeInformation = -1;
    private volatile boolean m_failGetFormattedTapeInformation;
    private volatile int m_failSubsequentGetFormattedTapeInformation = -1;
    private volatile String m_verifyQuiescedToCheckpointResponse = null;
    private volatile RuntimeException m_verifyQuiescedToCheckpointException;
    private volatile boolean m_hasChangedSinceCheckpointResponse = true;
    private volatile RuntimeException m_hasChangedSinceCheckpointException;
    private volatile RuntimeException m_takeOwnershipException;
    private volatile RuntimeException m_waitForDriveCleaningToCompleteException;
    private volatile RuntimeException m_verifyDataException;
    private volatile UUID m_tapeId;
    private volatile TapeType m_tapeType;
    private volatile TapeDriveResource m_invocationListener = 
            InterfaceProxyFactory.getProxy( TapeDriveResource.class, null );
    private volatile BlobIoFailures m_readDataResult = BeanFactory.newBean( BlobIoFailures.class )
            .setFailures( (BlobIoFailure[])Array.newInstance( BlobIoFailure.class, 0 ) );
    private volatile BlobIoFailures m_verifyDataResult = BeanFactory.newBean( BlobIoFailures.class )
            .setFailures( (BlobIoFailure[])Array.newInstance( BlobIoFailure.class, 0 ) );
    private volatile BlobIoFailures m_writeDataResult = BeanFactory.newBean( BlobIoFailures.class )
            .setFailures( (BlobIoFailure[])Array.newInstance( BlobIoFailure.class, 0 ) );
    private final List< TapeDriveType > m_formatTapeCalls = new CopyOnWriteArrayList<>();
}
