/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.tape.DriveTestResult;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.FormattedTapeInformation;
import com.spectralogic.s3.common.rpc.tape.domain.LoadedTapeInformation;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsToVerify;
import com.spectralogic.s3.common.rpc.tape.domain.TapeDriveInformation;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;
import com.spectralogic.util.net.rpc.server.RpcResponse;

/**
 * The instance name of a tape drive resource will be the manufacturer-supplied serial number of the tape
 * drive that the resource is for. <br><br>
 * 
 * The RPC resource instance is responsible for the lower-level SCSI commands issued.  There will be numerous 
 * cases where a single incoming RPC request will result in many lower-level SCSI requests.  This mapping is 
 * irrelevant to the RPC client. <br><br>
 * 
 * Warning: Communication to a tape drive must be serialized, or in other words, clients may not request that
 * a tape drive RPC resource instance execute multiple RPC requests concurrently.
 */
@RpcResourceName( "TapeDrive" )
public interface TapeDriveResource extends RpcResource
{
    /**
     * If the only thing needed is the loaded tape serial number, use {@link #getLoadedTapeSerialNumber}
     * instead since this method has additional dependencies to succeed, and thus, it is possible that 
     * retrieving the tape serial number via this method could fail where calling
     * {@link #getLoadedTapeSerialNumber} would succeed.
     * 
     * @return information about the tape loaded in this tape drive that can be looked up quickly, with no
     * dependency on the tape being formatted or not
     */
    @RpcMethodReturnType( LoadedTapeInformation.class )
    RpcFuture< LoadedTapeInformation > getLoadedTapeInformation();
    
    
    /**
     * @return the serial number of the tape loaded in this tape drive (note that the time required to service
     * this request is equal to or less than that of {@link #getLoadedTapeInformation} and that this method
     * returns the same serial number that {@link #getLoadedTapeInformation} would return in its payload
     */
    @RpcMethodReturnType( String.class )
    RpcFuture< String > getLoadedTapeSerialNumber();
    
    
    /**
     * @return information about the formatted tape loaded in this tape drive, which may take considerable
     * time to determine
     * 
     * @throws RpcProxyException if the tape is not formatted, if the tape cannot be LTFS-mounted for any
     * reason, or if any of the information returned in {@link FormattedTapeInformation} cannot be determined
     * for any reason (clients invoking this method must be designed to handle this method failing at any
     * time in any scenario or workflow and cannot rely on the successful completion of this method for any
     * scenario or workflow, except the formatting of tapes)
     */
    @RpcMethodReturnType( FormattedTapeInformation.class )
    RpcFuture< FormattedTapeInformation > getFormattedTapeInformation();
    
    
    /**<br><hr><b>Configures the tape for the specified LTFS file naming mode.</b><hr>
     * 
     * If the tape has already been configured for the specified LTFS file naming mode, this method shall
     * proceed normally.  <br><br>
     * 
     * If the tape has already been configured for an LTFS file naming mode other than the one specified,
     * this method shall throw a 409 failure, since the tape can only be configured for a specific LTFS
     * file naming mode once.  <br><br>
     * 
     * If the tape has not been configured for an LTFS file naming mode but has data on it, then the
     * current LTFS file naming mode is implied to be {@link LtfsFileNamingMode#OBJECT_NAME} (tapes written
     * to prior to LTFS file naming mode support will not have this flag set, but will be written in the
     * {@link LtfsFileNamingMode#OBJECT_NAME} format).  <br><br>
     * 
     * Note: Calling {@link #format} on a tape clears its LTFS file naming mode.<br>
     * 
     * <br><hr><b>Writes the specified objects to tape from cache.</b><hr>
     * 
     * Object metadata shall be included in the {@link S3ObjectsIoRequest} payload sent down via
     * {@link S3ObjectOnMedia#getMetadata} so that it may be persisted to tape.  A write failure of
     * the metadata shall equate to a failure to write the blobs for that object.  <br><br>
     * 
     * This request may fail in 2 ways:
     * <ol>
     * <li> Some of the objects may fail to write (e.g. ran out of space), in which case a 
     * {@link BlobIoFailure} shall be returned for every blob that could not be written.
     * <li> An unexpected exceptional condition occurred, in which case an {@link RpcException} shall be
     * thrown, indicating that no blobs could be written and that {@link TapeDriveResource#quiesce} does not
     * need to be called.
     * </ol>
     * 
     * <br><hr><b>This method does not quiesce any changes to the tape, and thus, does not create a new 
     * checkpoint. </b><hr>
     * 
     * Neither the data written nor the LTFS file naming mode flag set (if the flag needs to be set) shall
     * be quiesced to the tape via creating a new checkpoint as a result of this call.  In order to create a
     * checkpoint that contains the data written and LTFS file naming mode flag set, a subsequent call to
     * {@link #quiesce} is required. <br><br>
     */
    @RpcMethodReturnType( BlobIoFailures.class )
    RpcFuture< BlobIoFailures > writeData(
            final LtfsFileNamingMode ltfsFileNamingMode,
            final S3ObjectsIoRequest objectsToWriteToTape );
    
    
    /**
     * @return the LTFS file naming mode of the tape (Note: If the tape has been written to from legacy code
     * where the LTFS file naming mode was not recorded on the tape, then the LTFS file naming mode is implied
     * to be {@link LtfsFileNamingMode#OBJECT_NAME} and that value must be returned)
     * 
     * @throws {@link RpcProxyException} if the tape does not have an LTFS file naming mode (if it has not 
     * been formatted and written to yet)
     */
    @RpcMethodReturnType( LtfsFileNamingMode.class )
    RpcFuture< LtfsFileNamingMode > getLtfsFileNamingMode();
    
    
    /**
     * Reads the specified objects from tape to cache.  <br><br>
     * 
     * Object metadata need not be included in the {@link S3ObjectsIoRequest} payload sent down.  If it is 
     * included, the implementor of this method may ignore it or validate it, but if it validates it, it may
     * not assume that the metadata sent down is all of the metadata for the object i.e. it could be a subset
     * of the metadata.  <br><br>
     * 
     * This request may fail in 2 ways:
     * <ol>
     * <li> Some of the objects may fail to be read (e.g. not found or checksum mismatch), in which case a 
     * {@link BlobIoFailure} shall be returned for every blob that could not be read.  A 
     * {@link BlobIoFailureType} exists for every possible validation or read failure.  Implementors of this
     * method are expected to check for each of these {@link BlobIoFailureType} failure conditions.
     * <li> An unexpected exceptional condition occurred, in which case an {@link RpcException} shall be
     * thrown, indicating that no blobs could be read.
     * </ol>
     */
    @RpcMethodReturnType( BlobIoFailures.class )
    RpcFuture< BlobIoFailures > readData( final S3ObjectsIoRequest objectsToReadIntoCache );
    
    
    /**
     * Reads the specified objects from tape, but does not write them to cache.  <br><br>
     * 
     * This method should behave identically to {@link #readData} with exactly the same validation checks,
     * with the exception that data is never written to cache.  Amongst other things, this means that
     * this method will verify that the data read matches the checksums specified and that the
     * {@link S3Object} and {@link Blob} ids persisted on tape match those sent down in the objects to verify 
     * request.  <br><br>
     * 
     * In addition to performing all validations that {@link #readData} would perform, this method shall 
     * also verify that the S3 object metadata on tape matches the metadata sent down in the request.  A
     * metadata entry is said to be optional if it is included in 
     * {@link S3ObjectsToVerify#getOptionalS3ObjectMetadataKeys}.
     * Specifically, an S3 object metadata mismatch shall be reported if and only if:
     * <ol>
     * <li>A non-optional metadata key sent down does not exist on tape; OR
     * <li>A metdata key sent down exists on tape, but the value does not match between what was sent down 
     * and what resides on tape; OR
     * <li>A non-optional metadata key exists on tape but was not sent down
     * </ol>
     */
    @RpcMethodReturnType( BlobIoFailures.class )
    RpcFuture< BlobIoFailures > verifyData( final S3ObjectsToVerify objectsToVerify );
    
    
    /**
     * Opens a handle to find DS3 contents on a tape for the purpose of determining the contents of a foreign
     * Black Pearl (DS3) tape.  <br><br>
     * 
     * Handles must be explicitly closed via a call to {@link #closeContents} if this call succeeds
     * and returns a handle.  <br><br>
     * 
     * If the LTFS volume on the tape is inconsistent, this method invocation will result in rolling back
     * the tape to the most recent quiesced checkpoint on the tape, similar to calling 
     * {@link #verifyQuiescedToCheckpoint} with the most recent checkpoint returned by a {@link #quiesce},
     * except that this appliance does not know what the most recent checkpoint is since the tape is foreign
     * to it, which is why the implementor of this method is required to determine what it is and to roll
     * back to it.  Note: If the tape is write protected and inconsistent, the implementor of this method
     * shall mount the tape at the most recent checkpoint if possible.  <br><br>
     * 
     * If the <code>recursive</code> flag is specified, all the  buckets and S3 objects on tape will be 
     * discovered using the handle.  If the <code>recursive</code> flag is not specified, only the buckets on 
     * tape will be discovered using the handle (no S3 objects will be discovered).  <br><br>
     * 
     * Unless <code>includeObjectMetadata</code> is true, S3 object metadata will not be discovered or
     * included in response payloads for subsequent {@link #readContents} calls.  <br><br>
     * 
     * Note that zero-length objects and folders shall be included in the contents returned using the handle
     * returned by this method.  Zero-length objects and folders may be sent down on {@link #writeData} calls
     * and persisted to the tape, and thus, must be reported back up when reporting contents on the tape.
     * <br><br>
     * 
     * @return a String-encoded identifier for a newly-created handle to read tape contents, to be
     * used for subsequent calls to {@link #readContents} and {@link #closeContents}
     * 
     * @throws 307 temporarily unavailable exception if no additional handles may be created at this time
     * (note that implementors are not required to support more than one open handle per tape drive at a time)
     * 
     * @throws 409 if it is found that the contents on the tape are non-DS3 contents (in which case, it is
     * illegal to invoke this method and {@link #openForeignContents} should be invoked instead)
     */
    @RpcMethodReturnType( String.class )
    RpcFuture< String > openDs3Contents( 
            final boolean includeObjectMetadata, 
            final boolean recursive );
    
    
    /**
     * Opens a handle to find non-DS3 contents on a tape for the purpose of determining the contents of a 
     * non-DS3 (non-Black Pearl) tape to be imported read-only in-place.  <br><br>
     * 
     * Non-DS3 tapes shall not be modified at any time in any way, even if they are fully imported, unless
     * the tape is formatted and made available in the general pool of blank tapes.  <br><br>
     * 
     * Handles must be explicitly closed via a call to {@link #closeContents} if this call succeeds
     * and returns a handle.  <br><br>
     * 
     * If the LTFS volume on the tape is inconsistent, the implementor of this method shall mount the tape at 
     * the most recent index if possible. 
     * <br><br>
     * 
     * The <code>bucketName</code> flag refers to the bucket under which we shall import all the contents of 
     * this tape.  This must be specified since the bucket name will not be part of the LTFS file name for
     * non-DS3 content.  <br><br>
     * 
     * No checksums shall be computed for non-DS3 content at any time.  Specifically, the implementation of 
     * this method will need to return a "fake" checksum value since {@link BlobOnMedia} requires a checksum 
     * specification.  Implementations must check for this "fake" checksum value when {@link #readData} calls 
     * are made on the tape as an indication that no checksum value should be computed or verified against 
     * for the read operation.  By doing this, clients of this method can be made oblivious to the fact that
     * there is no checksum computed, provided that they never try to validate the data themselves with the
     * checksum.  Checksums are only computed against data being read by a client when either (i) reading the
     * data from tape, where the checksums are sent down in the {@link #readData} call, or (ii) by a client
     * performing end-to-end CRC.  (i) is not a problem since the implementation can ignore the "fake"
     * checksum as aforementioned.  (ii) means that clients cannot perform end-to-end CRC since the checksum 
     * value returned to clients will be the "fake" checksum, and furthermore, there is no way to provide any
     * meaningful end-to-end CRC since we never had an end-to-end CRC in the first place when we imported the
     * non-DS3 contents i.e. if we weren't sent the checksum by the client when the data was originally 
     * written, then any checksum we compute by definition cannot be an end-to-end CRC.  <br><br>
     * 
     * Implementations of this method are required to mock DS3 metadata for non-DS3 content.  Specifically,
     * every {@link S3ObjectOnMedia} must include the following metadata keys:
     * <ul>
     * <li><code>blobCountMetadataKey</code>: The number of blobs for the object
     * <li><code>creationDateMetadataKey</code>: The creation date of the object
     * </ul><br>
     * Note: The list above is the complete set of DS3 metadata that must be mocked by implementations of this
     * method.  While other metadata may need to be mocked for non-DS3 content, any such other metadata not
     * specified above shall be mocked by the invoker of this method and not by the implementation of this
     * method.  <br><br>
     * 
     * In addition to mocking DS3 metadata for non-DS3 content, implementations are required to report LTFS
     * extended attributes as S3 object metadata.  Specifically, for every LTFS extended attribute 
     * <b>K</b>=<b>V</b> where <b>K</b> is the key and <b>V</b> is the value, object metadata shall be 
     * reported as <code>x-spectra-ltfs-<b>K</b>=<b>V</b></code>.  Note that LTFS extended attributes apply
     * on a per-file basis and not on a per-extent basis, so we don't have to deal with the possibility of
     * multiple, conflicting LTFS extended attributes between different extents for the same file.  <br><br>
     * 
     * The algorithm the implementor uses to generate object and blob IDs must be deterministic relative to 
     * the bucket name, object name, and the blob offset, and must fully use the 128-bit-length of a UUID to 
     * minimize hash collision probability.  Specifically, the algorithm to compute object and blob IDs shall 
     * be as follows:
     * <ol>
     * <li>The ID of an object shall be the MD5 hash of {bucket name}$#{object name}$#0, where the 128 bits 
     * of the MD5 hash are encoded into a 128-bit UUID
     * <li>The ID of a blob shall be the MD5 hash of {bucket name}$#{object name}$#{blob byte offset}, where 
     * the 128 bits of the MD5 hash are encoded into a 128-bit UUID
     * </ol>
     * For example:
     * <ul>
     * <li>The ID of an object named 'foo' in bucket 'bar' shall be the MD5 hash of bar$#foo$#0, encoded into 
     * a 128-bit UUID
     * <li>The ID of a blob with byte offset 200 whose object is named 'foo' in bucket 'bar' shall be the MD5 
     * hash of bar$#foo$#200, encoded into a 128-bit UUID
     * <li>The ID of an object with name 'foo' in bucket 'bar' shall be reported to be the same value across
     * multiple invocations of this method, regardless as to whether or not the system has been restarted
     * <li>The ID of an object with name 'foo' in bucket 'bar' shall generally (barring hash collisions) be
     * reported to be different from the ID of an object with name 'foo' in bucket 'zoo'
     * <li>The ID of an object matches identically the ID of the same object's blob at byte offset 0
     * </ul>
     * 
     * Note that zero-length objects and folders shall be included in the contents returned using the handle
     * returned by this method.  Such folders and objects will be composed of a single blob with an offset 
     * and length of zero.  <br><br>
     * 
     * Note that the behavior for importing LTFS files that span across multiple tapes is indeterminate, not
     * a requirement, and not supported.  Specifically,
     * <ol>
     * <li>As a general rule, we cannot know when we're done importing an object if we don't know the blob
     * count, including blobs on different tapes
     * <li>Per the return payload contract for {@link #readData}, an object id must be specified for each
     * LTFS file (this must be mocked by the implementation of that method for foreign content without object
     * IDs), and implementations cannot maintain state between different tapes to ensure the same ID is used
     * <li>The blob count cannot be returned correctly if blobs may reside on other tapes
     * </ol><br>
     * 
     * @param bucketName - implementations must "fake" the bucket membership of non-DS3 content in the
     * response payload, pretending that all the contents belong to this bucket <br>
     * 
     * @param blobCountMetadataKey - implementations must report the number of blobs for each object as an S3
     * object metadata entry such that <b>K</b>=<b>V</b> where <b>K</b> is <code>blobCountMetadataKey</code>
     * and <b>V</b> is the number of blobs (extents) for the object (for example, if there are 2 LTFS extents
     * for an object where <code>maxBlobSize</code> is 1TB, one LTFS extent is 100GB, and the other is 1.1TB,
     * then the blob count reported shall be 3).
     * 
     * @param creationDateMetadataKey - implementations must report the creation date for each object as an
     * S3 object metadata entry such that <b>K</b>=<b>V</b> where <b>K</b> is
     * <code>creationDateMetadataKey</code> and <b>V</b> is the creation date of the LTFS file being imported
     * as an S3 object.
     *  
     * @param maxBlobSize - if an extent is encountered while reading non-DS3 content that exceeds this 
     * value, then said extent shall be reported as multiple blobs (or example, if the 
     * <code>maxBlobSize</code> is 1GB and a 1.1GB extent is encountered, 2 blobs shall be reported for the 
     * extent: the first being 1GB and the second being 0.1GB) <br>
     * 
     * @param maxLtfsExtendedAttributeValueLengthInBytesToIncludeInObjectMetadata - this is the maximum length
     * in bytes that an LTFS extended attribute value can be for it to be reported in the S3 object metadata
     * (note that if this value is negative, no LTFS extended attributes shall be reported in the S3 object
     * metadata) <br>
     * 
     * @return a String-encoded identifier for a newly-created handle to read tape contents, to be
     * used for subsequent calls to {@link #readContents} and {@link #closeContents}
     * 
     * @throws 307 temporarily unavailable exception if no additional handles may be created at this time
     * (note that implementors are not required to support more than one open handle per tape drive at a time)
     * 
     * @throws 409 if it is found that the contents on the tape are DS3 contents (in which case, it is
     * illegal to invoke this method and {@link #openDs3Contents} should be invoked instead)
     */
    @RpcMethodReturnType( String.class )
    RpcFuture< String > openForeignContents( 
            final String bucketName,
            final String blobCountMetadataKey,
            final String creationDateMetadataKey,
            final long maxBlobSize,
            final long maxLtfsExtendedAttributeValueLengthInBytesToIncludeInObjectMetadata );
    

    /**
     * Finds the next set of contents using the handle specified (created by calling 
     * {@link #openDs3Contents} or {@link #openForeignContents}).  Neither the number of buckets, nor the 
     * number of S3 objects, nor the number of blobs returned may exceed <code>maxResults</code>.  <br><br>
     * 
     * When we say "finds the next set of contents", next is in terms of physical placement of the
     * data on the tape.  Specifically, any blob included in the response of this method invocation must be
     * physically persisted after any blob included in any response of previous invocations of this method
     * for the same handle.  Furthermore, the order of the blobs in the response payload of this method 
     * invocation must match that of their physical persistence.  For example, if the physical layout of 
     * blobs on a tape is: <br>
     * <ol>
     * <li>Bucket Z, Object C, blob 1
     * <li>Bucket Z, Object B, blob 2
     * <li>Bucket Z, Object C, blob 2
     * <li>Bucket Z, Object A, blob 1
     * <li>Bucket Z, Object B, blob 1
     * <li>Bucket Z, Object D, blob 1
     * <li>Bucket Z, Object D, blob 2
     * </ol>
     * Then the blobs must be reported back in this same order.  One implication of this is that there cannot
     * be a single {@link S3ObjectOnMedia} instance in the response payload to contain both of object C's 
     * blobs, since object C is interleaved with object B.  Thus, there must be two instances of 
     * {@link S3ObjectOnMedia} in the response for this method invocation.  Note that the logical order of
     * the blobs for an object has no bearing over the its required ordering in the response for this method
     * invocation.  <br><br>
     * 
     * Note that, in the case of non-DS3 content, that the blobs would be extents.  For example, it is
     * possible to write a file to LTFS with multiple extents and to interleave other object extents in
     * between.  This plays into the example and requirements above seamlessly.  <br><br>
     * 
     * Note that an open handle can be invalidated at any time due to any of the following conditions:
     * <ol>
     * <li> The tape is modified (for example, via a call to {@link #writeData})
     * <li> The drive is having issues with the tape and has requested a forced tape removal
     * <li> The tape backend crashes and comes back up (handles are not persisted between running instances)
     * </ol>
     * 
     * @param preferredMaximumNumberOfResultsReturned - a preferred maximum number of results to return in
     * the response payload (implementations do not have to guarantee that the result count has a maximum
     * of precisely this number; however, they must respect this preference and not return a payload that
     * is significantly larger than the specified preference)
     * 
     * @param preferredMaximumTotalBlobLengthInBytesReturned - a preferred maximum of the sum of all 
     * {@link BlobOnMedia#LENGTH}s to return in the response payload (implementations do not have to 
     * guarantee that the result has a maximum of precisely this number; however, they must respect this 
     * preference and not return a payload that is significantly larger than the specified preference, unless 
     * a single blob is being returned that exceeds the maximum preference, since at least one blob must be
     * returned)
     * 
     * <br><br> Note: <code>preferredMaximumNumberOfResultsReturned</code> and 
     * <code>preferredMaximumTotalDataLengthInBytesReturned</code> shall both be honored in an "and"'d and
     * not an "or"'d manner.  For example, if the preferred maximums are 100 and 10GB and after 100 blobs,
     * the sum of the blob lengths is only 1MB, those 100 blobs shall be returned.  Similarly, if after
     * reading only 10 blobs, the sum of the blob lengths is 10GB, those 10 blobs shall be returned. <br><br>
     * 
     * @return null if there is no more content left to find for the handle specified
     * 
     * @throws 404 not found exception if the handle specified does not exist or has already been closed
     * 
     * @throws 410 gone exception if the handle has been invalidated
     * 
     * @throws 409 if it is found that the contents on the tape do not conform to the format expected (the 
     * handle has not been invalidated and a retry to invoke this method is allowed, which may or may not 
     * move past the 409 error)
     * 
     * @throws 400 bad request if the max results specified is too small or large for the implementation to
     * handle (the handle has not been invalidated and a retry to invoke this method is allowed)
     */
    @RpcMethodReturnType( S3ObjectsOnMedia.class )
    @NullAllowed RpcFuture< S3ObjectsOnMedia > readContents(
            final String handle,
            final int preferredMaximumNumberOfResultsReturned,
            final long preferredMaximumTotalBlobLengthInBytesReturned );
    

    /**
     * Closes the specified handle.  All handles must be explicitly closed after being opened - even if the
     * handle has been read until there were no remaining contents, or a call to 
     * {@link #readContents} resulted in an exception.  <br><br>
     * 
     * @throws 404 not found exception if the handle specified does not exist or has already been closed
     * 
     * @throws 410 gone exception if the handle has been invalidated
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > closeContents( final String handle );
    
    
    /**
     * If a tape is being written to, metadata is not necessarily updated along the way.  Thus, it is
     * possible that even after a write "completes" that it could fail.  Calling this method ensures that
     * whatever writes have been performed are committed and quiesced such that we are guaranteed that they
     * will not fail at some point in the future.  <br><br>
     * 
     * @return the identifier for the checkpoint made as a result of calling this method <br><br>
     * 
     * The checkpoint identifier is required to meet the following requirements:
     * 
     * <ol>
     * <li> The identifier must be represented as a {@link String}
     * <li> The identifier must be unique from one checkpoint to the next
     * <li> If the tape is formatted, checkpoint identifiers generated after the formatting must be unique
     *      when compared to the checkpoint identifiers generated prior to the formatting for that tape
     * </ol>
     */
    @RpcMethodReturnType( String.class )
    RpcFuture< String > quiesce();
    
    
    /**
     * This method must be supported by implementations for foreign contents as well as DS3 contents. <br><br>
     * 
     * Calling this method indicates that the tape is owned by this application.  This method is used to
     * ensure that the tape is in both (i) a consistent state and (ii) the state that the application expects 
     * it to be in.  <br><br>
     * 
     * The tape may not be in the state the application expects it to be in if the application state was lost 
     * and an older version of the application state restored.  The tape may not be in an LTFS-consistent 
     * state if the tape was removed from the tape drive without being properly quiesced first.  There are 
     * likely other scenarios where the tape may be left in an inconsistent state, and so we should never
     * assume that a tape loaded into a drive is consistent at the time it is loaded. <br><br>
     * 
     * Calling this method indicates that the tape should contain data on it such that the current checkpoint 
     * is the checkpoint specified.  If the newest checkpoint on the tape is the checkpoint specified, no tape
     * modification shall occur and the returned new checkpoint shall be null.  If the tape has newer 
     * checkpoints on it but contains the specified checkpoint, a rollback to the specified checkpoint shall 
     * occur and the new checkpoint returned (if we rollback to an earlier checkpoint, the new checkpoint does
     * not necessarily have to be the checkpoint we rolled back to).  If the tape does not contain the 
     * specified checkpoint, an {@link RpcProxyException} shall be thrown with code 
     * {@link TapeResourceFailureCode#CHECKPOINT_NOT_FOUND}. <br><br>
     * 
     * If this method is called for a tape that is write-protected and this method would have returned null
     * had the tape not been write-protected, this method must return null and not throw an error for said
     * write-protected tape.  In all other cases, an error shall be thrown.  For example, if this method 
     * would have returned non-null for a tape not write-protected, the same write-protected tape must throw 
     * an error since tape modification is necessary to return non-null, which is not possible for a 
     * write-protected tape. <br><br>
     * 
     * @return new checkpoint (if tape modification occurred), or null (if no tape modification occurred)
     * 
     * @throws RpcProxyException if the checkpoint identifier we send down is non-null and cannot be rolled
     * back to on the tape
     */
    @NullAllowed
    @RpcMethodReturnType( String.class )
    RpcFuture< String > verifyQuiescedToCheckpoint( final String checkpointIdentifier, final boolean allowRollback );
    
    
    /**
     * This method must be supported by implementations for foreign contents as well as DS3 contents. <br>
     * 
     * @return the current checkpoint, if the tape is consistent
     * 
     * @throws RpcProxyException if the tape isn't consistent or does not have a checkpoint yet, possibly due
     * to the tape not having been owned or formatted
     */
    @RpcMethodReturnType( String.class )
    RpcFuture< String > verifyConsistent();
    
    
    /**
     * @return FALSE if <code>checkpointIdentifier</code> exists and every new checkpoint between <code>
     * checkpointIdentifier</code> and the most recent checkpoint does not involve any changes to any data
     * on the tape (for example, TRUE would be returned if the more recent checkpoints on the tape were due
     * entirely to {@link #takeOwnershipOfTape} calls); OR  <br><br>
     * 
     * TRUE if any of the following applies:
     * <ol>
     * <li>The tape has not been LTFS-formatted or is in an unknown format
     * <li><code>checkpointIdentifier</code> does not exist on the tape
     * <li>Newer checkpoints since <code>checkpointIdentifier</code> that included data changes to the tape
     * </ol>  <br>
     * 
     * Calling this method on a tape that is LTFS-inconsistent is permitted.  In this case, the most recent,
     * consistent index should be used as the most recent checkpoint to consider.
     */
    @RpcMethodReturnType( Boolean.class )
    RpcFuture< Boolean > hasChangedSinceCheckpoint( final String checkpointIdentifier );
    

    /**
     * Clients should prepare for tape removal from the drive before issuing a move command in the 
     * {@link TapeEnvironmentResource} that will result in removing the tape from the drive.  If this call
     * fails, the move should not be attempted, unless the move is being performed since the 
     * {@link TapeDriveInformation#FORCE_TAPE_REMOVAL} flag is set on the tape drive the tape is being moved
     * out of.  <br><br>
     * 
     * Note: Preparing for tape removal from drive will automatically quiesce the tape.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > prepareForRemoval();
    

    /**
     * Formats the tape in this tape drive. <br><br>
     * 
     * After a tape is formatted, either (i) {@link #prepareForRemoval} will be called and the tape will
     * be removed from the tape drive, or (ii) other methods such as {@link #writeData} and 
     * {@link #getFormattedTapeInformation} may be called as if the tape was just inserted into the tape 
     * drive after a move.  <br><br>
     * 
     * After formatting a tape, {@link #takeOwnershipOfTape} must be called.  <br><br>
     * 
     * <b><font color = red>
     * Warning: Executing this command will permanently delete all data on the tape in the tape drive
     * </font></b>
     * 
     * 
     * @param density - If null, the media shall be formatted in the highest density possible;  otherwise, 
     * the media shall be formatted in the highest density possible that's still compatible with the tape 
     * drive specified.  <br><br>
     * 
     * Note that this param applies only to TS media, which can be formatted in different densities depending 
     * on the drive performing the format.  Sending down a non-null param for non-TS media is not allowed and
     * an error shall be thrown if this occurs.  <br><br>
     * 
     * If the compatibility level specified cannot be achieved since the drive performing the formatting isn't
     * capable of formatting the media at the specified level, an error shall be thrown.  If the compatibility
     * level specified is nonsensical or otherwise invalid, an error shall be thrown.  <br><br>
     * 
     * For example, if TS_JC media is loaded in a TS1140 drive:
     * <ul>
     * <li>And this method is invoked passing down null, the TS_JC shall be formatted in the TS1140 format.
     * <li>And this method is invoked passing down TS1140, the TS_JC shall be formatted in the TS1140 format.
     * <li>And this method is invoked passing down TS1150, an error shall be thrown since a TS1140 drive is 
     * not capable of formatting a TS_JC in the highest density possible that's still compatible with TS1150.
     * </ul><br>
     * 
     * For example, if TS_JC media is loaded in a TS1150 drive:
     * <ul>
     * <li>And this method is invoked passing down null, the TS_JC shall be formatted in the TS1150 format.
     * <li>And this method is invoked passing down TS1140, the TS_JC shall be formatted in the TS1140 format.
     * <li>And this method is invoked passing down TS1150, the TS_JC shall be formatted in the TS1150 format.
     * </ul>
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > format( final boolean characterize, @NullAllowed final TapeDriveType density );
    
    
    /**
     * Gets logical information about the tape currently in the tape drive.  Throws an exception if there is
     * no tape in the tape drive.  <br><br>
     * 
     * @return null if the tape is blank or empty, or whatever useful information can be provided if the tape 
     * is non-blank and non-empty to describe the tape's format, identifier, or any other attributes that 
     * would be helpful to a customer in identifying what the tape is about
     * 
     * @throws RpcProxyException if the tape appears to be formatted but its format is unknown
     */
    @NullAllowed
    @RpcMethodReturnType( String.class )
    RpcFuture< String > inspect();
    
    
    /**
     * Writes the specified tape id to the tape, thereby taking ownership of the tape.  See 
     * {@link FormattedTapeInformation#setTapeId} for more information about what the tape's tape id means.
     * 
     * @return the identifier for the checkpoint made as a result of calling this method <br><br>
     * 
     * The checkpoint identifier is required to meet the following requirements:
     * 
     * <ol>
     * <li> The identifier must be represented as a {@link String}
     * <li> The identifier must be unique from one checkpoint to the next
     * <li> If the tape is formatted, checkpoint identifiers generated after the formatting must be unique
     *      when compared to the checkpoint identifiers generated prior to the formatting for that tape
     * </ol>
     */
    @RpcMethodReturnType( String.class )
    RpcFuture< String > takeOwnershipOfTape( final UUID tapeId );
    
    
    /**
     * When a cleaning tape is loaded into a drive, the drive will be cleaned automatically.  No RPC requests
     * besides {@link #prepareForRemoval} and this method are applicable for a cleaning tape.  <br><br>
     * 
     * Note that this method may be called after the format has completed or erred.  If this occurs, the
     * returned response must be the same as if this method was called before the format was started or while
     * it was in progress.
     * 
     * @throws 409 conflict exception if the cleaning tape has been used too many times and cannot be used
     * to clean this drive
     * 
     * @throws 400 bad request exception if the tape in the drive is not a cleaning tape
     * 
     * @throws any other exception if the cleaning of the drive failed
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > waitForDriveCleaningToComplete();


    /**
     * Executes a test on the current tape, and blocks until it is complete.
     */
    @RpcMethodReturnType( DriveTestResult.class )
    RpcFuture< DriveTestResult > driveTestPostB();


    /**
     * Requests a dump from the drive.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > driveDump();
}
