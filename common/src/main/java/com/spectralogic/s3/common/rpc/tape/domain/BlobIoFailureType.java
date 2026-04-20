/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

public enum BlobIoFailureType
{
    /**
     * Only applies to write I/O requests.  <br><br>
     * 
     * There was insufficient space to write the blob.
     */
    OUT_OF_SPACE,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * The blob to read/verify looked like the right blob from the identifiers, but either the checksum
     * algorithm or value was missing.
     */
    CHECKSUM_MISSING,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * The blob to read/verify looked like the right blob from the identifiers, but when the checksum 
     * algorithm for that blob was looked up on tape, it was not an algorithm supported.
     */
    CHECKSUM_ALGORITHM_UNKNOWN,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * The blob to read/verify looked like the right blob from the identifiers, but when the checksum 
     * algorithm for that blob was looked up on tape, it did not match the checksum algorithm that was 
     * expected in the read or verify I/O request sent down.
     */
    CHECKSUM_ALGORITHM_MISMATCH,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * The blob to read/verify looked like the right blob from the identifiers, and the checksum algorithm on
     * tape matched what was expected in the read or verify I/O request sent down, but when the checksum value
     * for that blob was looked up on tape, it did not match the checksum that was expected in the read or 
     * verify I/O request sent down.
     */
    CHECKSUM_VALUE_MISMATCH,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * The blob to read/verify looked like the right blob from the identifiers, and the checksum for that blob
     * matched what was expected in the read or verify I/O request sent down, but when read, did not match the
     * expected checksum.
     */
    CHECKSUM_MISMATCH_DUE_TO_CORRUPTION,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * A blob to read/verify was found that appeared to be correct, but was not, based on the identifiers 
     * provided in the read or verify I/O request sent down.
     */
    BLOB_ID_MISMATCH,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * An object to read/verify was found that appeared to be correct, but was not, based on the identifiers 
     * provided in the read or verify I/O request sent down.  Every blob of the validation-failed object will
     * have this error.
     */
    OBJECT_ID_MISMATCH,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * An object to read/verify was found that appeared to be correct, but was not, based on the metadata 
     * provided in the read or verify I/O request sent down.  Every blob of the validation-failed object will 
     * have this error.
     */
    OBJECT_METADATA_MISMATCH,
    
    
    /**
     * Only applies to read and verify I/O requests.  <br><br>
     * 
     * The blob to read/verify could not be found, or the object to read could not be found, in which case 
     * every blob of the validation-failed object will have this error.
     */
    DOES_NOT_EXIST,
    
    
    /**
     * Only applies to write I/O requests in LTFS name mode.  <br><br>
     * 
     * There was a path conflict between an objects and a directory (eg. "foo/bar" and "foo/bar/" or
     * "foo/bar/baz").These kinds of overlaps are legal in an object store since there are no real
     * "directories" but will fail when translated into a filesystem.
     */
    PATH_CONFLICT,
    
    
    /**
     * This failure code should not be used unless absolutely necessary, since it provides no detail as to
     * what the problem is.
     */
    UNKNOWN
}
