package com.spectralogic.s3.target.frmwrk;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A wrapper for an InputStream that calculates an MD5 checksum on the fly
 * as the data is being read.
 */
public class ChecksummedInputStream extends InputStream {
    private final DigestInputStream digestStream;

    public ChecksummedInputStream(final InputStream inputStream) throws NoSuchAlgorithmException {
        // We only need to wrap the stream once with the DigestInputStream.
        // The read methods below will delegate to this stream, which in turn
        // reads from the wrapped stream while updating the message digest.
        this.digestStream = new DigestInputStream(inputStream, MessageDigest.getInstance("MD5"));
    }

    /**
     * Reads the next byte of data from the input stream.
     * @return the next byte of data, or -1 if the end of the stream is reached.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int read() throws IOException {
        return this.digestStream.read();
    }

    /**
     * Reads some number of bytes from the input stream and stores them into the buffer array b.
     * @param b the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int read(final byte[] b) throws IOException {
        return this.digestStream.read(b);
    }

    /**
     * Reads up to len bytes of data from the input stream into an array of bytes.
     * @param b the buffer into which the data is read.
     * @param off the start offset in the destination array b.
     * @param len the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        return this.digestStream.read(b, off, len);
    }

    /**
     * Returns the number of bytes that can be read from this input stream without blocking.
     * @return the number of bytes that can be read from this input stream without blocking.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int available() throws IOException {
        return this.digestStream.available();
    }

    /**
     * Closes this input stream and releases any system resources associated with the stream.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        this.digestStream.close();
    }

    /**
     * Skips over and discards n bytes of data from this input stream.
     * @param n the number of bytes to be skipped.
     * @return the actual number of bytes skipped.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public long skip(final long n) throws IOException {
        return this.digestStream.skip(n);
    }

    /**
     * Returns the calculated MD5 checksum for all data read from the stream.
     * @return a byte array containing the MD5 checksum digest.
     */
    public byte[] getCalculatedChecksum() {
        return this.digestStream.getMessageDigest().digest();
    }
}
