package com.spectralogic.s3.target;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import com.spectralogic.s3.target.frmwrk.RangedInputStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangedInputStream_Test
{
    @Test
    public void testReadFunctionsReturnCorrectBytes() throws IOException
    {
        final String testString = "0123456789ABC";

        InputStream innerStream = new ByteArrayInputStream( testString.getBytes( StandardCharsets.UTF_8 ) );
        try (final InputStream rangedStream = new RangedInputStream( innerStream, 0, 5) )
        {
            for ( int i = 0; i < 5; i++ )
            {
                assertEquals( ( i + "").charAt(0) , (char)( rangedStream.read() ) );
            }
            assertEquals(-1, rangedStream.read(), "Should have returned a -1 to signify end of stream");
        }

        innerStream = new ByteArrayInputStream( testString.getBytes( StandardCharsets.UTF_8 ) );
        try (final InputStream rangedStream = new RangedInputStream( innerStream, 4, 6) )
        {
            final byte[] readBuffer = new byte[2];
            for ( int i = 4; i < 10; i+= 2 )
            {
                final int readBytes = rangedStream.read( readBuffer );
                assertEquals( ( i + "").charAt(0) , (char)( readBuffer[ 0 ] ) );
                assertEquals( ( ( i + 1 ) + "").charAt(0) , (char)( readBuffer[ 1 ] ) );
                assertEquals( readBytes, 2 );
            }
            assertEquals(-1,  rangedStream.read(readBuffer), "Should have returned a -1 to signify end of stream");
        }

        innerStream = new ByteArrayInputStream( testString.getBytes( StandardCharsets.UTF_8 ) );
        try (final InputStream rangedStream = new RangedInputStream( innerStream, 4, 6) )
        {
            final byte[] readBuffer = new byte[4];
            for ( int i = 4; i < 10; i+= 2 )
            {
                final int readBytes = rangedStream.read( readBuffer, 1, 2 );
                assertEquals( 0 , (char)( readBuffer[ 0 ] ) );
                assertEquals( ( i + "").charAt(0) , (char)( readBuffer[ 1 ] ) );
                assertEquals( ( ( i + 1 ) + "").charAt(0) , (char)( readBuffer[ 2 ] ) );
                assertEquals( 0 , (char)( readBuffer[ 3 ] ) );
                assertEquals( readBytes, 2 );
            }
            assertEquals(-1, rangedStream.read(readBuffer, 1, 2), "Should have returned a -1 to signify end of stream");
        }
    }

    @Test
    public void testSkip() throws IOException
    {
        final String testString = "0123456789ABC";
        final int skipVal = 3;

        InputStream innerStream = new ByteArrayInputStream( testString.getBytes( StandardCharsets.UTF_8 ) );
        try (final InputStream rangedStream = new RangedInputStream( innerStream, 0, 5 + skipVal) )
        {
            rangedStream.skip( skipVal );
            for ( int i = 0; i < 5; i++ )
            {
                assertEquals( ( ( i + skipVal )+ "").charAt(0) , (char)( rangedStream.read() ) );
            }
            assertEquals(-1, rangedStream.read(), "Should have returned a -1 to signify end of stream");
        }
    }

    @Test
    public void testAvailable() throws IOException
    {
        final String testString = "0123456789ABC";

        InputStream innerStream = new ByteArrayInputStream( testString.getBytes( StandardCharsets.UTF_8 ) );
        try (final InputStream rangedStream = new RangedInputStream( innerStream, 0, 5) )
        {
            rangedStream.skip( 1 );
            rangedStream.read();
            assertEquals(rangedStream.available(), 3);
            rangedStream.read( new byte[2]);
            assertEquals(rangedStream.available(), 1);
            rangedStream.skip( 1 );
            assertEquals(rangedStream.available(), 0);
        }
    }

    @Test
    public void testMarkAndResetSuccess() throws IOException {
        byte[] data = "0123456789ABC".getBytes();
        ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
        RangedInputStream rangedStream = new RangedInputStream(byteStream, 7, 10);

        assertTrue(rangedStream.markSupported());
        rangedStream.mark(10);
        byte[] buffer = new byte[5];
        rangedStream.read(buffer);

        assertEquals("789A", new String(buffer, 0, 4)); // First 4 bytes read
        rangedStream.reset(); // Should rewind
        int nextByte = rangedStream.read();
        assertEquals('7', nextByte);
    }

    public void testCompatibleWithMd5ComputingFileInputStream() throws IOException {
        final String targetContent = "hello world";
        final String prefix = "asdf";
        final String suffix = "asdf";
        final String fullContent = prefix + targetContent + suffix;
        final int rangeStart = prefix.length();
        final int rangeLength = targetContent.length();

        final File directFile = Files.createTempFile("direct", ".txt").toFile();
        final File rangedFile = Files.createTempFile("ranged", ".txt").toFile();

        try {
            Files.write(directFile.toPath(), targetContent.getBytes(StandardCharsets.UTF_8));
            Files.write(rangedFile.toPath(), fullContent.getBytes(StandardCharsets.UTF_8));

            byte[] md5FromDirect;
            byte[] md5FromRanged;

            try (final Md5ComputingFileInputStream md5Stream1 = new Md5ComputingFileInputStream(directFile);
                 final RangedInputStream rangedStream1 = new RangedInputStream(md5Stream1, 0, rangeLength)) {

                while (rangedStream1.read() != -1) {
                    // Consume all bytes to compute MD5
                }
                md5FromDirect = md5Stream1.getDigest().digestAndReset();
            }

            try (final Md5ComputingFileInputStream md5Stream2 = new Md5ComputingFileInputStream(rangedFile);
                 final RangedInputStream rangedStream2 = new RangedInputStream(md5Stream2, rangeStart, rangeLength)) {

                while (rangedStream2.read() != -1) {
                    // Consume all bytes to compute MD5
                }
                md5FromRanged = md5Stream2.getDigest().digestAndReset();
            }

            assertEquals(md5FromDirect, md5FromRanged, "MD5 checksums should be identical");

        } finally {
            directFile.delete();
            rangedFile.delete();
        }
    }
}
