package com.spectralogic.s3.target;

import com.spectralogic.util.security.FastMD5;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Md5ComputingFileInputStream extends FileInputStream {
    public Md5ComputingFileInputStream(final File file) throws FileNotFoundException {
        super(file);
    }


    @Override
    public int read() throws IOException {
        final int retval = super.read();
        if (-1 != retval) {
            m_digest.update(new byte[]{(byte) retval}, 0, 1);
        }
        return retval;
    }


    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        final int retval = super.read(b, off, len);
        if (0 < retval) {
            m_digest.update(b, off, retval);
        }
        return retval;
    }


    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }


    public FastMD5 getDigest() {
        return m_digest;
    }


    private final FastMD5 m_digest = new FastMD5();
} // end inner class def
