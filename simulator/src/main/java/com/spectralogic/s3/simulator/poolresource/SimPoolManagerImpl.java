package com.spectralogic.s3.simulator.poolresource;

import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimPartition;
import com.spectralogic.s3.simulator.domain.SimPool;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.s3.simulator.lang.PoolConfig;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.s3.simulator.poolresource.api.SimPoolResource;
import com.spectralogic.s3.simulator.state.simresource.SimDevices;
import com.spectralogic.s3.simulator.state.simresource.SimStateManagerImpl;
import com.spectralogic.s3.simulator.taperesource.SimTapeEnvironmentResource;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;
import org.apache.log4j.Logger;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.math.BigInteger;
import static com.spectralogic.s3.simulator.Simulator.DEFAULT_PARTITION_SN;
import static com.spectralogic.s3.simulator.Simulator.DEFAULT_POOL_SN;
import static com.spectralogic.s3.simulator.state.simresource.SimStateManagerImpl.deleteNonUnderscoreFiles;

public final class SimPoolManagerImpl extends BaseRpcResource implements SimPoolStateManager {
    private static final String DEFAULT_SN = "1234";
    private boolean m_changed;
    private final PoolConfig m_config;
    private final SimDevices m_simDevices = new SimDevices();
    private final static Logger LOG = Logger.getLogger( SimPoolManagerImpl.class );

    public SimPoolManagerImpl(final RpcServer rpcServer, final PoolConfig config) {
        Validations.verifyNotNull( "RPC Server", rpcServer );

        m_config = config;
        SimPoolEnvironmentResource m_simPoolEnvironmentResource = new SimPoolEnvironmentResource(this, rpcServer);
        rpcServer.register(
                null,
                m_simPoolEnvironmentResource);
        populateSimPool( config );

    }

    public static UUID incrementUuid(UUID uuid) {
        // Define the expected length of the string
        final int EXPECTED_LENGTH = 36;
        // Define the start and end indices for the last 8 digits
        final int START_INDEX = 28;
        final int END_INDEX = 36; // Exclusive

        String inputString = uuid.toString();
        // Basic validation for the input string format
        if (inputString == null || inputString.length() != EXPECTED_LENGTH ||
                inputString.charAt(8) != '-' || inputString.charAt(13) != '-' ||
                inputString.charAt(18) != '-' || inputString.charAt(23) != '-') {
            throw new IllegalArgumentException("Input string is not in the expected UUID-like format (e.g., XXXXXXXX-XXXX-XXXX-XXXX-YYYYYYYY).");
        }

        String prefix = inputString.substring(0, START_INDEX);
        String last8DigitsStr = inputString.substring(START_INDEX, END_INDEX);

        long currentNumber;
        try {
            currentNumber = Long.parseLong(last8DigitsStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The last 8 characters are not valid digits: " + last8DigitsStr, e);
        }

        // Increment the number
        currentNumber++;

        // Handle potential overflow if it goes beyond 8 digits (e.g., 99999999 + 1 = 100000000)
        // We want it to wrap around to 0 if it exceeds 8 digits.
        // The maximum 8-digit number is 99,999,999.
        // If currentNumber becomes 100,000,000, we want it to be 0.
        // If the requirement is to keep incrementing beyond 8 digits, this logic needs adjustment.
        // For this function, we assume it should remain 8 digits, so it wraps.
        if (currentNumber > 99999999L) {
            currentNumber = 0; // Wrap around to 0 if it overflows 8 digits
        }


        // Format the number back to an 8-digit string with leading zeros
        String incrementedDigitsStr = String.format("%08d", currentNumber);

        // Reconstruct the full string
        return UUID.fromString(prefix + incrementedDigitsStr);

    }

    private void populateSimPool(PoolConfig config) {
        LOG.info( Platform.NEWLINE + Platform.NEWLINE +
                "Initializing PoolSimulator state." );
        UUID originalUuid = UUID.fromString("00000000-0000-0000-0000-000000000000");
        for (int p = 0; p < config.getNumPools(); p++) {
            originalUuid = incrementUuid(originalUuid);
            final String poolId = originalUuid.toString();
            final SimPool pool = BeanFactory.newBean(SimPool.class);
            //pool.setPoolId(poolId);
            pool.setPoolId(null);
            pool.setPartitionId(poolId);
            pool.setGuid(DEFAULT_POOL_SN + "-" + p);
            pool.setName("Pool-" + p);
            pool.setQuiesced(Quiesced.YES);
            pool.setOnline(true);
            pool.setHealth("ONLINE");
            pool.setPoweredOn(true);
            String configMountPoint = config.getMountPoint() + "/Pool" + p;
            pool.setMountPoint(configMountPoint);
            pool.setTotalRawCapacity(8000L * 1000 * 1000 * 1000 );
            pool.setUsedRawCapacity(100L);
            pool.setAvailableRawCapacity(8000L * 1000 * 1000 * 1000 );
            addPool(pool);
        }
    }

    @Override
    public boolean hasChanged() {
        return m_changed;
    }

    @Override
    public SimPool getPool(String poolGuid) {
        for ( final SimPool s : m_simDevices.getPools().values() )
        {
            if ( s.getGuid() != null && poolGuid.equals( s.getGuid() ) )
            {
                return s;
            }
        }

        return null;
    }

    @Override
    public Set<SimPool> getPools() {
        Collection<SimPool> valuesCollection = m_simDevices.getPools().values();
        return new HashSet<>(valuesCollection);
    }

    @Override
    public void simulateDelay(int maxMillis) {
        final int delayInMillis = ( 0 >= maxMillis ) ?
                1
                : new SecureRandom().nextInt( maxMillis );
        try
        {
            Thread.sleep( delayInMillis );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    @Override
    public void simulateDelay(int maxMillis, String action) {

    }

    @Override
    public RpcFuture<SimPool> addPool(SimPool pool) {
        m_changed = true;
        Validations.verifyNotNull( "Pool", pool );
        m_simDevices.getPools().put( pool.getGuid(), pool );
        return new RpcResponse<>( pool );
    }

    @Override
    public RpcFuture<SimPool> updatePool(String serialNumber, String message, boolean online) {
        return null;
    }
    @Override
    public PoolConfig getPoolConfig() {
        return m_config;
    }

    public void cleanup() {
        LOG.info("Delete pools on startup enabled in config. Will attempt to delete all files in virtual library directory: " + m_config.getMountPoint());
        try {
            for (int p = 0; p < m_config.getNumPools(); p++) {
                final File f = new File(m_config.getMountPoint() + "/Pool" + p);
                if (f.exists()) {
                    LOG.info("Deleting virtual library directory: ----------" + m_config.getMountPoint() + f.exists());
                    for (File file : Objects.requireNonNull(f.listFiles())) {
                        deleteNonUnderscoreFiles(file);
                    }
                }
            }


        } catch (Exception e) {
            LOG.warn("Could not delete files in library directory: " + m_config.getMountPoint(), e);
        }
        LOG.info("Deleted all files in pool mount directory.");
    }
}
