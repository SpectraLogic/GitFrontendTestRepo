package com.spectralogic.s3.simulator.state.simresource;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.rpc.tape.domain.ElementAddressBlockInformation;
import com.spectralogic.s3.simulator.domain.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class SimDevices {
    public Map<String, SimLibrary> getLibraries() {
        return m_libraries;
    }

    public Map<String, SimPartition> getPartitions() {
        return m_partitions;
    }

    public Map<String, SimDrive> getDrives() {
        return m_drives;
    }

    public Map<String, SimTape> getTapes() {
        return m_tapes;
    }

    public Map<String, SimPool> getPools() {
        return m_pools;
    }

    public Map<String, Map<ElementAddressType, Set<ElementAddressBlockInformation>>> getElementAddressBlocks() {
        return m_elementAddressBlocks;
    }

    private final Map< String, SimLibrary> m_libraries = new HashMap<>();
    private final Map< String, SimPartition> m_partitions = new HashMap<>();
    private final Map< String, SimDrive> m_drives = new HashMap<>();
    private final Map< String, SimTape> m_tapes = new HashMap<>();
    private final Map< String, SimPool> m_pools = new HashMap<>();
    private final Map< String, Map<ElementAddressType, Set<ElementAddressBlockInformation>> >
            m_elementAddressBlocks = new HashMap<>(); // Map< Partition S/N, Map< EAT, Set< EABI > > >
}
