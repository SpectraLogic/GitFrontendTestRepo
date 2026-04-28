/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.testfrmwrk;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.domain.pool.ObsoleteBlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolHealth;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.*;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ImportExportConfiguration;
import com.spectralogic.s3.common.dao.domain.tape.ImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.ObsoleteBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.RawImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDensityDirective;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.orm.BlobRM;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumGenerator;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.TestUtil;
import lombok.NonNull;

public class MockDaoDriver
{
    public MockDaoDriver( final DatabaseSupport databaseSupport )
    {
        m_databaseSupport = databaseSupport;
    }
    
    
    public < T extends DatabasePersistable > T retrieve( final Class< T > type, final UUID id )
    {
        return m_databaseSupport.getServiceManager().getRetriever( type ).retrieve( id );
    }
    
    
    public < T extends DatabasePersistable > T retrieve( final Class< T > type, final T bean )
    {
        return retrieve( type, bean.getId() );
    }
    
    
    @SuppressWarnings( "unchecked" )
    public < T extends DatabasePersistable > T retrieve( final T bean )
    {
        return retrieve( (Class< T >)InterfaceProxyFactory.getType( bean.getClass() ), bean.getId() );
    }


    public < T extends DatabasePersistable > void create( final T bean )
    {
        m_databaseSupport.getServiceManager()
                .getCreator( (Class< T >)InterfaceProxyFactory.getType( bean.getClass() ) ).create( bean );
    }


    public < T extends DatabasePersistable > Set< T > retrieveAll( final Class< T > type )
    {
        return m_databaseSupport.getServiceManager().getRetriever(type).retrieveAll().toSet();
    }

    
    
    public < T extends DatabasePersistable > T attainOneAndOnly( final Class< T > type )
    {
        return m_databaseSupport.getServiceManager().getRetriever( type ).attain( Require.nothing() );
    }
    
    
    public < T extends DatabasePersistable > T attain( final Class< T > type, final UUID id )
    {
        return m_databaseSupport.getServiceManager().getRetriever( type ).attain( id );
    }
    
    
    public < T extends DatabasePersistable > T attain( final Class< T > type, final T bean )
    {
        return attain( type, bean.getId() );
    }
    

    @SuppressWarnings( "unchecked" )
    public < T extends DatabasePersistable > T attain( final T bean )
    {
        return attain( (Class< T >)InterfaceProxyFactory.getType( bean.getClass() ), bean.getId() );
    }
    
    
    public < T extends DatabasePersistable > void attainAndUpdate( final Class< T > type, final T bean )
    {
        final T latest = attain( type, bean );
        BeanCopier.copy( bean, latest );
    }
    
    
    public < T extends DatabasePersistable > void attainAndUpdate( final T bean )
    {
        final T latest = attain( bean );
        BeanCopier.copy( bean, latest );
    }
    
    
    public < T extends DatabasePersistable > void delete( final Class< T > type, final T bean )
    {
        delete( type, bean.getId() );
    }


    public < T extends DatabasePersistable > void delete( final Class< T > type, final Collection<T> beans )
    {
        m_databaseSupport.getServiceManager().getDeleter(type)
                .delete(Require.beanPropertyEqualsOneOf(
                        Identifiable.ID,
                        BeanUtils.extractPropertyValues(beans, Identifiable.ID) ) );
    }
    
    
    public < T extends DatabasePersistable > void delete( final Class< T > type, final UUID beanId )
    {
        m_databaseSupport.getDataManager().deleteBean( type, beanId );
    }
    
    
    public < T extends DatabasePersistable > void deleteAll( final Class< T > type )
    {
        m_databaseSupport.getDataManager().deleteBeans( type, Require.nothing() );
    }
    
    
    public FeatureKey createFeatureKey( final FeatureKeyType key )
    {
        return createFeatureKey( key, null, null );
    }
    
    
    public FeatureKey createFeatureKey( 
            FeatureKeyType key, 
            final Long limit,
            final Date expiration )
    {
        if ( null == key )
        {
            key = FeatureKeyType.values()[ 0 ];
        }
        
        final FeatureKey retval = BeanFactory.newBean( FeatureKey.class )
                .setKey( key )
                .setLimitValue( limit )
                .setExpirationDate( expiration );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    

    public User createUser( final String userName )
    {
        final User user = BeanFactory.newBean( User.class )
                .setName( userName )
                .setAuthId( userName + "_authid" )
                .setSecretKey( "mySecretKey" );
        m_databaseSupport.getDataManager().createBean( user );
        return user;
    }
    
    
    public Group createGroup( final String groupName )
    {
        final Group group = BeanFactory.newBean( Group.class )
                .setName( groupName );
        m_databaseSupport.getDataManager().createBean( group );
        return group;
    }
    
    
    public Group getBuiltInGroup( final BuiltInGroup group )
    {
        return m_databaseSupport.getServiceManager().getService( 
                GroupService.class ).getBuiltInGroup( group );
    }
    
    
    public BucketAcl addBucketAcl( 
            final UUID bucketId,
            final UUID groupId,
            final UUID userId,
            final BucketAclPermission permission )
    {
        final BucketAcl acl = BeanFactory.newBean( BucketAcl.class )
                .setBucketId( bucketId ).setGroupId( groupId )
                .setUserId( userId ).setPermission( permission );
        m_databaseSupport.getDataManager().createBean( acl );
        return acl;
    }
    
    
    public DataPolicyAcl addDataPolicyAcl( 
            final UUID dataPolicyId,
            final UUID groupId,
            final UUID userId )
    {
        final DataPolicyAcl acl = BeanFactory.newBean( DataPolicyAcl.class )
                .setDataPolicyId( dataPolicyId ).setGroupId( groupId )
                .setUserId( userId );
        m_databaseSupport.getDataManager().createBean( acl );
        return acl;
    }
    
    
    public GroupMember addUserMemberToGroup( final UUID groupId, final UUID memberId )
    {
        final GroupMember member = BeanFactory.newBean( GroupMember.class )
                .setGroupId( groupId )
                .setMemberUserId( memberId );
        m_databaseSupport.getDataManager().createBean( member );
        return member;
    }
    
    
    public GroupMember addUserMemberToGroup( final BuiltInGroup group, final UUID memberId )
    {
        return addUserMemberToGroup( 
                m_databaseSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup(
                        group ).getId(),
                memberId );
    }
    
    
    public GroupMember addGroupMemberToGroup( final UUID groupId, final UUID memberId )
    {
        final GroupMember member = BeanFactory.newBean( GroupMember.class )
                .setGroupId( groupId )
                .setMemberGroupId( memberId );
        m_databaseSupport.getDataManager().createBean( member );
        return member;
    }
    
    
    public void deleteGroupMember( final UUID memberId )
    {
        m_databaseSupport.getDataManager().deleteBean( GroupMember.class, memberId );
    }
    

    public DataPolicy createDataPolicy( final String dataPolicyName )
    {
        final DataPolicy retval = BeanFactory.newBean( DataPolicy.class )
                .setName( dataPolicyName );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public StorageDomain createStorageDomain( final String name )
    {
        final StorageDomain retval = BeanFactory.newBean( StorageDomain.class )
                .setName( name );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public StorageDomainMember addTapePartitionToStorageDomain( 
            final UUID storageDomainId, UUID tapePartitionId, TapeType mediaType )
    {
        return addTapePartitionToStorageDomain( storageDomainId, tapePartitionId, mediaType, null );
    }
    
    
    public StorageDomainMember addTapePartitionToStorageDomain( 
            final UUID storageDomainId, 
            UUID tapePartitionId, 
            TapeType mediaType,
            final WritePreferenceLevel writePreference )
    {
        if ( null == tapePartitionId )
        {
            tapePartitionId = createTapePartition(
                    null, String.valueOf( new SecureRandom().nextInt() ) ).getId();
        }
        if ( null == mediaType )
        {
            mediaType = TapeType.values()[ 0 ];
        }
        
        final StorageDomainMember retval = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( storageDomainId )
                .setTapePartitionId( tapePartitionId )
                .setTapeType( mediaType );
        if ( null != writePreference )
        {
            retval.setWritePreference( writePreference );
        }
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public StorageDomainMember addPoolPartitionToStorageDomain(
            final UUID storageDomainId, UUID poolPartitionId )
    {
        return addPoolPartitionToStorageDomain( storageDomainId, poolPartitionId, null );
    }
    
    
    public StorageDomainMember addPoolPartitionToStorageDomain(
            final UUID storageDomainId, UUID poolPartitionId, WritePreferenceLevel writePreference )
    {
        if ( null == poolPartitionId )
        {
            poolPartitionId = createPoolPartition(
                    PoolType.values()[ 0 ], String.valueOf( new SecureRandom().nextInt() ) ).getId();
        }

        final StorageDomainMember retval = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( storageDomainId )
                .setPoolPartitionId( poolPartitionId );
        if ( null != writePreference )
        {
            retval.setWritePreference( writePreference );
        }
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public DataPersistenceRule createDataPersistenceRule(
            final UUID dataPolicyId, 
            final DataPersistenceRuleType type, 
            final UUID storageDomainId )
    {
        return createDataPersistenceRule( null, dataPolicyId, type, storageDomainId );
    }
    
    
    public DataPersistenceRule createDataPersistenceRule( 
            DataIsolationLevel isolationLevel,
            final UUID dataPolicyId, 
            DataPersistenceRuleType type, 
            final UUID storageDomainId )
    {
        if ( null == isolationLevel )
        {
            isolationLevel = DataIsolationLevel.STANDARD;
        }
        if ( null == type )
        {
            type = DataPersistenceRuleType.PERMANENT;
        }
        final DataPersistenceRule retval = BeanFactory.newBean( DataPersistenceRule.class )
                .setState( DataPlacementRuleState.NORMAL )
                .setDataPolicyId( dataPolicyId )
                .setType( type )
                .setStorageDomainId( storageDomainId )
                .setIsolationLevel( isolationLevel );
        if( DataPersistenceRuleType.TEMPORARY == type )
        {
            retval.setMinimumDaysToRetain( Integer.valueOf( 365 ) );
        }
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public Ds3DataReplicationRule createDs3DataReplicationRule( 
            final UUID dataPolicyId, 
            DataReplicationRuleType type, 
            UUID ds3TargetId )
    {
        if ( null == type )
        {
            type = DataReplicationRuleType.PERMANENT;
        }
        if ( null == ds3TargetId )
        {
            ds3TargetId = createDs3Target( null ).getId();
        }
        final Ds3DataReplicationRule retval = BeanFactory.newBean( Ds3DataReplicationRule.class )
                .setState( DataPlacementRuleState.NORMAL )
                .setDataPolicyId( dataPolicyId )
                .setType( type )
                .setTargetId( ds3TargetId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public AzureDataReplicationRule createAzureDataReplicationRule( 
            final UUID dataPolicyId, 
            DataReplicationRuleType type, 
            UUID azureTargetId )
    {
        if ( null == type )
        {
            type = DataReplicationRuleType.PERMANENT;
        }
        if ( null == azureTargetId )
        {
            azureTargetId = createAzureTarget( null ).getId();
        }
        final AzureDataReplicationRule retval = BeanFactory.newBean( AzureDataReplicationRule.class )
                .setState( DataPlacementRuleState.NORMAL )
                .setDataPolicyId( dataPolicyId )
                .setType( type )
                .setTargetId( azureTargetId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public S3DataReplicationRule createS3DataReplicationRule( 
            final UUID dataPolicyId, 
            DataReplicationRuleType type, 
            UUID s3TargetId )
    {
        if ( null == type )
        {
            type = DataReplicationRuleType.PERMANENT;
        }
        if ( null == s3TargetId )
        {
            s3TargetId = createS3Target( null ).getId();
        }
        final S3DataReplicationRule retval = BeanFactory.newBean( S3DataReplicationRule.class )
                .setState( DataPlacementRuleState.NORMAL )
                .setDataPolicyId( dataPolicyId )
                .setType( type )
                .setTargetId( s3TargetId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public Ds3Target createDs3Target( final String name )
    {
        return createDs3Target( UUID.randomUUID(), name );
    }
    
    
    public Ds3Target createDs3Target( final UUID id, String name )
    {
        if ( null == name )
        {
            name = "DS3Target-" + UUID.randomUUID();
        }
        
        final Ds3Target retval = BeanFactory.newBean( Ds3Target.class )
                .setAdminAuthId( "aid" )
                .setAdminSecretKey( "ask" )
                .setDataPathEndPoint( "dp" )
                .setName( name );
        retval.setId( id );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public Ds3TargetReadPreference createDs3TargetReadPreference(
            UUID ds3TargetId, 
            UUID bucketId,
            TargetReadPreferenceType preference )
    {
        if ( null == ds3TargetId )
        {
            ds3TargetId = createDs3Target( null ).getId();
        }
        bucketId = ensureBucketExists( bucketId );
        if ( null == preference )
        {
            preference = TargetReadPreferenceType.values()[ 0 ];
        }
        
        final Ds3TargetReadPreference retval = 
                BeanFactory.newBean( Ds3TargetReadPreference.class )
                .setTargetId( ds3TargetId )
                .setBucketId( bucketId )
                .setReadPreference( preference );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }

    
    public S3Target createS3TargetToAmazon( final String name )
    {
        //If we leave the dataPathEndPoint null it points to amazon 
        return createS3TargetInternal( name, null );
    }
    
    
    public S3Target createS3Target( final String name )
    {
        final String dataPathEndPoint = "S3EndPoint" + UUID.randomUUID();
        return createS3TargetInternal( name, dataPathEndPoint );
    }
    
    
    private S3Target createS3TargetInternal( String name, final String endPoint )
    {
        if ( null == name )
        {
            name = "S3Target-" + UUID.randomUUID();
        }
        final String key = "key-" + UUID.randomUUID();
        final S3Target retval = BeanFactory.newBean( S3Target.class )
                .setAccessKey( key )
                .setSecretKey( "ask" )
                .setName( name )
                .setOfflineDataStagingWindowInTb( 1 )
                .setDataPathEndPoint( endPoint );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }


    public void addS3FeatureKey( )
    {
        final String key = "key-" + UUID.randomUUID();
        final FeatureKey retval = BeanFactory.newBean( FeatureKey.class )
                        .setKey(FeatureKeyType.AWS_S3_CLOUD_OUT)
                                .setLimitValue(1152921504606846976L);

        m_databaseSupport.getDataManager().createBean( retval );

    }
    
    
    public S3TargetBucketName createS3TargetBucketName( 
            UUID bucketId,
            UUID targetId,
            String name )
    {
        if ( null == bucketId )
        {
            bucketId = ensureBucketExists( null );
        }
        if ( null == targetId )
        {
            targetId = createS3Target( null ).getId();
        }
        if ( null == name )
        {
            name = "custombucketname";
        }
        
        final S3TargetBucketName retval = BeanFactory.newBean( S3TargetBucketName.class )
                .setBucketId( bucketId )
                .setTargetId( targetId )
                .setName( name );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public AzureTarget createAzureTarget( String name )
    {
        if ( null == name )
        {
            name = "Azure-" + UUID.randomUUID();
        }
        
        final String accountName = "AzureAcct" + UUID.randomUUID(); 
        final AzureTarget retval = BeanFactory.newBean( AzureTarget.class )
                .setAccountName( accountName )
                .setAccountKey( "ask" )
                .setName( name );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public AzureTargetBucketName createAzureTargetBucketName( 
            UUID bucketId,
            UUID targetId,
            String name )
    {
        if ( null == bucketId )
        {
            bucketId = ensureBucketExists( null );
        }
        if ( null == targetId )
        {
            targetId = createAzureTarget( null ).getId();
        }
        if ( null == name )
        {
            name = "custombucketname";
        }
        
        final AzureTargetBucketName retval = BeanFactory.newBean( AzureTargetBucketName.class )
                .setBucketId( bucketId )
                .setTargetId( targetId )
                .setName( name );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public AzureTargetReadPreference createAzureTargetReadPreference(
            UUID azureTargetId, 
            UUID bucketId,
            TargetReadPreferenceType preference )
    {
        if ( null == azureTargetId )
        {
            azureTargetId = createAzureTarget( null ).getId();
        }
        bucketId = ensureBucketExists( bucketId );
        if ( null == preference )
        {
            preference = TargetReadPreferenceType.values()[ 0 ];
        }
        
        final AzureTargetReadPreference retval = 
                BeanFactory.newBean( AzureTargetReadPreference.class )
                .setTargetId( azureTargetId )
                .setBucketId( bucketId )
                .setReadPreference( preference );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public S3TargetReadPreference createS3TargetReadPreference(
            UUID s3TargetId, 
            UUID bucketId,
            TargetReadPreferenceType preference )
    {
        if ( null == s3TargetId )
        {
            s3TargetId = createS3Target( null ).getId();
        }
        bucketId = ensureBucketExists( bucketId );
        if ( null == preference )
        {
            preference = TargetReadPreferenceType.values()[ 0 ];
        }
        
        final S3TargetReadPreference retval = 
                BeanFactory.newBean( S3TargetReadPreference.class )
                .setTargetId( s3TargetId )
                .setBucketId( bucketId )
                .setReadPreference( preference );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    

    public Bucket createBucket( final UUID userId, final String bucketName )
    {
        return createBucket( userId, null, bucketName );
    }
    

    public Bucket createBucket( final UUID userId, final UUID dataPolicyId, final String bucketName )
    {
        final Bucket bucket = BeanFactory.newBean( Bucket.class )
                .setUserId( ensureUserExists( userId ) )
                .setDataPolicyId( ensureDataPolicyExists( dataPolicyId ) )
                .setName( bucketName );
        m_databaseSupport.getDataManager().createBean( bucket );
        return bucket;
    }
    
    
    public DataPolicy getDataPolicyFor( final UUID bucketId )
    {
        return m_databaseSupport.getServiceManager().getRetriever( DataPolicy.class ).attain( 
                m_databaseSupport.getServiceManager().getRetriever( Bucket.class ).attain(
                        bucketId ).getDataPolicyId() );
    }
    

    public S3Object createObject( final UUID bucketId, final String objectName )
    {
        return createObject( 
                bucketId,
                objectName, 
                ( objectName.endsWith( S3Object.DELIMITER ) ) ? 0 : 10,
                null );
    }


    public S3Object createObject( final UUID bucketId, final String objectName, final Date creationDate )
    {
        return createObject( 
                bucketId,
                objectName, 
                ( objectName.endsWith( S3Object.DELIMITER ) ) ? 0 : 10,
                creationDate );
    }


    public S3Object createObject(
            final UUID bucketId, 
            final String objectName,
            final long size )
    {
        return createObject( 
                bucketId,
                objectName, 
                size,
                null );
    }


    public S3Object createObject(
            final UUID bucketId, 
            final String objectName,
            final long size,
            final Date creationDate )
    {
    	final S3Object object = createObjectStub( bucketId, objectName, size );
    	updateBean( object.setCreationDate( creationDate ), S3Object.CREATION_DATE );
    	simulateObjectUploadCompletion( object.getId() );
    	return object;
    }
    
    
    public S3Object createObjectStub(
            final UUID bucketId, 
            final String objectName,
            final long size )
    {
        final UUID actualBucketId = ensureBucketExists( bucketId );
        final S3ObjectType type = objectName.endsWith( S3Object.DELIMITER ) ?
                S3ObjectType.FOLDER
                : S3ObjectType.DATA;
                
        final S3Object object = BeanFactory.newBean( S3Object.class )
                .setBucketId( actualBucketId )
                .setName( objectName )
                .setType( type );
        final BeansServiceManager transaction = m_databaseSupport.getServiceManager().startTransaction();
        try
        {
        	transaction.getService( S3ObjectService.class )
        		.create( CollectionFactory.toSet( object ) );
        	transaction.commitTransaction();
        }
        finally
        {
        	transaction.closeTransaction();
        }
        
        if ( 0 <= size )
        {
            m_databaseSupport.getDataManager().createBean( BeanFactory.newBean( Blob.class )
                    .setByteOffset( 0 )
                    .setLength( size )
                    .setObjectId( object.getId() ) );
        }
        return object;
    }


    public Obsoletion createObsoletion(
            final Date date )
    {
        final Obsoletion obs = BeanFactory.newBean( Obsoletion.class )
                .setDate( date );
        final BeansServiceManager transaction = m_databaseSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( ObsoletionService.class )
                    .create( CollectionFactory.toSet( obs ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        return obs;
    }
    
    
    public Blob getBlobFor( final UUID objectId )
    {
        return m_databaseSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, objectId );
    }


    public JobEntry getJobEntryFor(final UUID blobId) {
        return m_databaseSupport.getServiceManager().getRetriever(JobEntry.class).attain(
                JobEntry.BLOB_ID, blobId);
    }


    public Blob createBlob( UUID objectId, final long size ) {
        return createBlobs( objectId, 1, size ).get( 0 );
    }


    public List< Blob > createBlobs( UUID objectId, final int numberOfBlobs, final long sizePerBlob )
    {
        return createBlobs( objectId, numberOfBlobs, 0, sizePerBlob );
    }



    public List< Blob > createBlobsAtOffset( UUID objectId, final int numberOfBlobs, final long sizePerBlob, final long offset )
    {
        return createBlobs( objectId, numberOfBlobs, offset, sizePerBlob );
    }


    public List< Blob > createBlobs( 
            UUID objectId, final int numberOfBlobs, final long offset, final long sizePerBlob )
    {
        objectId = ensureObjectExists( objectId );
        final List< Blob > blobs = new ArrayList<>();
        for ( int i = 0; i < numberOfBlobs; ++i )
        {
            blobs.add( BeanFactory.newBean( Blob.class )
                    .setByteOffset( offset + i * sizePerBlob ).setLength( sizePerBlob )
                    .setObjectId( objectId ) );
        }
        
        final DataManager transaction =
                m_databaseSupport.getDataManager().startTransaction();
        transaction.createBeans( new HashSet<>( blobs ) );
        transaction.commitTransaction();
        
        return blobs;
    }
    
    
    public DegradedBlob createDegradedBlob( final UUID blobId, final UUID ruleId )
    {
        final DataPersistenceRule persistenceRule = 
                m_databaseSupport.getServiceManager().getRetriever( DataPersistenceRule.class ).retrieve( 
                        ruleId );
        
        final DegradedBlob bean = 
                BeanFactory.newBean( DegradedBlob.class ).setBlobId( blobId );
        if ( null != persistenceRule )
        {
            bean.setPersistenceRuleId( ruleId );
        }
        else
        {
            if ( null != m_databaseSupport.getServiceManager().getRetriever( 
                    Ds3DataReplicationRule.class ).retrieve( ruleId ) )
            {
                bean.setDs3ReplicationRuleId( ruleId );
            }
            else if ( null != m_databaseSupport.getServiceManager().getRetriever( 
                    AzureDataReplicationRule.class ).retrieve( ruleId ) )
            {
                bean.setAzureReplicationRuleId( ruleId );
            }
            else if ( null != m_databaseSupport.getServiceManager().getRetriever( 
                            S3DataReplicationRule.class ).retrieve( ruleId ) )
            {
                bean.setS3ReplicationRuleId( ruleId );
            }
            else
            {
                throw new UnsupportedOperationException(
                        "Cannot determine what kind of rule " + ruleId + " is." );
            }
        }
        final S3Object o = m_databaseSupport.getServiceManager().getRetriever( S3Object.class ).attain( 
                Require.exists( 
                        Blob.class,
                        Blob.OBJECT_ID, 
                        Require.beanPropertyEquals( Identifiable.ID, blobId ) ) );
        m_databaseSupport.getDataManager().createBean( bean.setBucketId( o.getBucketId() ) );
        return bean;
    }
    
    
    public CanceledJob createCanceledJob( 
            UUID bucketId, UUID userId, JobRequestType requestType, Date dateCanceled )
    {
        bucketId = ensureBucketExists( bucketId );
        userId = ensureUserExists( userId );
        if ( null == requestType )
        {
            requestType = JobRequestType.values()[ 0 ];
        }
        final CanceledJob job = BeanFactory.newBean( CanceledJob.class )
                .setBucketId( bucketId ).setRequestType( requestType ).setUserId( userId )
                .setPriority( BlobStoreTaskPriority.URGENT );
        if ( null != dateCanceled )
        {
            job.setDateCanceled( dateCanceled );
        }
        m_databaseSupport.getDataManager().createBean( job );
        return job;
    }
    
    
    public CompletedJob createCompletedJob(
            UUID bucketId, UUID userId, JobRequestType requestType, Date dateCompleted )
    {
        bucketId = ensureBucketExists( bucketId );
        userId = ensureUserExists( userId );
        if ( null == requestType )
        {
            requestType = JobRequestType.values()[ 0 ];
        }
        final CompletedJob job = BeanFactory.newBean( CompletedJob.class )
                .setBucketId( bucketId ).setRequestType( requestType ).setUserId( userId )
                .setPriority( BlobStoreTaskPriority.URGENT );
        if ( null != dateCompleted )
        {
            job.setDateCompleted( dateCompleted );
        }
        m_databaseSupport.getDataManager().createBean( job );
        return job;
    }
    
    
    public void enableImplicitJobResolutionForAllJobs()
    {
        updateAllBeans( 
                BeanFactory.newBean( Job.class ).setImplicitJobIdResolution( true ), 
                Job.IMPLICIT_JOB_ID_RESOLUTION );
    }
    
    
    public Job createJob( UUID bucketId, UUID userId, final JobRequestType requestType )
    {
        return createJobInternal( m_databaseSupport.getServiceManager(), bucketId, userId, requestType );
    }
    
    
    public Job createJobInternal( 
            final BeansServiceManager transaction,
            UUID bucketId, 
            UUID userId,
            final JobRequestType requestType )
    {
        bucketId = ensureBucketExists( bucketId );
        userId = ensureUserExists( userId );
        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucketId ).setRequestType( requestType ).setUserId( userId )
                .setPriority( BlobStoreTaskPriority.URGENT );
        transaction.getService( JobService.class ).create( job );
        return job;
    }


    public BlobCache markBlobInCache( final UUID blobId ) {
        return createBlobCache( blobId, CacheEntryState.IN_CACHE );
    }


    public BlobCache allocateBlob( final UUID blobId ) {
        return createBlobCache( blobId, CacheEntryState.ALLOCATED );
    }


    public BlobCache createBlobCache(final UUID blobId, final CacheEntryState state )
    {
        final Blob b = m_databaseSupport.getServiceManager().getRetriever( Blob.class ).attain( blobId );
        final BlobCache retval = BeanFactory.newBean( BlobCache.class )
                .setBlobId( blobId )
                .setPath( "/test/path/for/blob/" +  blobId)
                .setState( state)
                .setLastAccessed( new Date() )
                .setSizeInBytes(b.getLength());
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }


    public void markBlobsInCache( final Collection< UUID > blobIds )
    {
        for ( final UUID blobId : blobIds )
        {
            markBlobInCache( blobId );
        }
    }


    public void allocateBlobs( final Collection< UUID > blobIds )
    {
        for ( final UUID blobId : blobIds )
        {
            allocateBlob( blobId );
        }
    }


    public JobEntry createJobWithEntry(final Blob blob ) {
        return createJobWithEntry( JobRequestType.PUT, blob );
    }


    public JobEntry createJobWithEntry(@NonNull final JobRequestType jobRequestType, @NonNull final Blob blob )
    {
        return createJobWithEntries(jobRequestType, Set.of(blob)).iterator().next();
    }


    public Set<JobEntry> createJobWithEntries(final Blob ... blobs ) {
        return createJobWithEntries(JobRequestType.PUT, blobs);
    }


    public Set<JobEntry> createJobWithEntries(JobRequestType jobRequestType, Blob ... blobs ) {
        return createJobWithEntries(jobRequestType, CollectionFactory.toSet(blobs));
    }

    public Set<JobEntry> createJobWithEntries(@NonNull JobRequestType jobRequestType, @NonNull final Collection< Blob > blobs )
    {
        final UUID someObjectId = blobs.iterator().next().getObjectId();
        final S3Object someObject =
                m_databaseSupport.getServiceManager().getRetriever( S3Object.class ).attain( someObjectId );
        final BeansServiceManager transaction =
                m_databaseSupport.getServiceManager().startTransaction();
        try
        {
            final Job job = createJobInternal(
                    transaction, someObject.getBucketId(), null, jobRequestType );
            final Set<JobEntry> retval = createJobEntriesInternal(transaction, job.getId(), blobs);
            transaction.commitTransaction();
            return retval;
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    //Invalid if there is more than one blob already in the database
    public JobEntry createJobEntry( @NonNull final UUID jobId ) {
        final Blob blob;
        if (getServiceManager().getRetriever(Blob.class).getCount() > 0) {
            blob = attainOneAndOnly(Blob.class);
        } else {
            final S3Object o = createObject(null, DEFAULT_OBJECT_NAME + UUID.randomUUID());
            blob = getBlobFor(o.getId());
        }
        return createJobEntry(jobId, blob);
    }




    public JobEntry createJobEntry( @NonNull UUID jobId, final Blob blob )
    {
        return createJobEntries(jobId, Set.of(blob)).iterator().next();
    }


    public Set<JobEntry> createJobEntries( @NonNull UUID jobId, final Blob ... blobs )
    {
        return createJobEntries(jobId, CollectionFactory.toSet( blobs ) );
    }


    public Set<JobEntry> createJobEntries(@NonNull final Collection< Blob > blobs ) {
        final UUID jobId = attainOneAndOnly(Job.class).getId();
        return createJobEntries(jobId, blobs);
    }

    public Set<JobEntry> createJobEntries(@NonNull final UUID jobId, @NonNull final Collection< Blob > blobs ) {
        final BeansServiceManager transaction =
                m_databaseSupport.getServiceManager().startTransaction();
        try
        {
            final Set< JobEntry > retval = createJobEntriesInternal( transaction, jobId, blobs );
            transaction.commitTransaction();
            return retval;
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    private Set< JobEntry > createJobEntriesInternal(
            @NonNull final BeansServiceManager transaction,
            @NonNull final UUID jobId,
            @NonNull final Collection< Blob > blobs )
    {
        final Job job = transaction.getService( JobService.class ).retrieve( jobId );

        /*
         *  Order blobs for the same object together / don't split them up since we won't split them up in
         *  production.
         */
        final Map< UUID, Set< Blob > > objectIdToBlobSetMap = new HashMap<>();
        final List< Set< Blob > > blobSets = new ArrayList<>();
        for ( final Blob blob : blobs )
        {
            if ( !objectIdToBlobSetMap.containsKey( blob.getObjectId() ) )
            {
                objectIdToBlobSetMap.put( blob.getObjectId(), new HashSet< Blob >() );
                blobSets.add( objectIdToBlobSetMap.get( blob.getObjectId() ) );
            }
            objectIdToBlobSetMap.get( blob.getObjectId() ).add( blob );
        }

        final Set< JobEntry > retval = new HashSet<>();
        for ( Set< Blob > blobsInObject : blobSets )
        {
            blobsInObject = BeanUtils.sort( blobsInObject );
            for ( final Blob blob : blobsInObject )
            {
                retval.add( BeanFactory.newBean( JobEntry.class )
                        .setChunkNumber(m_nextChunkNumber.getAndIncrement())
                        .setChunkId(UUID.randomUUID())
                        .setBlobId( blob.getId() ).setJobId( jobId ));
            }
        }

        transaction.getService( JobEntryService.class ).create( retval );

        return retval;
    }


    public <R extends DataReplicationRule<R> & DatabasePersistable, CT extends RemoteBlobDestination<CT> & DatabasePersistable> Set<CT>
    createReplicationTargetsForChunks(final Class<R> ruleType,
                                     final Class<CT> chunkTargetType,
                                     final Collection<JobEntry> chunks) {
        return chunks.stream()
                .flatMap(chunk -> {
                    return createReplicationTargetsForChunk(ruleType, chunkTargetType, chunk.getId()).stream();
                }).collect(Collectors.toSet());
    }


    public <R extends DataReplicationRule<R> & DatabasePersistable, CT extends RemoteBlobDestination<CT> & DatabasePersistable> Set<CT>
    createReplicationTargetsForChunk(final Class<R> ruleType,
                                     final Class<CT> chunkTargetType,
                                     final UUID chunkId) {
        final JobEntry entry = m_databaseSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunkId);
        final Job job = m_databaseSupport.getServiceManager().getRetriever(Job.class).attain(entry.getJobId());
        final Set<CT> chunkTargets = retrieveAll(ruleType).stream()
                .map(rule -> BeanFactory.newBean(chunkTargetType)
                        .setEntryId(chunkId)
                        .setRuleId(rule.getId())
                        .setTargetId(rule.getTargetId())
                ).collect(Collectors.toSet());


        try (final NestableTransaction transaction = m_databaseSupport.getServiceManager().startNestableTransaction()) {
            transaction.getCreator( chunkTargetType ).create( chunkTargets );
            transaction.commitTransaction();
        }
        return chunkTargets;
    }

    public  boolean createLocalBlobDestinations(final Collection<JobEntry> entries, final Collection<DataPersistenceRule> rules, UUID bucketId) {
        final Map<UUID, UUID> isolatedBucketIdsByRule = new HashMap<>();
        for (DataPersistenceRule rule : rules) {
            final UUID isolatedBucketId = rule.getIsolationLevel() == DataIsolationLevel.BUCKET_ISOLATED ? bucketId : null;
            isolatedBucketIdsByRule.put(rule.getId(), isolatedBucketId);
        };
        final Set<LocalBlobDestination> persistenceTargets = entries.stream()
                .flatMap(entry -> rules.stream()
                        .map(rule -> BeanFactory.newBean(LocalBlobDestination.class)
                                .setEntryId(entry.getId())
                                .setStorageDomainId(rule.getStorageDomainId())
                                .setIsolatedBucketId(isolatedBucketIdsByRule.get(rule.getId()))
                                .setPersistenceRuleId(rule.getId())

                        )
                ).collect(Collectors.toSet());
        final NestableTransaction transaction = m_databaseSupport.getServiceManager().startNestableTransaction();

        transaction.getService(LocalBlobDestinationService.class).create(persistenceTargets);
        transaction.commitTransaction();
        return !persistenceTargets.isEmpty();
    }

    public  boolean createS3BlobDestinations(final Collection<JobEntry> entries, final Collection<DataPersistenceRule> rules, UUID bucketId, UUID targetId) {
        final Map<UUID, UUID> isolatedBucketIdsByRule = new HashMap<>();
        for (DataPersistenceRule rule : rules) {
            final UUID isolatedBucketId = rule.getIsolationLevel() == DataIsolationLevel.BUCKET_ISOLATED ? bucketId : null;
            isolatedBucketIdsByRule.put(rule.getId(), isolatedBucketId);
        };
        final Set<S3BlobDestination> persistenceTargets = entries.stream()
                .flatMap(entry -> rules.stream()
                        .map(rule -> BeanFactory.newBean(S3BlobDestination.class)
                                .setEntryId(entry.getId())
                                .setTargetId(targetId)
                                .setEntryId(entry.getId())
                                .setRuleId(rule.getId())


                        )
                ).collect(Collectors.toSet());
        final NestableTransaction transaction = m_databaseSupport.getServiceManager().startNestableTransaction();

        transaction.getService(S3BlobDestinationService.class).create(persistenceTargets);
        transaction.commitTransaction();
        return !persistenceTargets.isEmpty();
    }


    public Set<LocalBlobDestination> createPersistenceTargetsForChunks(final Collection<JobEntry> chunks) {
        final Set<LocalBlobDestination> persistenceTargets = chunks.stream()
                .flatMap(chunk -> createPersistenceTargetsForChunk(chunk.getId()).stream())
                .collect(Collectors.toSet());
        return persistenceTargets;
    }


    public Set<LocalBlobDestination> createPersistenceTargetsForChunk(final UUID chunkId) {
        final DetailedJobEntry entry = m_databaseSupport.getServiceManager().getRetriever(DetailedJobEntry.class).attain(chunkId);
        final Job job = m_databaseSupport.getServiceManager().getRetriever(Job.class).attain(entry.getJobId());
        final Set<LocalBlobDestination> persistenceTargets = retrieveAll(DataPersistenceRule.class).stream()
                .map(rule -> {
                    return BeanFactory.newBean(LocalBlobDestination.class)
                            .setEntryId(chunkId)
                            .setStorageDomainId(rule.getStorageDomainId())
                            .setPersistenceRuleId(rule.getId())
                            .setIsolatedBucketId(rule.getIsolationLevel() == DataIsolationLevel.BUCKET_ISOLATED ? job.getBucketId() : null);
                }).collect(Collectors.toSet());

        try (final NestableTransaction transaction = m_databaseSupport.getServiceManager().startNestableTransaction()) {
            transaction.getService( LocalBlobDestinationService.class ).create( persistenceTargets );
            transaction.commitTransaction();
        }
        return persistenceTargets;
    }


    private UUID ensureUserExists( final UUID userId )
    {
        synchronized ( LOCK )
        {
            if ( null != userId )
            {
                return userId;
            }
            
            final User user = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( User.class )
                    .retrieve( NameObservable.NAME, DEFAULT_USER_NAME );
            if ( null != user )
            {
                return user.getId();
            }
            return createUser( DEFAULT_USER_NAME ).getId();
        }
    }
    
    
    private UUID ensureDataPolicyExists( final UUID dataPolicyId )
    {
        synchronized ( LOCK )
        {
            if ( null != dataPolicyId )
            {
                return dataPolicyId;
            }
            
            final DataPolicy dataPolicy = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( DataPolicy.class )
                    .retrieve( NameObservable.NAME, DEFAULT_DATA_POLICY_NAME );
            if ( null != dataPolicy )
            {
                return dataPolicy.getId();
            }
            return createDataPolicy( DEFAULT_DATA_POLICY_NAME ).getId();
        }
    }
    
    
    private UUID ensureABMConfigExists( final UUID dataPolicyId )
    {
        synchronized ( LOCK )
        {
            if ( null != dataPolicyId )
            {
                return dataPolicyId;
            }
            
            final DataPolicy dataPolicy = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( DataPolicy.class )
                    .retrieve( NameObservable.NAME, DEFAULT_DATA_POLICY_NAME );
            if ( null != dataPolicy )
            {
                return dataPolicy.getId();
            }
            return createABMConfigSingleCopyOnTape().getId();
        }
    }


    private UUID ensureBucketExists( final UUID bucketId )
    {
        synchronized ( LOCK )
        {
            if ( null != bucketId )
            {
                return bucketId;
            }
            
            final Bucket bucket = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( Bucket.class )
                    .retrieve( Bucket.NAME, DEFAULT_BUCKET_NAME );
            if ( null != bucket )
            {
                return bucket.getId();
            }
            return createBucket( null, DEFAULT_BUCKET_NAME ).getId();
        }
    }


    private UUID ensureObjectExists( final UUID objectId )
    {
        synchronized ( LOCK )
        {
            if ( null != objectId )
            {
                return objectId;
            }
    
            final S3Object object = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( S3Object.class )
                    .retrieve( S3Object.NAME, DEFAULT_OBJECT_NAME );
            if ( null != object )
            {
                return object.getId();
            }
            return createObject( null, DEFAULT_OBJECT_NAME ).getId();
        }
    }
    
    
    public List< S3ObjectProperty > createObjectProperties(
            final UUID objectId,
            final Map< String, String > propertiesMapping )
    {
        UUID actualObjectId = objectId;
        if ( objectId == null )
        {
            final S3Object object = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( S3Object.class )
                    .retrieve( S3Object.NAME, DEFAULT_OBJECT_NAME );
            actualObjectId = ( object == null ) ?
                    createBucket( null, DEFAULT_OBJECT_NAME ).getId()
                    : object.getId();
        }
        
        final List< S3ObjectProperty > properties = new ArrayList<>();
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            properties.add( BeanFactory.newBean( S3ObjectProperty.class )
                    .setKey( propertyEntry.getKey() )
                    .setValue( propertyEntry.getValue() ) );
        }
        m_databaseSupport.getServiceManager().getService( S3ObjectPropertyService.class )
                .createProperties( actualObjectId, properties );
        return properties;
    }
    
    
    public Tape createTape()
    {
        return createTape( null );
    }
    
    
    public void nullOutCapacityStats( final Tape tape )
    {
        updateBean( 
                tape.setAvailableRawCapacity( null ).setTotalRawCapacity( null ), 
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
    }
    
    
    public Tape createTape( final TapeState state )
    {
        return createTape( null, state );
    }
    
    
    public Tape createTape( final UUID partitionId, final TapeState state )
    {
        return createTape( partitionId, state, null );
    }
    
    
    public Tape createTape( final UUID partitionId, final TapeState state, final TapeType type )
    {
        final Tape retval = BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( type )
                .setAvailableRawCapacity(1000L * 1024L * 1024L)
                .setTotalRawCapacity(2000L * 1024L * 1024L)
                .setPartitionId( ensurePartitionExists( partitionId ) )
                .setLastCheckpoint( ( TapeState.NORMAL == state ) ? "new" : null );
        if ( null != state )
        {
            retval.setState( state );
        }
        if ( null == type )
        {
            retval.setType( TapeType.LTO5 );
        }
        m_databaseSupport.getDataManager().createBean( retval );
        return m_databaseSupport.getServiceManager().getService( TapeService.class ).retrieveAll(
                Require.beanPropertyEquals( Identifiable.ID, retval.getId() ) ).getFirst();
    }
    
    
    public TapeLibrary createLibrary( final String sn )
    {
        return createLibrary( UUID.randomUUID().toString(), sn );
    }
    
    
    public TapeLibrary createLibrary( final String name, final String sn )
    {
        final TapeLibrary retval = BeanFactory.newBean( TapeLibrary.class )
                .setManagementUrl( "a" ).setSerialNumber( sn )
                .setName( name );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    private UUID ensureLibraryExists( final UUID libraryId )
    {
        synchronized ( LOCK )
        {
            if ( null != libraryId )
            {
                return libraryId;
            }
            
            final TapeLibrary retval = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( TapeLibrary.class )
                    .retrieve( SerialNumberObservable.SERIAL_NUMBER, DEFAULT_LIBRARY_SN );
            if ( null != retval )
            {
                return retval.getId();
            }
            return createLibrary( DEFAULT_LIBRARY_SN ).getId();
        }
    }
    
    
    public TapePartition createTapePartition( UUID libraryId, String sn )
    {
        return createTapePartition( libraryId, sn, TapeDriveType.LTO6 );
    }


    public TapePartition createTapePartition( UUID libraryId, String sn, TapeDriveType driveType )
    {
        return createTapePartition(libraryId, sn, driveType, false);
    }
    
    
    public TapePartition createTapePartition( UUID libraryId, String sn, TapeDriveType driveType, boolean autoCompactionEnabled )
    {
        libraryId = ensureLibraryExists( libraryId );
        if ( null == sn )
        {
            sn = UUID.randomUUID().toString();
        }

        final TapePartition retval = BeanFactory.newBean( TapePartition.class )
                .setQuiesced( Quiesced.NO )
                .setDriveType( driveType )
                .setLibraryId( libraryId ).setName( "name-" + sn ).setSerialNumber( sn )
                .setImportExportConfiguration( ImportExportConfiguration.SUPPORTED )
                .setAutoCompactionEnabled(autoCompactionEnabled);
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public TapeDensityDirective createTapeDensityDirective( 
            UUID tapePartitionId,
            TapeType tapeType,
            TapeDriveType density )
    {
        if ( null == tapePartitionId )
        {
            tapePartitionId = createTapePartition( null, null ).getId();
        }
        if ( null == tapeType )
        {
            tapeType = TapeType.TS_JC;
        }
        if ( null == density )
        {
            density = TapeDriveType.TS1140;
        }
        
        final TapeDensityDirective retval = BeanFactory.newBean( TapeDensityDirective.class )
                .setPartitionId( tapePartitionId ).setTapeType( tapeType ).setDensity( density );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public PoolPartition createPoolPartition( PoolType type, final String name )
    {
        if ( null == type )
        {
            type = PoolType.values()[ 0 ];
        }
        final PoolPartition retval = BeanFactory.newBean( PoolPartition.class )
                .setType( type )
                .setName( name );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public UUID ensurePartitionExists( final UUID partitionId )
    {
        synchronized ( LOCK )
        {
            if ( null != partitionId )
            {
                return partitionId;
            }
            
            TapePartition retval = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( TapePartition.class )
                    .retrieve( SerialNumberObservable.SERIAL_NUMBER, DEFAULT_PARTITION_SN );
            if ( null == retval ) {
                //if the default tape partition is not present, use whatever is there.
                retval = m_databaseSupport
                        .getServiceManager()
                        .getRetriever( TapePartition.class )
                        .retrieveAll().getFirst();
            }
            if ( null != retval )
            {
                return retval.getId();
            }
            return createTapePartition( ensureLibraryExists( null ), DEFAULT_PARTITION_SN ).getId();
        }
    }


    public UUID ensurePoolPartitionExists( final UUID partitionId )
    {
        synchronized ( LOCK )
        {
            if ( null != partitionId )
            {
                return partitionId;
            }

            final PoolPartition retval = m_databaseSupport
                    .getServiceManager()
                    .getRetriever( PoolPartition.class )
                    .retrieve( Pool.NAME, DEFAULT_POOL_PARTITION_NAME );
            if ( null != retval )
            {
                return retval.getId();
            }
            return createPoolPartition( PoolType.ONLINE, DEFAULT_POOL_PARTITION_NAME ).getId();
        }
    }
    
    
    public TapeDrive createTapeDrive( UUID partitionId, final String sn )
    {
        return createTapeDrive( partitionId, sn, null );
    }
    
    
    public TapeDrive createTapeDrive( UUID partitionId, String sn, final UUID tapeId )
    {
        return createTapeDrive( partitionId, sn, tapeId, TapeDriveType.LTO5 );
    }
    
    
    public TapeDrive createTapeDrive( UUID partitionId, String sn, final UUID tapeId, final TapeDriveType tapeDriveType )
    {
        if ( null == sn )
        {
            sn = UUID.randomUUID()
                     .toString();
        }
        final TapeDrive retval = BeanFactory.newBean( TapeDrive.class )
                                            .setPartitionId( ensurePartitionExists( partitionId ) )
                                            .setSerialNumber( sn )
                                            .setType( tapeDriveType )
                                            .setTapeId( tapeId );
        m_databaseSupport.getDataManager()
                         .createBean( retval );
        return retval;
    }
    
    
    public void simulateObjectUploadCompletion( final UUID objectId )
    {
        m_databaseSupport.getDataManager().updateBeans( 
                CollectionFactory.toSet( ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE ),
                BeanFactory.newBean( Blob.class )
                    .setChecksum( "checksum" ).setChecksumType( ChecksumType.MD5 ),
                Require.beanPropertyEquals( Blob.OBJECT_ID, objectId ) );
        m_databaseSupport.getServiceManager().getService( S3ObjectService.class )
        	.markObjectReceived( retrieve( S3Object.class, objectId ) );
    }
    
    
    public void grantOwnerPermissionsToEveryUser()
    {
        for ( final User user 
                : m_databaseSupport.getServiceManager().getRetriever( User.class ).retrieveAll().toSet() )
        {
            addBucketAcl( null, null, user.getId(), BucketAclPermission.OWNER );
        }
    }
    
    
    public void grantDataPolicyAccessToEveryUser()
    {
        for ( final User user 
                : m_databaseSupport.getServiceManager().getRetriever( User.class ).retrieveAll().toSet() )
        {
            addDataPolicyAcl( null, null, user.getId() );
        }
    }
    
    
    public BlobTape putBlobOnTape( UUID tapeId, final UUID blobId )
    {
        if ( null == tapeId )
        {
            tapeId = createTape( TapeState.NORMAL ).getId();
        }
        
        final int nextOrderIndex = (int)m_databaseSupport.getDataManager().getMax(
                BlobTape.class, 
                BlobTape.ORDER_INDEX, 
                Require.beanPropertyEquals( BlobTape.TAPE_ID, tapeId ) ) + 1;
        final BlobTape retval = BeanFactory.newBean( BlobTape.class )
                .setBlobId( blobId ).setTapeId( tapeId ).setOrderIndex( nextOrderIndex );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }


    public List<BlobTape> putBlobsOnTape( UUID tapeId, final Blob ... blobId ) {
        return Arrays.stream(blobId).map((bt) -> putBlobOnTape(tapeId, bt.getId())).collect(Collectors.toList());
    }
    
    
    
    public BlobTape putBlobOnTapeAndDetermineStorageDomain( UUID tapeId, final UUID blobId )
    {
        if ( null == tapeId )
        {
            tapeId = createTape( TapeState.NORMAL ).getId();
        }
         
        final Set< DataPersistenceRule > rules =
                new BlobRM( blobId, m_databaseSupport.getServiceManager() )
                    .getObject().getBucket().getDataPolicy().getDataPersistenceRules().toSet();
        
        if ( 1 != rules.size() )
        {
            throw new IllegalStateException(" Cannot automatically determine which storage domain to use for"
                    + "data policy with " + rules.size() + " rules.");
        }
        return putBlobOnTapeForStorageDomain( tapeId , blobId, rules.iterator().next().getStorageDomainId() );
    }
    
    
    public BlobTape putBlobOnTapeForStorageDomain( UUID tapeId, final UUID blobId, final UUID storageDomainId )
    {
        final int nextOrderIndex = (int)m_databaseSupport.getDataManager().getMax(
                BlobTape.class, 
                BlobTape.ORDER_INDEX, 
                Require.beanPropertyEquals( BlobTape.TAPE_ID, tapeId ) ) + 1;
        final BlobTape retval = BeanFactory.newBean( BlobTape.class )
                .setBlobId( blobId ).setTapeId( tapeId ).setOrderIndex( nextOrderIndex );
        m_databaseSupport.getDataManager().createBean( retval );
        
        final Tape tape = attain( Tape.class, tapeId ).setAssignedToStorageDomain(true);
        final UUID storageDomainMemberId =
                m_databaseSupport.getServiceManager().getService( StorageDomainService.class )
                    .selectAppropriateStorageDomainMember( tape, storageDomainId );
        if ( null != tape.getStorageDomainMemberId()
                && !tape.getStorageDomainMemberId().equals( storageDomainMemberId ) )
        {
            throw new IllegalStateException( "Cannot assign tape to storage domain member id " + storageDomainMemberId 
                    + ", it is already assigned to " + tape.getStorageDomainMemberId() + "." );
        }
        updateBean( tape.setStorageDomainMemberId( storageDomainMemberId ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        return retval;
    }
    
    
    public BlobPool putBlobOnPool( UUID poolId, final UUID blobId )
    {
        return putBlobOnPool( poolId, blobId, null, null );
    }
    
    
    public BlobPool putBlobOnPool( 
            UUID poolId, final UUID blobId, Date dateWritten, Date lastAccessed )
    {
        if ( null == poolId )
        {
            poolId = createPool( PoolState.NORMAL ).getId();
        }
        if ( null == dateWritten )
        {
            dateWritten = new Date();
        }
        if ( null == lastAccessed )
        {
            lastAccessed = new Date();
        }
        
        final S3Object o = m_databaseSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                Require.exists(
                        Blob.class, 
                        Blob.OBJECT_ID, 
                        Require.beanPropertyEquals( Identifiable.ID, blobId ) ) );
        final BlobPool retval = BeanFactory.newBean( BlobPool.class )
                .setBlobId( blobId ).setPoolId( poolId ).setBucketId( o.getBucketId() )
                .setDateWritten( dateWritten ).setLastAccessed( lastAccessed );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public BlobPool putBlobOnPoolForStorageDomain( UUID poolId, final UUID blobId, final UUID storageDomainId )
    {
        final BlobPool blobPool = putBlobOnPool( poolId, blobId );
        final Pool pool = attain( Pool.class, poolId ).setAssignedToStorageDomain( true );
        
        final UUID storageDomainMemberId = m_databaseSupport.getServiceManager()
                                                            .getService( StorageDomainService.class )
                                                            .selectAppropriateStorageDomainMember( pool,
                                                                    storageDomainId );
        
        if ( null != pool.getStorageDomainMemberId() && !pool.getStorageDomainMemberId()
                                                             .equals( storageDomainMemberId ) )
        {
            throw new IllegalStateException(
                    "Cannot assign tape to storage domain member id " + storageDomainMemberId + ", it " +
                            "is already assigned to " + pool.getStorageDomainMemberId() + "." );
        }
        
        pool.setStorageDomainMemberId( storageDomainMemberId );
        updateBean( pool, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        return blobPool;
    }
    
    public BlobDs3Target putBlobOnDs3Target( UUID targetId, final UUID blobId )
    {
        final BlobDs3Target retval = BeanFactory.newBean( BlobDs3Target.class );
        retval.setTargetId( targetId ).setBlobId( blobId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public BlobAzureTarget putBlobOnAzureTarget( UUID targetId, final UUID blobId )
    {
        final BlobAzureTarget retval = BeanFactory.newBean( BlobAzureTarget.class );
        retval.setTargetId( targetId ).setBlobId( blobId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public BlobS3Target putBlobOnS3Target( UUID targetId, final UUID blobId )
    {
        final BlobS3Target retval = BeanFactory.newBean( BlobS3Target.class );
        retval.setTargetId( targetId ).setBlobId( blobId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public SuspectBlobTape makeSuspect( final BlobTape blobTape )
    {
        final SuspectBlobTape retval = BeanFactory.newBean( SuspectBlobTape.class );
        BeanCopier.copy( retval, blobTape );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public SuspectBlobPool makeSuspect( final BlobPool blobPool )
    {
        final SuspectBlobPool retval = BeanFactory.newBean( SuspectBlobPool.class );
        BeanCopier.copy( retval, blobPool );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public SuspectBlobDs3Target makeSuspect( final BlobDs3Target blobTarget )
    {
        final SuspectBlobDs3Target retval = BeanFactory.newBean( SuspectBlobDs3Target.class );
        BeanCopier.copy( retval, blobTarget );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public SuspectBlobAzureTarget makeSuspect( final BlobAzureTarget blobTarget )
    {
        final SuspectBlobAzureTarget retval = BeanFactory.newBean( SuspectBlobAzureTarget.class );
        BeanCopier.copy( retval, blobTarget );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public SuspectBlobS3Target makeSuspect( final BlobS3Target blobTarget )
    {
        final SuspectBlobS3Target retval = BeanFactory.newBean( SuspectBlobS3Target.class );
        BeanCopier.copy( retval, blobTarget );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }


    public ObsoleteBlobTape makeObsolete( final BlobTape blobTape, final UUID obsoletionID )
    {
        final ObsoleteBlobTape retval = BeanFactory.newBean( ObsoleteBlobTape.class );
        BeanCopier.copy( retval, blobTape );
        retval.setObsoletionId( obsoletionID );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }


    public ObsoleteBlobPool makeObsolete( final BlobPool blobPool, final UUID obsoletionID )
    {
        final ObsoleteBlobPool retval = BeanFactory.newBean( ObsoleteBlobPool.class );
        BeanCopier.copy( retval, blobPool );
        retval.setObsoletionId( obsoletionID );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }


    public Pool createPool()
    {
        return createPool( null );
    }
    
    
    public Pool createPool( final PoolState state )
    {
        return createPool( null, state );
    }
    
    
    public Pool createPool( final UUID poolPartitionId, final PoolState state )
    {
        return createPool( null, null, poolPartitionId, state, true );
    }
    
    
    public Pool createPool( 
            PoolType type,
            String mountPoint,
            final UUID poolPartitionId, 
            final PoolState state,
            final boolean poweredOn )
    {
        final int index = m_databaseSupport.getDataManager().getCount(
                Pool.class,
                Require.nothing() );
        if ( null == type )
        {
            type = PoolType.NEARLINE;
        }
        if ( null == mountPoint )
        {
            mountPoint = "/mountpoint-" + index;
        }
        final Pool retval = BeanFactory.newBean( Pool.class )
                .setPartitionId( poolPartitionId )
                .setGuid( UUID.randomUUID().toString() )
                .setType( type )
                .setAvailableCapacity( 10000 )
                .setUsedCapacity( 20000 )
                .setReservedCapacity( 0 )
                .setHealth( PoolHealth.OK )
                .setMountpoint( mountPoint )
                .setName( "pool" + index )
                .setPoweredOn( poweredOn );
        if ( state == null )
        {
            retval.setState(  PoolState.NORMAL );
        }
        else
        {
            retval.setState( state );
        }
        m_databaseSupport.getDataManager().createBean( retval );
        return m_databaseSupport.getServiceManager().getService(PoolService.class ).retrieveAll(
                Require.beanPropertyEquals( Identifiable.ID, retval.getId() ) ).getFirst();
    }
    
    
    public ImportTapeDirective createImportTapeDirective( UUID tapeId, UUID userId, UUID dataPolicyId )
    {
        if ( null == tapeId )
        {
            tapeId = createTape( TapeState.NORMAL ).getId();
        }
        if ( null == userId )
        {
            userId = ensureUserExists( null );
        }
        if ( null == dataPolicyId )
        {
            dataPolicyId = ensureDataPolicyExists( null );
        }
        
        final ImportTapeDirective retval = BeanFactory.newBean( ImportTapeDirective.class )
                .setTapeId( tapeId ).setUserId( userId ).setDataPolicyId( dataPolicyId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }


    public RawImportTapeDirective createRawImportTapeDirective( UUID tapeId, UUID bucketId )
    {
        if ( null == tapeId )
        {
            tapeId = createTape( TapeState.NORMAL ).getId();
        }
        if ( null == bucketId )
        {
            bucketId = ensureBucketExists( null );
        }
        
        final RawImportTapeDirective retval = BeanFactory.newBean( RawImportTapeDirective.class )
                .setTapeId( tapeId ).setBucketId( bucketId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public ImportPoolDirective createImportPoolDirective( UUID poolId, UUID userId, UUID dataPolicyId )
    {
        if ( null == poolId )
        {
            poolId = createPool().getId();
        }
        if ( null == userId )
        {
            userId = createUser( DEFAULT_USER_NAME ).getId();
        }
        if ( null == dataPolicyId )
        {
            userId = createDataPolicy( DEFAULT_DATA_POLICY_NAME ).getId();
        }
        
        final ImportPoolDirective retval = BeanFactory.newBean( ImportPoolDirective.class )
                .setPoolId( poolId ).setUserId( userId ).setDataPolicyId( dataPolicyId );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    

    public ImportAzureTargetDirective createImportAzureTargetDirective(
            final UUID targetId,
            UUID userId,
            UUID dataPolicyId,
            final String cloudBucketName )
    {
        if ( null == userId )
        {
            userId = createUser( DEFAULT_USER_NAME ).getId();
        }
        if ( null == dataPolicyId )
        {
            userId = createDataPolicy( DEFAULT_DATA_POLICY_NAME ).getId();
        }
        
        final ImportAzureTargetDirective retval = BeanFactory.newBean( ImportAzureTargetDirective.class )
                .setTargetId( targetId ).setUserId( userId ).setDataPolicyId( dataPolicyId )
                .setCloudBucketName( cloudBucketName );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public ImportS3TargetDirective createImportS3TargetDirective(
            final UUID targetId,
            UUID userId,
            UUID dataPolicyId,
            final String cloudBucketName )
    {
        if ( null == userId )
        {
            userId = createUser( DEFAULT_USER_NAME ).getId();
        }
        if ( null == dataPolicyId )
        {
            userId = createDataPolicy( DEFAULT_DATA_POLICY_NAME ).getId();
        }
        
        final ImportS3TargetDirective retval = BeanFactory.newBean( ImportS3TargetDirective.class )
                .setTargetId( targetId ).setUserId( userId ).setDataPolicyId( dataPolicyId )
                .setCloudBucketName( cloudBucketName );
        m_databaseSupport.getDataManager().createBean( retval );
        return retval;
    }
    
    
    public < T extends DatabasePersistable > T updateBean( final T bean, final String ... propertiesToUpdate )
    {
        m_databaseSupport.getDataManager().updateBean( CollectionFactory.toSet( propertiesToUpdate ), bean );
        return bean;
    }
    
    
    public < T extends DatabasePersistable > void updateAllBeans(
            final T bean,
            final String ... propertiesToUpdate )
    {
        m_databaseSupport.getDataManager().updateBeans(
                CollectionFactory.toSet( propertiesToUpdate ), 
                bean,
                Require.nothing() );
    }
    
    
    public Tape createTapeBlobsFixture( final String objectName )
    {
        final StorageDomain sd1 = createStorageDomain( "sd" + UUID.randomUUID() );
        final Tape tape = createTape();
        final StorageDomainMember sdm1 =
                addTapePartitionToStorageDomain( sd1.getId(), tape.getPartitionId(), tape.getType() );
        final UUID tapeId = tape.getId();
        final S3Object object = createObject( null, objectName, -1 );
        final UUID objectId = object.getId();
        final UUID blobId1 = createBlobs( objectId, 1, 0L, 5024L )
                .iterator().next().getId();
        final UUID blobId2 = createBlobs( objectId, 1, 5024L, 5024L )
                .iterator().next().getId();
        final DataManager dataManager = m_databaseSupport.getDataManager();
        dataManager.createBean( BeanFactory.newBean( BlobTape.class )
                .setBlobId( blobId1 )
                .setTapeId( tapeId )
                .setOrderIndex( 12 ) );
        dataManager.createBean( BeanFactory.newBean( BlobTape.class )
                .setBlobId( blobId2 )
                .setTapeId( tapeId )
                .setOrderIndex( 1234 ) );
        m_databaseSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setBucketId( object.getBucketId() ).setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        return tape;
    }

    public DataPolicy createABMConfigDualCopyOnPool() {
        final UUID ppId = ensurePoolPartitionExists( null );
        final DataPolicy dp = createDataPolicy( DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain sd2 = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME + "2");
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId());
        addPoolPartitionToStorageDomain(sd.getId(), ppId);
        addPoolPartitionToStorageDomain(sd2.getId(), ppId);
        //all all pools to the pool partition:
        for ( final Pool p : m_databaseSupport.getServiceManager().getRetriever( Pool.class ).retrieveAll().toSet() )
        {
            addPoolToPartition( p.getId(), ppId );
        }
        return dp;
    }


    public DataPolicy createABMConfigSingleCopyOnTape()
    {
        final UUID tpId = ensurePartitionExists( null );
        final DataPolicy dp = createDataPolicy( DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());
        for ( TapeType t : TapeType.values() )
        {
            //add storage domain members for all supported tape types
            if ( t.canContainData() )
            {
                addTapePartitionToStorageDomain(sd.getId(), tpId, t );
            }
        }
        return dp;
    }


    public DataPolicy createABMConfigSingleCopyOnPool()
    {
        final UUID ppId = ensurePoolPartitionExists( null );
        final DataPolicy dp = createDataPolicy( DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());
        addPoolPartitionToStorageDomain(sd.getId(), ppId);
        //all all pools to the pool partition:
        for ( final Pool p : m_databaseSupport.getServiceManager().getRetriever( Pool.class ).retrieveAll().toSet() )
        {
            addPoolToPartition( p.getId(), ppId );
        }
        return dp;
    }

    public void addPoolToPartition(UUID poolId, UUID partitionId) {
        updateBean( attain( Pool.class, poolId ).setPartitionId( partitionId ), Pool.PARTITION_ID );
    }


    public DataPolicy createDataPolicySingleCopyOnPool(final String dataPolicyName, final String storageDomainName)
    {
        final UUID ppId = ensurePoolPartitionExists( null );
        final DataPolicy dp = createDataPolicy( dataPolicyName );
        final StorageDomain sd = createStorageDomain( storageDomainName );
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());
        addPoolPartitionToStorageDomain(sd.getId(), ppId);
        return dp;
    }
    
    
    public DataPolicy createABMConfigDualCopyOnTape()
    {
        final UUID tpId = ensurePartitionExists( null );
        final DataPolicy dp = createDataPolicy( DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain sd2 = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME + "2");
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId());
        for ( TapeType t : TapeType.values() )
        {
            //add storage domain members for all supported tape types
            if ( t.canContainData() )
            {
                addTapePartitionToStorageDomain(sd.getId(), tpId, t );
                addTapePartitionToStorageDomain(sd2.getId(), tpId, t );
            }
        }
        return dp;
    }

    public DataPolicy createABMConfigTapeAndCloud(S3Target s3Target)
    {
        final UUID tpId = ensurePartitionExists( null );
        final DataPolicy dp = createDataPolicy( DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId());
        for ( TapeType t : TapeType.values() )
        {
            //add storage domain members for all supported tape types
            if ( t.canContainData() )
            {
                addTapePartitionToStorageDomain(sd.getId(), tpId, t );
            }
        }

        createS3DataReplicationRule( dp.getId(), DataReplicationRuleType.PERMANENT, s3Target.getId());
        return dp;
    }


    public void quiesceAllTapePartitions() {
        updateAllBeans( BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
    }


    public void unquiesceAllTapePartitions() {
        updateAllBeans( BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
    }


    public DataPolicy createABMConfigTemporaryOnly()
    {
        final UUID tpId = ensurePartitionExists( null );
        final DataPolicy dp = createDataPolicy( DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.TEMPORARY, sd.getId());

        return dp;
    }

    public DataPolicy createABMConfigOneCopyPoolOneCopyTape() {
        final UUID ppId = ensurePoolPartitionExists( null );
        final UUID tpId = ensurePartitionExists( null );
        final DataPolicy dp = createDataPolicy( DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sdPool = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME + "_pool");
        final StorageDomain sdTape = createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME + "_tape");
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sdPool.getId());
        createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sdTape.getId());
        addPoolPartitionToStorageDomain(sdPool.getId(), ppId);
        //all all pools to the pool partition:
        for ( final Pool p : m_databaseSupport.getServiceManager().getRetriever( Pool.class ).retrieveAll().toSet() )
        {
            addPoolToPartition( p.getId(), ppId );
        }
        // Add tape partitions for all supported tape types
        for ( TapeType t : TapeType.values() )
        {
            //add storage domain members for all supported tape types
            if ( t.canContainData() )
            {
                addTapePartitionToStorageDomain(sdTape.getId(), tpId, t );
            }
        }
        return dp;
    }


    public void createCacheFilesystem()
    {
        final CacheFilesystem retval = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( "temp" ).setMaxCapacityInBytes( Long.valueOf( 22 ) )
                .setNodeId( m_databaseSupport.getServiceManager().getService( NodeService.class )
                        .getThisNode().getId() )
                .setCacheSafetyEnabled( false );
        m_databaseSupport.getDataManager().createBean( retval );
    }


    public <T extends Failure<?,?>> T waitForFailure(final Class<T> failureType) {
        return waitForFailure(failureType, 10 * 1000L);
    }


    public <T extends Failure<?,?>> T waitForFailure(final Class<T> failureType, final long max) {
        List<T> failures = m_databaseSupport.getServiceManager().getRetriever(failureType).retrieveAll().toList();
        int millisWaited = 0;
        while (failures.isEmpty() && millisWaited < max) {
            TestUtil.sleep(100);
            millisWaited += 100;
            failures = m_databaseSupport.getServiceManager().getRetriever(failureType).retrieveAll().toList();
        }
        if (failures.isEmpty()) {
            throw new RuntimeException("Wait expired");
        }
        return failures.get(0);
    }

    public BeansServiceManager getServiceManager() {
        return m_databaseSupport.getServiceManager();
    }
    

    private final AtomicInteger m_nextChunkNumber = new AtomicInteger( 1 );
    private final DatabaseSupport m_databaseSupport;
    
    public static final String DEFAULT_USER_NAME = "default_user_name";
    public static final String DEFAULT_DATA_POLICY_NAME = "default_data_policy_name";
    public static final String DEFAULT_STORAGE_DOMAIN_NAME = "default_storage_domain_name";
    public static final String DEFAULT_BUCKET_NAME = "default-bucket-name";
    public static final String DEFAULT_OBJECT_NAME = "default_object_name";
    public static final String DEFAULT_LIBRARY_SN = "default_library_sn";
    public static final String DEFAULT_PARTITION_SN = "default_partition_sn";
    public static final String DEFAULT_POOL_PARTITION_NAME = "default_pool_partition_name";

    private final static Object LOCK = new Object();


}
