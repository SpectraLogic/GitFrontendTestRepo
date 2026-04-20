/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.mockdomain.Principal;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherType;
import com.spectralogic.util.db.mockservice.PrincipalService;
import com.spectralogic.util.db.mockservice.TeacherService;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public class BaseService_Test 
{
    @Test
    public void testRemoveAlreadyExistentBeansAllScenarios()
    {   
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );

        final Teacher teacher1 = buildTeacher1();
        final Teacher teacher2 = buildTeacher2();
        final Teacher teacher3 = buildTeacher3();
        final Teacher teacher4 = buildTeacher4();

        final BeansServiceManager serviceMgr = dbSupport.getServiceManager();
        final TeacherService teacherService = serviceMgr.getService( TeacherService.class );

        final Set< Teacher > t1SetTeacher12 = CollectionFactory.toSet( teacher1, teacher2 );
        final int t1NrRemoved = teacherService.removeExistentPersistedBeansFromSetOfTeachers( 
                t1SetTeacher12 );
        assertEquals(0,  t1NrRemoved, "No saved beans -> nothing to remove -> return 0.");
        assertEquals(2,  t1SetTeacher12.size(), "No saved beans -> nothing to remove -> input Set size 2.");

        final BeansServiceManager transactionServiceMgr = serviceMgr.startTransaction();
        final TeacherService teacherTransactionServiceMgr;
        final List< Teacher > allTeachers;        
        try
        {
            teacherTransactionServiceMgr = transactionServiceMgr.getService( TeacherService.class );
            teacherTransactionServiceMgr.create( CollectionFactory.toSet( teacher1, teacher2) );
            assertNotNull( teacher1.getId(),
                    "Shoulda set the primary key on the first teacher." );
            assertNotNull( teacher2.getId(),
                    "Shoulda set the primary key on the second teacher." );
            allTeachers = teacherTransactionServiceMgr.retrieveAll().toList();
            transactionServiceMgr.commitTransaction();
        }
        finally
        {
            transactionServiceMgr.closeTransaction();
        }
        assertEquals(2,  allTeachers.size(), "Check to make sure the persistence did not barf -- returned exactly 2 teachers.");
        Collections.sort( allTeachers, new BeanComparator<>( Teacher.class, Teacher.NAME ) );
        assertTeacherEquals( teacher1, allTeachers.get( 0 ) );
        assertTeacherEquals( teacher2, allTeachers.get( 1 ) );

        final Set< Teacher > t2SetTeacher123 = CollectionFactory.toSet( teacher1, teacher2, teacher3 );
        final int t2NrRemoved = teacherService.removeExistentPersistedBeansFromSetOfTeachers( 
                t2SetTeacher123 );
        assertEquals(2,  t2NrRemoved, "2 saved beans -> 2 to remove -> return 2.");
        assertEquals(1,  t2SetTeacher123.size(), "2 saved beans -> 2 to remove of 3-> input size is 1.");

        final Set< Teacher > t3SetTeacher34 = CollectionFactory.toSet( teacher3, teacher4 );
        final int t3NrRemoved = teacherService.removeExistentPersistedBeansFromSetOfTeachers( 
                t3SetTeacher34 );
        assertEquals(0,  t3NrRemoved, "2 saved beans -> 2 to remove DON'T MATCH -> return 0.");
        assertEquals(2,  t3SetTeacher34.size(), "2 saved beans -> 2 to remove DON'T MATCH -> size=2.");

        final int t4NrRemoved = teacherService.removeExistentPersistedBeansFromSetOfTeachers( 
                new HashSet<Teacher>());
        assertEquals(0,  t4NrRemoved, "Shoulda removed zero bens since we passed in null.");

        final int initialSetSizeForDbMaxWhereClause = 155;
        final Set< Teacher > t5SetOf3DbChunksWhenRemovingTeachers = new HashSet<>();
        for( int i=0; i < initialSetSizeForDbMaxWhereClause; i++)
        {
            t5SetOf3DbChunksWhenRemovingTeachers.add( buildTeacher( i ) );
        }        
        final int t5NrRemoved = teacherService.removeExistentPersistedBeansFromSetOfTeachers( 
                t5SetOf3DbChunksWhenRemovingTeachers );
        assertEquals(2,  t5NrRemoved, "Shoulda removed teacher 1 and 2.");
        assertEquals(initialSetSizeForDbMaxWhereClause - 2,  t5SetOf3DbChunksWhenRemovingTeachers.size(), "2 saved beans -> should have size -2");

        TestUtil.assertThrows(
                "Should throw IllegalArgument bean cannot be null.",
                IllegalArgumentException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        teacherService.removeExistentPersistedBeansFromSetOfTeachers( null );
                    }
                });        
    }
        
    
    @Test
    public void testAttainWhenObjectDoesNotExistThrowsExpectedFailureTypeForSingleArgumentConstructor()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        final TeacherService teacherService =
                dbSupport.getServiceManager().getService( TeacherService.class );
        TestUtil.assertThrows(
                "Shoulda returned a not found exception.",
                GenericFailure.NOT_FOUND,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        teacherService.attain( UUID.randomUUID() );
                    }
                });
    }
    
    
    @Test
    public void testAttainWhenObjectDoesNotExistThrowsExpectedFailureTypeForDoubleArgumentConstructor()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Principal.class, PrincipalService.class );
        final PrincipalService principalService =
                dbSupport.getServiceManager().getService( PrincipalService.class );
        TestUtil.assertThrows(
                "Shoulda thrown an internal error exception.",
                GenericFailure.INTERNAL_ERROR,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        principalService.attain( UUID.randomUUID() );
                    }
                });
    }
    

    @Test
    public void testCreateRetrieveDeleteRetrieveBehavesAsExpected()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        final TeacherService teacherService =
                dbSupport.getServiceManager().getService( TeacherService.class );
        
        final Teacher teacher = buildTeacher1();
        teacherService.create( teacher );
        assertNotNull(
                teacher.getId(),
                "Shoulda set the primary key on the teacher." );
        
        final Teacher retrievedTeacher = teacherService.retrieve( teacher.getId() );
        assertTeacherEquals( teacher, retrievedTeacher );
        
        teacherService.delete( teacher.getId() );
        
        assertNull(
                teacherService.retrieve( teacher.getId() ),
                "Shoulda returned null since we just deleted the teacher."
                 );
    }
    

    @SuppressWarnings( "unchecked" )
    @Test
    public void testCreateSetThenRetrieveBehavesAsExpected()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        final Teacher teacher1 = buildTeacher1();
        final Teacher teacher2 = buildTeacher2();
        
        final BeansServiceManager serviceMgr = dbSupport.getServiceManager();
        final BeansServiceManager transaction =serviceMgr.startTransaction();
        final TeacherService teacherService = serviceMgr.getService( TeacherService.class );
        
        final TeacherService teacherDbTransactionService;
        final List< Teacher > allTeachers;
        try
        {
            teacherDbTransactionService = transaction.getService( TeacherService.class );            
            teacherDbTransactionService.create( CollectionFactory.toSet( teacher1, teacher2 ) );
            
            assertNotNull(  teacher1.getId(),
                    "Shoulda set the primary key on the first teacher.");
            assertNotNull( teacher2.getId(),
                    "Shoulda set the primary key on the second teacher." );
            
            allTeachers = teacherDbTransactionService.retrieveAll().toList();
            
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        assertEquals(2,  allTeachers.size(), "Shoulda returned exactly two teachers.");
        Collections.sort( allTeachers, new BeanComparator<>( Teacher.class, Teacher.NAME ) );
        assertTeacherEquals( teacher1, allTeachers.get( 0 ) );
        assertTeacherEquals( teacher2, allTeachers.get( 1 ) );
        
        ((BaseService< Teacher>)teacherService).deleteAll();        
        final List< Teacher > allTeachersAfterDeleteAll = teacherService.retrieveAll().toList();
        assertEquals(0,  allTeachersAfterDeleteAll.size(), "Should find nothing in the db after deleteAll.");
    }
    

    @Test
    public void testCreateThenUpdateModifiesOnlySpecifiedColumnsAndRows()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        final Teacher teacher1 = buildTeacher1();
        final Teacher teacher2 = buildTeacher2();
        
        final TeacherService teacherService =
                dbSupport.getServiceManager().getService( TeacherService.class );
        
        teacherService.create( teacher1 );
        assertNotNull( teacher1.getId(),
                "Shoulda set the primary key on the first teacher."  );
        teacherService.create( teacher2 );
        assertNotNull( teacher2.getId(),
                "Shoulda set the primary key on the second teacher."  );
        
        final int teacher1WarningsInitial = teacher1.getWarningsIssued();
        final int teacher1WarningsToChangeButNotUpdateTo = 99;
        teacher1
            .setComments( "Updated comments." )
            .setSsn( "321-65-4987" )
            .setWarningsIssued( teacher1WarningsToChangeButNotUpdateTo);
        teacherService.update( teacher1, Teacher.COMMENTS, Teacher.SSN );
        teacher1.setWarningsIssued( teacher1WarningsInitial );
        
        final List< Teacher > allTeachers = teacherService.retrieveAll().toList();
        assertEquals(2,  allTeachers.size(), "Shoulda returned exactly two teachers.");
        Collections.sort( allTeachers, new BeanComparator<>( Teacher.class, Teacher.NAME ) );

        assertTeacherEquals( teacher1, allTeachers.get( 0 ) );
        assertTeacherEquals( teacher2, allTeachers.get( 1 ) );
        
        assertNotSame(
                teacher1,
                allTeachers.get( 0 ),
                "Shoulda not had the same instance since the db wouldn't know about instances."
                 );
    }
    

    @Test
    public void testUpdateWithNoColumnsNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        final Teacher teacher1 = buildTeacher1();
        final Teacher teacher2 = buildTeacher2();
        
        final TeacherService teacherService =
                dbSupport.getServiceManager().getService( TeacherService.class );
        
        teacherService.create( teacher1 );
        assertNotNull(
                teacher1.getId(),
                "Shoulda set the primary key on the first teacher." );
        teacherService.create( teacher2 );
        assertNotNull( teacher2.getId(),
                "Shoulda set the primary key on the second teacher." );
        
        teacher1
            .setComments( "Updated comments." )
            .setSsn( "321-65-4987" )
            .setWarningsIssued( 2 );
        
        teacherService.update( teacher1 );
        teacherService.update( teacher1, ( String[] ) null );
    }
    
    
    @Test
    public void testVerifyInsideTransactionAllowedIffInsideTransaction()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        TestUtil.assertThrows(
                null,
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        ( (BaseService< ? >)dbSupport.getServiceManager().getService( TeacherService.class ) )
                            .verifyInsideTransaction();
                    }
                });
        
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        ( (BaseService< ? >)transaction.getService( TeacherService.class ) ).verifyInsideTransaction();
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testVerifyNotTransactionAllowedIffNotTransaction()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        ( (BaseService< ? >)dbSupport.getServiceManager().getService( TeacherService.class ) )
            .verifyNotTransaction();
        
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        TestUtil.assertThrows(
                null,
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        ( (BaseService< ? >)transaction.getService( TeacherService.class ) )
                            .verifyNotTransaction();
                    }
                });
        transaction.closeTransaction();
    }


    private static Teacher buildTeacher1()
    {
        return buildTeacher( 1 );
    }


    private static Teacher buildTeacher2()
    {
        return buildTeacher( 2 );
    }

    
    private static Teacher buildTeacher3()
    {
        return buildTeacher( 3 ); 
    }
    
    
    private static Teacher buildTeacher4()
    {
        return buildTeacher( 4 ); 
    }
    
    
    private static Teacher buildTeacher(int appendTeacherNr)
    {
        try
        {
            return BeanFactory.newBean( Teacher.class )
                    .setComments( "Tests too easy" + appendTeacherNr )
                    .setDateOfBirth( new SimpleDateFormat("yyyy/MM/dd").parse( "2014/0" + appendTeacherNr 
                            + "/0" + appendTeacherNr + "\"" ) )
                    .setName( "Dr. Jenkins" + appendTeacherNr )
                    .setPassword( "supersecreteverybodyknows" + appendTeacherNr )
                    .setSsn( "987-65-33" + appendTeacherNr + appendTeacherNr )
                    .setType( TeacherType.TEACHER )
                    .setWarningsIssued( appendTeacherNr )
                    .setYearsOfService( appendTeacherNr );
        }
        catch ( final ParseException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    
    private static void assertTeacherEquals( final Teacher teacher, final Teacher retrievedTeacher )
    {
        assertNotNull( retrievedTeacher,
                "Shoulda found a teacher since we just added it." );
        assertEquals(teacher.getComments(), retrievedTeacher.getComments(), "Shoulda returned the same comments.");
        assertEquals(teacher.getDateOfBirth(), retrievedTeacher.getDateOfBirth(), "Shoulda returned the same date of birth.");
        assertEquals(teacher.getName(), retrievedTeacher.getName(), "Shoulda returned the same name.");
        assertEquals( teacher.getPassword(), retrievedTeacher.getPassword(), "Shoulda returned the same password.");
        assertEquals(teacher.getSsn(), retrievedTeacher.getSsn(), "Shoulda returned the same ssn.");
        assertEquals(teacher.getWarningsIssued(), retrievedTeacher.getWarningsIssued(), "Shoulda returned the same warnings issued.");
        assertEquals(teacher.getYearsOfService(), retrievedTeacher.getYearsOfService(), "Shoulda returned the same years of service.");
        assertEquals(teacher.getType(), retrievedTeacher.getType(), "Shoulda returned the same type.");
    }
}
