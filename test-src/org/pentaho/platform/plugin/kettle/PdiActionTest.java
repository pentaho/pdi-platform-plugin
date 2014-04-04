/*!
* This program is free software; you can redistribute it and/or modify it under the
* terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
* Foundation.
*
* You should have received a copy of the GNU Lesser General Public License along with this
* program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
* or from the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
* without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU Lesser General Public License for more details.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package org.pentaho.platform.plugin.kettle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.pentaho.commons.connection.IPentahoResultSet;
import org.pentaho.commons.connection.memory.MemoryMetaData;
import org.pentaho.commons.connection.memory.MemoryResultSet;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleMissingPluginsException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.platform.api.engine.ActionExecutionException;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.IPentahoDefinableObjectFactory.Scope;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.engine.ISolutionEngine;
import org.pentaho.platform.api.engine.ISystemSettings;
import org.pentaho.platform.api.engine.IUserRoleListService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.scheduler2.IScheduler;
import org.pentaho.platform.api.scheduler2.Job;
import org.pentaho.platform.api.scheduler2.JobTrigger;
import org.pentaho.platform.api.scheduler2.SchedulerException;
import org.pentaho.platform.engine.core.system.PathBasedSystemSettings;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.platform.engine.core.system.boot.PlatformInitializationException;
import org.pentaho.platform.engine.security.SecurityHelper;
import org.pentaho.platform.engine.services.solution.SolutionEngine;
import org.pentaho.platform.plugin.boot.PentahoBoot;
import org.pentaho.platform.plugin.kettle.security.policy.rolebased.actions.RepositoryExecuteAction;
import org.pentaho.platform.repository2.unified.DefaultUnifiedRepository;
import org.pentaho.platform.repository2.unified.fs.FileSystemBackedUnifiedRepository;
import org.pentaho.platform.scheduler2.quartz.QuartzScheduler;
import org.pentaho.test.platform.engine.core.MicroPlatform;
import org.springframework.security.userdetails.UserDetailsService;
import org.springframework.util.Assert;

@SuppressWarnings( { "all" })
public class PdiActionTest {

  private QuartzScheduler scheduler;

  public static final String SESSION_PRINCIPAL = "SECURITY_PRINCIPAL";

  private String TEST_USER = "TestUser";

  private HashMap<String, Serializable> jobParams;

  @Before
  public void init() throws SchedulerException, PlatformInitializationException {
    System.setProperty("java.naming.factory.initial", "org.osjava.sj.SimpleContextFactory"); //$NON-NLS-1$ //$NON-NLS-2$
    System.setProperty("org.osjava.sj.root", "test-src/simple-jndi"); //$NON-NLS-1$ //$NON-NLS-2$
    System.setProperty("org.osjava.sj.delimiter", "/"); //$NON-NLS-1$ //$NON-NLS-2$

    System.setProperty("PENTAHO_SYS_CFG_PATH", new File( "test-src/solution/pentaho.xml" ).getAbsolutePath() ); //$NON-NLS-1$ //$NON-NLS-2$
    
    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession(session);
    
    scheduler = new QuartzScheduler();
    scheduler.start();

    MicroPlatform mp = new MicroPlatform("test-src/solution");
    mp.define(IUserRoleListService.class, StubUserRoleListService.class);
    mp.define(UserDetailsService.class, StubUserDetailService.class);
    mp.defineInstance(IAuthorizationPolicy.class, new TestAuthorizationPolicy());
    mp.setSettingsProvider( new PathBasedSystemSettings() );
    mp.defineInstance(IScheduler.class, scheduler );
    
    mp.define( ISolutionEngine.class, SolutionEngine.class );    
    mp.define( IUnifiedRepository.class, FileSystemBackedUnifiedRepository.class, Scope.GLOBAL );
    FileSystemBackedUnifiedRepository repo = (FileSystemBackedUnifiedRepository) PentahoSystem.get( IUnifiedRepository.class );
    File root = new File( "test-src/solution" );
    repo.setRootDir( root );

    mp.start();

    SecurityHelper.getInstance().becomeUser(TEST_USER);
    jobParams = new HashMap<String, Serializable>();
  }
  
  @Test(expected=Exception.class)
  public void testValidatation() throws Exception {
    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.execute();
  }
  
  @Test
  public void testTransformationVariableOverrides() throws Exception {
    Map<String, String> variables = new HashMap<String, String>();
    variables.put("customVariable", "customVariableValue");

    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments(new String[] { "dummyArg" } );
    action.setVariables(variables);
    
    
    Map<String, String> overrideParams = new HashMap<String, String>();
    overrideParams.put("param2", "12");
    action.setParameters(overrideParams);
    action.setDirectory("test-src/solution");
    action.setTransformation("pdi/testTransformationVariableOverrides");

    action.execute();
    
    List lines = FileUtils.readLines(new File("testTransformationVariableOverrides.out.txt"));
    assertTrue("File \"testTransformationVariableOverrides.out.txt\" should not be empty", lines.size() > 0);
    String rowData = (String)lines.get(1);
    //Columns are as follows:
    //generatedRow|cmdLineArg1|param1|param2|repositoryDirectory|customVariable
    String[] columnData = rowData.split("\\|");
    assertEquals("param1 value is wrong (default value should be in effect)", "param1DefaultValue", columnData[2].trim());
    assertEquals("param2 value is wrong (overridden value should be in effect)", 12L, Long.parseLong(columnData[3].trim()));
    
    assertEquals("The number of rows generated should have equaled the value of param2", 13, lines.size());
    
    assertEquals("customVariable value is wrong", "customVariableValue", columnData[5].trim());
  }
  
  @Test
  public void testLoadingFromVariousPaths() throws Exception {
    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments(new String[] { "dummyArg" } );
    
    action.setDirectory("test-src/solution");
    action.setTransformation("pdi/testTransformationVariableOverrides");

    action.execute();
  }
  
  @Test(expected=ActionExecutionException.class)
  public void testBadFileThrowsException() throws Exception {
    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments(new String[] { "dummyArg" } );
    
    action.setDirectory("/dne");
    action.setTransformation("dne.ktr");

    action.execute();
  }
  
  @Test
  public void testTransformationResource() throws Exception {
    Map<String, Object> args = new HashMap<String, Object>();
    args.put("parameterKey", "parameterValue");

    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setVarArgs(args);
    
    File ktr = new File("test-src/solution/pdi/sample2.ktr");
    assertTrue(ktr.exists());

    action.setDirectory("test-src/solution");
    action.setTransformation("pdi/sample2");

    action.execute();

    String status = action.getStatus();
    assertNotNull(status);
    // assertEquals( Trans.STRING_FINISHED, status );

    int result = action.getResult();
    assertEquals(0, result);

    assertEquals("", 0, action.getTransformationOutputRowsCount());
    assertEquals("", 0, action.getTransformationOutputErrorRowsCount());

    IPentahoResultSet rows = action.getTransformationOutputRows();
    assertNull(rows);
    rows = action.getTransformationOutputErrorRows();
    assertNull(rows);

    String log = action.getLog();
    assertTrue(log.indexOf("QUADRANT_ACTUALS") != -1);
    assertTrue(log.indexOf("R=148") != -1);
    assertTrue(log.indexOf("Filter rows") != -1);
    assertTrue(log.indexOf("R=148") != -1);
    assertTrue(log.indexOf("Java Script Value") != -1);
    assertTrue(log.indexOf("W=5") != -1);
    assertTrue(log.indexOf("XML Output") != -1);
    assertTrue(log.indexOf("O=5") != -1);
    assertNotNull(log);

  }

  @Test
  public void testTransformationPaths() {
    String dir = "test-src/solution";
    String ktr = "pdi/sample2";

    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setDirectory(dir);
    action.setTransformation(ktr);

    try {
      action.execute();
    } catch (Exception ex) {
      ex.printStackTrace();
      fail(ex.getMessage());
    }

    String status = action.getStatus();
    assertNotNull(status);
    // assertEquals( Trans.STRING_FINISHED, status );

    int result = action.getResult();
    assertEquals(0, result);

    assertEquals("", 0, action.getTransformationOutputRowsCount());
    assertEquals("", 0, action.getTransformationOutputErrorRowsCount());

    IPentahoResultSet rows = action.getTransformationOutputRows();
    assertNull(rows);
    rows = action.getTransformationOutputErrorRows();
    assertNull(rows);

    String log = action.getLog();
    assertTrue(log.indexOf("QUADRANT_ACTUALS") != -1);
    assertTrue(log.indexOf("R=148") != -1);
    assertTrue(log.indexOf("Filter rows") != -1);
    assertTrue(log.indexOf("R=148") != -1);
    assertTrue(log.indexOf("Java Script Value") != -1);
    assertTrue(log.indexOf("W=5") != -1);
    assertTrue(log.indexOf("XML Output") != -1);
    assertTrue(log.indexOf("O=5") != -1);
    assertNotNull(log);
  }

  @Test
  public void testJobPaths() throws Exception {
    
    File kjb = new File( "test-src/solution/pdi/ETLJob1.kjb" );
    assertTrue( kjb.exists() );

    PdiAction component = new PdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setDirectory( "test-src/solution" );
    component.setJob( "pdi/ETLJob1" );

    component.execute();
    //if no exception then the test passes
  }

  @Test
  public void testTransformationMonitor() {
    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    
    File ktr = new File("test-src/solution/pdi/sample2.ktr");
    assertTrue(ktr.exists());
    
    action.setDirectory( "test-src/solution" );
    action.setTransformation( "pdi/sample2" );

    action.setMonitorStep("XML Output");

    try {
      action.execute();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    String status = action.getStatus();
    assertNotNull(status);
    // assertEquals( "Finished", status );

    assertEquals(5, action.getTransformationOutputRowsCount());
    assertEquals(0, action.getTransformationOutputErrorRowsCount());

    IPentahoResultSet rows = action.getTransformationOutputRows();
    assertNotNull(rows);
    assertEquals(5, rows.getRowCount());

    assertEquals("Central", rows.getValueAt(0, 0));
    assertEquals("Sales", rows.getValueAt(0, 1));
    assertEquals("Account Executive", rows.getValueAt(0, 2));
    assertEquals("Hello, Account Executive", rows.getValueAt(0, 3));

    rows = action.getTransformationOutputErrorRows();
    assertNotNull(rows);

    String log = action.getLog();
    assertTrue(log.indexOf("QUADRANT_ACTUALS") != -1);
    assertTrue(log.indexOf("R=148") != -1);
    assertTrue(log.indexOf("Filter rows") != -1);
    assertTrue(log.indexOf("R=148") != -1);
    assertTrue(log.indexOf("Java Script Value") != -1);
    assertTrue(log.indexOf("W=5") != -1);
    assertTrue(log.indexOf("XML Output") != -1);
    assertTrue(log.indexOf("O=5") != -1);
    assertNotNull(log);
  }

  @Test
  public void testTransformationInjector() throws Exception {
    PdiAction component = new PdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession(session);
    
    File ktr = new File("test-src/solution/pdi/sample3.ktr");
    assertTrue(ktr.exists());

    component.setDirectory( "test-src/solution" );
    component.setTransformation( "pdi/sample3" );

    String[][] columnNames = { { "REGION", "DEPARTMENT", "POSITIONTITLE" } };
    MemoryMetaData metadata = new MemoryMetaData(columnNames, null);

    MemoryResultSet rowsIn = new MemoryResultSet(metadata);
    rowsIn.addRow(new Object[] { "abc", "123", "bogus" });
    rowsIn.addRow(new Object[] { "region2", "Sales", "bad" });
    rowsIn.addRow(new Object[] { "Central", "Sales", "test title" });
    rowsIn.addRow(new Object[] { "Central", "xyz", "bad" });

    component.setInjectorRows(rowsIn);
    component.setInjectorStep("Injector");
    component.setMonitorStep("Output");

    component.execute();

    String status = component.getStatus();
    assertNotNull(status);
    // assertEquals( "Finished", status );

    assertEquals(1, component.getTransformationOutputRowsCount());
    assertEquals(0, component.getTransformationOutputErrorRowsCount());

    IPentahoResultSet rows = component.getTransformationOutputRows();
    assertNotNull(rows);
    assertEquals(1, rows.getRowCount());

    assertEquals("Central", rows.getValueAt(0, 0));
    assertEquals("Sales", rows.getValueAt(0, 1));
    assertEquals("test title", rows.getValueAt(0, 2));
    assertEquals("Hello, test title", rows.getValueAt(0, 3));

    rows = component.getTransformationOutputErrorRows();
    assertNotNull(rows);

    String log = component.getLog();
    assertTrue(log.indexOf("Injector") != -1);
    assertTrue(log.indexOf("R=1") != -1);
    assertTrue(log.indexOf("Filter rows") != -1);
    assertTrue(log.indexOf("W=1") != -1);
    assertTrue(log.indexOf("Java Script Value") != -1);
    assertTrue(log.indexOf("W=1") != -1);
    assertTrue(log.indexOf("Output") != -1);
    assertTrue(log.indexOf("W=4") != -1);
    assertNotNull(log);
  }
  
  @Test
  public void testKettleTransformationSchedule() throws SchedulerException, InterruptedException {
    jobParams.put("directory", "test-src/solution/pdi");
    jobParams.put("transformation", "sample2.ktr");
    scheduler.createJob("testName", PdiAction.class, jobParams, JobTrigger.ONCE_NOW);
    sleep(5);
  }


  @Test
  public void testKettleTransformationScheduleWithNoExecutePermision() throws SchedulerException, InterruptedException, PlatformInitializationException {
    System.setProperty("java.naming.factory.initial", "org.osjava.sj.SimpleContextFactory"); //$NON-NLS-1$ //$NON-NLS-2$
    System.setProperty("org.osjava.sj.root", "test-src/simple-jndi"); //$NON-NLS-1$ //$NON-NLS-2$
    System.setProperty("org.osjava.sj.delimiter", "/"); //$NON-NLS-1$ //$NON-NLS-2$

    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession(session);
    
    scheduler = new QuartzScheduler();
    scheduler.start();

    MicroPlatform mp = new MicroPlatform("test-src/solution");
    mp.define(IUserRoleListService.class, StubUserRoleListService.class);
    mp.define(UserDetailsService.class, StubUserDetailService.class);
    mp.defineInstance(IAuthorizationPolicy.class, new TestAuthorizationPolicyNoExecute());
    mp.defineInstance(IScheduler.class, scheduler );
    
    mp.define( ISolutionEngine.class, SolutionEngine.class );    
    mp.define( IUnifiedRepository.class, FileSystemBackedUnifiedRepository.class, Scope.GLOBAL );
    FileSystemBackedUnifiedRepository repo = (FileSystemBackedUnifiedRepository) PentahoSystem.get( IUnifiedRepository.class );
    repo.setRootDir( new File( "test-src/solution" ) );
    
    mp.start();

    SecurityHelper.getInstance().becomeUser(TEST_USER);
    jobParams = new HashMap<String, Serializable>();
    
    jobParams.put("directory", "/pdi");
    jobParams.put("transformation", "sample2.ktr");
    
    try {
      Job job = scheduler.createJob("testNameNoExecute", PdiAction.class, jobParams, JobTrigger.ONCE_NOW);
      Assert.notNull( job );
      scheduler.triggerNow(job.getJobId());
    } catch (Exception e) {
      assertNotNull(e);
    }
    sleep(5);
  }

  @Test
  public void testNoSettings() {
    PdiAction component = new PdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    try {
      component.validate();
      fail();
    } catch (Exception ex) {
    }
  }

  @Test
  public void testBadSolutionTransformation() {
    PdiAction component = new PdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setTransformation("bogus.ktr");
    try {
      component.validate();
      fail();
    } catch (Exception ex) {
    }
  }

  @Test
  public void testBadSolutionJob() {
    PdiAction component = new PdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setJob("bogus.kjb");
    try {
      component.validate();
      fail();
    } catch (Exception ex) {
    }
  }
  
  @Test
  public void testJobParameterPassing() throws Exception {
    
	// this job will pass these parameters to its underlying transformation
	// the transformation will simply add 'firstName' and 'lastName' into a 'fullName'
	  
	final int RESULT_OK = 0;
	final String STATUS_FINISHED = "Finished";
	  
    File kjb = new File( "test-src/solution/pdi/parameter-passing/kjbparams.kjb" );
    assertTrue( kjb.exists() );

    PdiAction component = new PdiAction();
    component.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    component.setDirectory( "test-src/solution" );
    component.setJob( "pdi/parameter-passing/kjbparams" );
    
    Map<String, String> params = new HashMap<String, String>();
    params.put("firstName" , "John");
    params.put("lastName" , "Doe");
    
    component.setParameters(params);

    component.execute();
    
    // 1) check if job execution is successful
    assertEquals( RESULT_OK, component.getResult() );
    assertEquals( STATUS_FINISHED, component.getStatus() );
    
    // 2) log scraping: check what the ktr's calculation was for ${first} + ${last} = ${fullName}
    String logScraping = component.getLog().substring( 
    		component.getLog().indexOf("${first} + ${last} = ${fullName}") , 
    		component.getLog().indexOf("====================") );
    
    if ( logScraping != null && logScraping.contains( "fullName =" ) ){
    	logScraping = logScraping.substring( logScraping.indexOf( "fullName =" ) );
    	logScraping = logScraping.substring( 0, logScraping.indexOf( "\n") );
    	String fullName = logScraping.replace( "fullName =", "").trim();
    	
    	assertEquals(fullName.trim(), "JohnDoe");
    }    
    
    // 3) if no exception then the test passes
  }

  private void sleep(int seconds) {
    try {
      Thread.sleep(seconds * 1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }  
  
  public class TestAuthorizationPolicy implements IAuthorizationPolicy {

    List<String> allowedActions = new ArrayList<String>();
    @Override
    public List<String> getAllowedActions(String arg0) {
      // TODO Auto-generated method stub
      allowedActions.add("org.pentaho.repository.read");
      allowedActions.add("org.pentaho.repository.create");
      return allowedActions;
    }

    @Override
    public boolean isAllowed(String arg0) {
      // TODO Auto-generated method stub
      return true;
    }
    
  }
  
  public class TestAuthorizationPolicyNoExecute implements IAuthorizationPolicy {

    List<String> allowedActions = new ArrayList<String>();
    @Override
    public List<String> getAllowedActions(String arg0) {
      // TODO Auto-generated method stub
      allowedActions.add("org.pentaho.repository.read");
      allowedActions.add("org.pentaho.repository.create");
      return allowedActions;
    }

    @Override
    public boolean isAllowed(String action) {
      if(action != null && action.equals(RepositoryExecuteAction.NAME)) {
        return false;
      }
      return true;
    }
    
  }
  
  @Test
  public void testKtrIsValid(){
	  
	TransMeta meta = null;
	
	try {
		meta = new TransMeta( "test-src/solution/pdi/testTransformationVariableOverrides.ktr" );
		
	} catch (KettleException ke) {
		fail( ke.getMessage() );
	}
	
	List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>();
	meta.checkSteps( remarks, false, null );
	        
	assertTrue( remarks != null && isValidationOK( remarks ) ); 
  }
  
  @Test
  public void testKjbIsValid(){
	  
	JobMeta meta = null;
	
	try {
		meta = new JobMeta( "test-src/solution/pdi/ETLJob1.kjb", null );
		
	} catch (KettleException ke) {
		fail( ke.getMessage() );
	}
	
	List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>();
	meta.checkJobEntries( remarks, false , null );
	
	assertTrue( remarks != null && isValidationOK( remarks ) ); 
  }

  private boolean isValidationOK( List<CheckResultInterface> remarks ){
	  
	  for( CheckResultInterface result : remarks ){
		  if( CheckResultInterface.TYPE_RESULT_ERROR == result.getType() ){
			  return false;
		  }
	  }
	  
	  return true;
  }
}
