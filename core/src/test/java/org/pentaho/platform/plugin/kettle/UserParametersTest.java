/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.platform.plugin.kettle;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.engine.ISolutionEngine;
import org.pentaho.platform.api.engine.IUserRoleListService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.util.IPdiContentProvider;
import org.pentaho.platform.engine.core.system.PathBasedSystemSettings;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.platform.engine.core.system.boot.PlatformInitializationException;
import org.pentaho.platform.engine.security.SecurityHelper;
import org.pentaho.platform.engine.services.solution.SolutionEngine;
import org.pentaho.platform.repository2.unified.fs.FileSystemBackedUnifiedRepository;
import org.pentaho.test.platform.engine.core.MicroPlatform;
import org.springframework.security.core.userdetails.UserDetailsService;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class UserParametersTest {

  public static final String SESSION_PRINCIPAL = "SECURITY_PRINCIPAL";
  // this is the parameter named that should be filtered out, given that it starts with an underscore
  private static final String SAMPLE_PROTECTED_PARAMETER_NAME = "_protected";
  private static final String SOLUTION_REPOSITORY = "target/test-classes/solution";
  private static final String SAMPLE_TRANS = "/org/pentaho/platform/plugin/kettle/UserParametersTest.ktr";
  private static final String SAMPLE_JOB = "/org/pentaho/platform/plugin/kettle/UserParametersTest.kjb";
  private static final String TEST_USER = "TestUser";
  private MicroPlatform mp = new MicroPlatform( SOLUTION_REPOSITORY );

  @Before
  public void init() throws PlatformInitializationException {
    System.setProperty( "java.naming.factory.initial", "org.osjava.sj.SimpleContextFactory" );
    System.setProperty( "org.osjava.sj.root", "test-src/simple-jndi" );
    System.setProperty( "org.osjava.sj.delimiter", "/" );
    System.setProperty( "PENTAHO_SYS_CFG_PATH", new File( SOLUTION_REPOSITORY + "/pentaho.xml" ).getAbsolutePath() );

    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession( session );

    mp.define( IUserRoleListService.class, StubUserRoleListService.class );
    mp.define( UserDetailsService.class, StubUserDetailService.class );
    mp.defineInstance( IAuthorizationPolicy.class, new TestAuthorizationPolicy() );
    mp.setSettingsProvider( new PathBasedSystemSettings() );
    mp.define( ISolutionEngine.class, SolutionEngine.class );
    FileSystemBackedUnifiedRepository repo = new FileSystemBackedUnifiedRepository( SOLUTION_REPOSITORY );
    mp.defineInstance( IUnifiedRepository.class, repo );

    mp.start();

    SecurityHelper.getInstance().becomeUser( TEST_USER );
  }

  @After
  public void tearDown() {
    if ( mp != null ) {
      mp.stop();
    }
  }

  @Test
  public void testTransformationUserParameters() throws Exception {
    PdiAction action = getSpyPdiAction();

    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments( new String[] { "dummyArg" } );
    action.setDirectory( SOLUTION_REPOSITORY );
    action.setTransformation( SAMPLE_TRANS );

    action.execute();

    assertTrue( action.localTrans != null && action.localTrans.getTransMeta() != null );

    boolean protectedParameterNameExistsInKtr = false;

    for ( String param : action.localTrans.getTransMeta().listParameters() ) {
      protectedParameterNameExistsInKtr |= SAMPLE_PROTECTED_PARAMETER_NAME.equals( param );
    }

    // we make sure: this ktr does indeed have a system/hidden parameter
    assertTrue( protectedParameterNameExistsInKtr );

    // reset attribute
    protectedParameterNameExistsInKtr = false;

    // we now call IPdiContentProvider.getUserParameters( kjb ), that should filter out protected parameters
    IPdiContentProvider pdiContentProvider = new PdiContentProvider( PentahoSystem.get( IUnifiedRepository.class ) );
    Map<String, String> userParams =
      pdiContentProvider.getUserParameters( SOLUTION_REPOSITORY + SAMPLE_TRANS );

    for ( String userParam : userParams.keySet() ) {
      protectedParameterNameExistsInKtr |= SAMPLE_PROTECTED_PARAMETER_NAME.equals( userParam );
    }

    // we make sure: IPdiContentProvider has filtered it out
    assertFalse( protectedParameterNameExistsInKtr );
  }

  @Test
  public void testJobUserParameters() throws Exception {
    PdiAction action = getSpyPdiAction();

    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments( new String[] { "dummyArg" } );
    action.setDirectory( SOLUTION_REPOSITORY );
    action.setJob( SAMPLE_JOB );

    action.execute();

    assertTrue( action.localJob != null && action.localJob.getJobMeta() != null );

    boolean protectedParameterNameExistsInKjb = false;

    for ( String param : action.localJob.getJobMeta().listParameters() ) {
      protectedParameterNameExistsInKjb |= SAMPLE_PROTECTED_PARAMETER_NAME.equals( param );
    }

    // we make sure: this kjb does indeed have a system/hidden parameter
    assertTrue( protectedParameterNameExistsInKjb );

    // reset attribute
    protectedParameterNameExistsInKjb = false;

    // we now call IPdiContentProvider.getUserParameters( kjb ), that should filter out protected parameters
    IPdiContentProvider pdiContentProvider = new PdiContentProvider( PentahoSystem.get( IUnifiedRepository.class ) );
    Map<String, String> userParams = pdiContentProvider.getUserParameters( SOLUTION_REPOSITORY + SAMPLE_JOB );

    for ( String userParam : userParams.keySet() ) {
      protectedParameterNameExistsInKjb |= SAMPLE_PROTECTED_PARAMETER_NAME.equals( userParam );
    }

    // we make sure: IPdiContentProvider has filtered it out
    assertFalse( protectedParameterNameExistsInKjb );
  }

  static class TestAuthorizationPolicy implements IAuthorizationPolicy {
    private static final String ACTION_READ = "org.pentaho.repository.read";
    private static final String ACTION_CREATE = "org.pentaho.repository.create";

    List<String> allowedActions = new ArrayList<>();

    @Override
    public List<String> getAllowedActions( String arg0 ) {
      allowedActions.add( ACTION_READ );
      allowedActions.add( ACTION_CREATE );
      return allowedActions;
    }

    @Override
    public boolean isAllowed( String arg0 ) {
      return true;
    }
  }

  /**
   * Returns a PdiAction that does not override any configuration.
   *
   * @return a PdiAction that does not override any configuration
   */
  private PdiAction getSpyPdiAction() {
    return getSpyPdiAction( null, null, null );
  }

  /**
   * Returns a PdiAction that will override the configuration based on the given parameters.
   * Passing a null for any of the configurations, will result in not existing the corresponding property.
   *
   * @param gatherMetrics the value for the Gather Metrics configuration
   * @param safeMode      the value for the Safe Mode configuration
   * @param logLevel      the value for the Log Level configuration
   * @return a PdiAction that will override the configuration based on the given parameters
   */
  private PdiAction getSpyPdiAction( String gatherMetrics, String safeMode, String logLevel ) {
    Properties props = mock( Properties.class );

    if ( null != gatherMetrics ) {
      doReturn( gatherMetrics ).when( props ).getProperty( PdiAction.GATHER_METRICS_PROPERTY );
    }

    if ( null != safeMode ) {
      doReturn( safeMode ).when( props ).getProperty( PdiAction.SAFE_MODE_PROPERTY );
    }

    if ( null != logLevel ) {
      doReturn( logLevel ).when( props ).getProperty( PdiAction.LOG_LEVEL_PROPERTY );
    }

    PdiAction spiedPdiAction = spy( new PdiAction() );
    doReturn( props ).when( spiedPdiAction ).getPluginSettings();

    return spiedPdiAction;
  }
}
